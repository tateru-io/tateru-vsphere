// Deployment manager for vSphere, main program
// Copyright (C) 2020  Christian Svensson
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/coreos/go-systemd/daemon"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/vmware/govmomi"
	"github.com/vmware/govmomi/find"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"
	"gopkg.in/yaml.v2"
)

var (
	listenApi    = flag.String("listen-api", "[::]:7707", "Address and port (TCP) to listen for the manager API server")
	timeoutFlag  = flag.String("timeout", "10s", "Timeout for operations talking to managed resources")
	intervalFlag = flag.String("poll-interval", "30s", "Poll interval for machine database refreshes")
	confFile     = flag.String("config", "manager.yml", "Configuration file to read endpoints from")
	buildVersion = "unknown"

	mVersionInfo = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "deploy_manager_version_info",
			Help: "Build information about the deploy manager server",
		},
		[]string{"version"},
	)
	timeout      time.Duration
	pollInterval time.Duration
)

type managerServer struct {
	mu    sync.RWMutex
	cfg   *config
	log   *zap.Logger
	machs []machine
}

type config struct {
	Targets []managerCfg
}

type managerCfg struct {
	Hostname string
	Username string
	Password string
	Insecure bool
}

type machine struct {
	UUID string `json:"uuid"`
	Name string `json:"name"`
}

func (s *managerServer) StatusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("content-type", "text/plain; charset=UTF-8")
	w.Write([]byte("vSphere deployment manager\n"))
	w.Write([]byte(fmt.Sprintf("Version: %s\n", buildVersion)))
}

func (s *managerServer) pollAll() []machine {
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	cfgs := []*managerCfg{}
	for _, opts := range s.cfg.Targets {
		cfgs = append(cfgs, &opts)
	}
	nj := len(cfgs)

	s.log.Debug("Poll started", zap.Int("managers", nj))

	start := time.Now()
	jobs := make(chan *managerCfg, nj)
	results := make(chan []machine, nj)

	for i := 0; i < nj; i++ {
		jobs <- cfgs[i]
		go s.poll(ctx, jobs, results)
	}
	close(jobs)

	var machs []machine
	for i := 0; i < nj; i++ {
		machs = append(machs, <-results...)
	}
	end := time.Now()
	s.log.Debug("Poll complete", zap.Int("total_vm_count", len(machs)), zap.Duration("total_duration", end.Sub(start)))
	return machs
}

func (s *managerServer) Poller() {
	for {
		machs := s.pollAll()
		s.mu.Lock()
		s.machs = machs
		s.mu.Unlock()
		time.Sleep(pollInterval)
	}
}

func (s *managerServer) poll(ctx context.Context, jobs chan *managerCfg, results chan []machine) {
	opts := <-jobs
	if opts == nil {
		s.log.Fatal("No work assigned to worker, this should not happen")
	}
	start := time.Now()
	var machs []machine
	u := &url.URL{
		Scheme: "https",
		User:   url.UserPassword(opts.Username, opts.Password),
		Host:   opts.Hostname,
		Path:   "/sdk",
	}

	c, err := govmomi.NewClient(ctx, u, opts.Insecure)
	if err != nil {
		s.log.Error("Failed to connect to vCenter host", zap.String("host", opts.Hostname), zap.Error(err))
		results <- []machine{}
		return
	}
	// Logout the connection when we are done but do it in the background
	// TODO: Add event stream to allow for subscribing to events like VM creation
	// between poll intervals. It would allow us to do a best-effort optimization
	// to drive down the latencies in the normal case, allowing higher poll intervals.
	defer func() {
		go c.Logout(context.Background())
	}()
	f := find.NewFinder(c.Client)
	vms, err := f.VirtualMachineList(ctx, "/...")
	if err != nil {
		s.log.Error("Failed to list VMs", zap.String("host", opts.Hostname), zap.Error(err))
		results <- []machine{}
		return
	}
	for i := range vms {
		vm := vms[i]
		machs = append(machs, machine{
			UUID: vm.UUID(ctx),
			Name: vm.Name(),
		})
	}
	end := time.Now()

	s.log.Debug("Poll worker complete", zap.String("host", opts.Hostname), zap.Duration("duration", end.Sub(start)), zap.Int("vm_count", len(machs)))
	results <- machs
}

func (s *managerServer) MachinesHandler(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	js, err := json.MarshalIndent(s.machs, "", "  ")
	if err != nil {
		s.log.Error("Failed to marshal Machines collection", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(js)
}

func init() {
	prometheus.MustRegister(mVersionInfo)
}

func (s *managerServer) loadConfig(file string) error {
	cfgb, err := ioutil.ReadFile(file)
	if err != nil {
		return err
	}
	var cfg config
	if err := yaml.Unmarshal(cfgb, &cfg); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cfg = &cfg
	return nil
}

func main() {
	var err error
	flag.Parse()
	timeout, err = time.ParseDuration(*timeoutFlag)
	if err != nil {
		panic(err)
	}
	pollInterval, err = time.ParseDuration(*intervalFlag)
	if err != nil {
		panic(err)
	}

	mVersionInfo.With(prometheus.Labels{"version": buildVersion}).Inc()
	rootlog, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}

	defer rootlog.Sync()
	rawlog := rootlog.With(zap.String("version", buildVersion))
	log := rawlog.Sugar()

	rawlog.Info("vSphere deploy manager starting up")

	srv := &managerServer{log: rawlog}
	if err := srv.loadConfig(*confFile); err != nil {
		rawlog.Fatal("Failed to load configuration file", zap.String("file", *confFile), zap.Error(err))
	}

	r := mux.NewRouter()
	r.HandleFunc("/", srv.StatusHandler).Methods("GET")
	r.HandleFunc("/v1/machines", srv.MachinesHandler).Methods("GET")
	r.Handle("/metrics", promhttp.Handler())
	// TODO: Add boot-installer (https://github.com/vmware/govmomi/blob/1bed5d199a3ad01fb42455993c432349dec99023/govc/device/boot.go#L64)

	go func() {
		if err := http.ListenAndServe(*listenApi, r); err != nil {
			rawlog.Fatal("Failed to listen and serve status port", zap.Error(err))
		}
	}()

	go srv.Poller()

	daemon.SdNotify(false, daemon.SdNotifyReady)

	// Wait for SIGINT / Ctrl+C
	schan := make(chan os.Signal, 1)
	signal.Notify(schan, unix.SIGINT, unix.SIGHUP)
	for {
		s := <-schan
		if s == unix.SIGHUP {
			log.Infof("SIGHUP received! Reloading configuration")
			if err := srv.loadConfig(*confFile); err != nil {
				rawlog.Error("Failed to load new configuration file, old configuration is still active", zap.String("file", *confFile), zap.Error(err))
			}
		} else if s == unix.SIGINT {
			break
		}
	}

	daemon.SdNotify(false, daemon.SdNotifyStopping)
	log.Infof("SIGINT received! Shutting down")

	// Nothing to do

	log.Infof("Shutdown complete")
}
