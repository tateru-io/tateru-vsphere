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
	confFile     = flag.String("config", "manager.yml", "Configuration file to read endpoints from")
	buildVersion = "unknown"

	mVersionInfo = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "deploy_manager_version_info",
			Help: "Build information about the deploy manager server",
		},
		[]string{"version"},
	)
	timeout time.Duration
)

type managerServer struct {
	mu  sync.RWMutex
	cfg *config
	log *zap.Logger
}

type config struct {
	Manager map[string]struct {
		Username string
		Password string
	}
}

type machine struct {
	UUID string
	Name string
}

func (s *managerServer) StatusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("content-type", "text/plain; charset=UTF-8")
	w.Write([]byte("vSphere deployment manager\n"))
	w.Write([]byte(fmt.Sprintf("Version: %s\n", buildVersion)))
}

func (s *managerServer) MachinesHandler(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// TODO: Cache these and don't do them sequentially
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	var machs []machine
	for host, opts := range s.cfg.Manager {
		u := &url.URL{
			Scheme: "https",
			User:   url.UserPassword(opts.Username, opts.Password),
			Host:   host,
			Path:   "/sdk",
		}
		c, err := govmomi.NewClient(ctx, u /* insecure= */, false)
		if err != nil {
			s.log.Error("Failed to connect to vCenter host", zap.String("host", host), zap.Error(err))
			continue
		}
		// Logout the connection when we are done but do it in the background
		defer func() {
			go c.Logout(context.Background())
		}()
		f := find.NewFinder(c.Client)
		vms, err := f.VirtualMachineList(ctx, "/...")
		if err != nil {
			s.log.Error("Failed to list VMs", zap.String("host", host), zap.Error(err))
			continue
		}
		for i := range vms {
			vm := vms[i]
			machs = append(machs, machine{
				UUID: vm.UUID(ctx),
				Name: vm.Name(),
			})
		}
	}
	js, err := json.Marshal(machs)
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

	go func() {
		if err := http.ListenAndServe(*listenApi, r); err != nil {
			rawlog.Fatal("Failed to listen and serve status port", zap.Error(err))
		}
	}()

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
