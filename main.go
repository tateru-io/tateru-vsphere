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
	"math"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/coreos/go-systemd/daemon"
	"github.com/gorilla/mux"
	lru "github.com/hashicorp/golang-lru"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/vmware/govmomi"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25/types"
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
		[]string{"name", "version"},
	)
	mPollDuration = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "deploy_manager_poll_duration_seconds",
			Help: "Duration taken to poll the targets in seconds",
		},
		[]string{"target"},
	)
	mPollSuccess = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "deploy_manager_poll_success",
			Help: "Set to 1 if the poll of a target was successful",
		},
		[]string{"target"},
	)
	mMachines = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "deploy_manager_machines_total",
			Help: "Number of machines known to the manager",
		},
		[]string{"target"},
	)
	timeout      time.Duration
	pollInterval time.Duration
)

type managerServer struct {
	mu        sync.RWMutex
	cfg       *config
	log       *zap.Logger
	machs     []machine
	opCache   *lru.Cache
	opUUIDMap sync.Map
}

type config struct {
	Targets []target
}

type target struct {
	Hostname string
	Username string
	Password string
	Insecure bool
}

type machine struct {
	UUID string `json:"uuid"`
	Name string `json:"name"`
	tgt  *target
}

type OpStatus string

const (
	InstallerOpInProgress       OpStatus = ""
	InstallerOpOK               OpStatus = "ok"
	InstallerOpFailed           OpStatus = "failed"
	InstallerOpFailedConcurrent OpStatus = "failed-concurrent"
)

type Op struct {
	m           *machine
	nonce       string
	status      OpStatus
	statusMutex sync.RWMutex
}

func (s *managerServer) StatusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("content-type", "text/plain; charset=UTF-8")
	w.Write([]byte("vSphere deployment manager\n"))
	w.Write([]byte(fmt.Sprintf("Version: %s\n", buildVersion)))
}

func (s *managerServer) pollAll() []machine {
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	cfgs := []*target{}
	for _, opts := range s.cfg.Targets {
		cfgs = append(cfgs, &opts)
	}
	nj := len(cfgs)

	s.log.Debug("Poll started", zap.Int("managers", nj))

	start := time.Now()
	jobs := make(chan *target, nj)
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

func connect(ctx context.Context, tgt *target) (*govmomi.Client, error) {
	u := &url.URL{
		Scheme: "https",
		User:   url.UserPassword(tgt.Username, tgt.Password),
		Host:   tgt.Hostname,
		Path:   "/sdk",
	}
	return govmomi.NewClient(ctx, u, tgt.Insecure)
}

func (s *managerServer) poll(ctx context.Context, jobs chan *target, results chan []machine) {
	tgt := <-jobs
	if tgt == nil {
		s.log.Fatal("No work assigned to worker, this should not happen")
	}
	start := time.Now()
	var machs []machine
	c, err := connect(ctx, tgt)
	if err != nil {
		s.log.Error("Failed to connect to vCenter host", zap.String("host", tgt.Hostname), zap.Error(err))
		results <- []machine{}
		mPollSuccess.With(prometheus.Labels{"target": tgt.Hostname}).Set(0.0)
		mPollDuration.With(prometheus.Labels{"target": tgt.Hostname}).Set(math.NaN())
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
		s.log.Error("Failed to list VMs", zap.String("host", tgt.Hostname), zap.Error(err))
		results <- []machine{}
		mPollSuccess.With(prometheus.Labels{"target": tgt.Hostname}).Set(0.0)
		mPollDuration.With(prometheus.Labels{"target": tgt.Hostname}).Set(math.NaN())
		return
	}
	for i := range vms {
		vm := vms[i]
		machs = append(machs, machine{
			UUID: vm.UUID(ctx),
			Name: vm.Name(),
			tgt:  tgt,
		})
	}
	end := time.Now()

	mPollSuccess.With(prometheus.Labels{"target": tgt.Hostname}).Set(1.0)
	mPollDuration.With(prometheus.Labels{"target": tgt.Hostname}).Set(end.Sub(start).Seconds())
	mMachines.With(prometheus.Labels{"target": tgt.Hostname}).Set(float64(len(machs)))
	s.log.Debug("Poll worker complete", zap.String("host", tgt.Hostname), zap.Duration("duration", end.Sub(start)), zap.Int("vm_count", len(machs)))
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

func (s *managerServer) SpecificMachineHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	uuid := vars["uuid"]
	s.mu.RLock()
	defer s.mu.RUnlock()
	var m *machine
	for _, x := range s.machs {
		if x.UUID == uuid {
			m = &x
			break
		}
	}
	if m == nil {
		http.NotFound(w, r)
		return
	}

	js, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		s.log.Error("Failed to marshal Machine object", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(js)
}

func (s *managerServer) opCacheEvict(key interface{}, value interface{}) {
	op := value.(*Op)
	// Lock on the operation to block the eviction
	op.statusMutex.RLock()
	defer op.statusMutex.RUnlock()
	if op.status == InstallerOpInProgress {
		// This is bad, this means that some request might end up being executed again
		// that were just trying to get the status of an already in progress operation.
		// Fail hard to avoid any damage on the target VMs.
		s.log.Fatal("Operation was evicted while still running!")
	}
	s.log.Debug("Operation evicted from cache", zap.String("uuid", op.m.UUID), zap.String("nonce", op.nonce))
}

func (s *managerServer) BootInstallerHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	uuid := vars["uuid"]
	s.mu.RLock()
	var m *machine
	for _, x := range s.machs {
		if x.UUID == uuid {
			m = &x
			break
		}
	}
	s.mu.RUnlock()
	if m == nil {
		http.NotFound(w, r)
		return
	}

	var p struct {
		Nonce string `json:"nonce"`
	}
	err := json.NewDecoder(r.Body).Decode(&p)
	if err != nil || p.Nonce == "" {
		http.Error(w, "json: nonce missing from request", http.StatusBadRequest)
		return
	}

	op := &Op{
		m:     m,
		nonce: p.Nonce,
	}
	v, found, _ := s.opCache.PeekOrAdd(uuid+":"+op.nonce, op)
	if !found {
		// This was our op that we inserted
		c := make(chan int)
		go s.executeInstallerOp(c, op)
		<-c
	} else {
		op = v.(*Op)
	}
	op.statusMutex.RLock()
	defer op.statusMutex.RUnlock()
	if op.status == InstallerOpOK {
		// All OK
		return
	} else if op.status == InstallerOpFailedConcurrent {
		http.Error(w, "concurrent operation in progress", http.StatusConflict)
		return
	}
	http.Error(w, "request failed", http.StatusInternalServerError)
}

func (s *managerServer) executeInstallerOp(ch chan int, op *Op) {
	m := op.m
	s.log.Debug("Executing boot installer operation", zap.String("uuid", m.UUID), zap.String("nonce", op.nonce))
	op.statusMutex.Lock()
	defer op.statusMutex.Unlock()
	defer func() {
		s.log.Info("Boot installer operation completed", zap.String("uuid", m.UUID), zap.String("nonce", op.nonce), zap.String("status", string(op.status)))
	}()
	ch <- 1

	_, loaded := s.opUUIDMap.LoadOrStore(m.UUID, ".")
	if loaded {
		s.log.Error("Another operation on UUID already running", zap.String("uuid", m.UUID), zap.String("nonce", op.nonce))
		op.status = InstallerOpFailedConcurrent
		return
	}

	defer func() {
		s.opUUIDMap.Delete(m.UUID)
	}()

	ctx, _ := context.WithTimeout(context.Background(), timeout)
	c, err := connect(ctx, m.tgt)
	if err != nil {
		s.log.Error("Failed to connect to vCenter host", zap.String("host", m.tgt.Hostname), zap.Error(err))
		op.status = InstallerOpFailed
		return
	}
	defer func() {
		go c.Logout(context.Background())
	}()

	si := object.NewSearchIndex(c.Client)
	ref, err := si.FindByUuid(ctx, nil, m.UUID, true, types.NewBool(false))
	if err != nil {
		s.log.Error("FindByUuid failed", zap.String("host", m.tgt.Hostname), zap.String("uuid", m.UUID), zap.Error(err))
		op.status = InstallerOpFailed
		return
	}
	if ref == nil {
		s.log.Error("FindByUuid returned nil, VM gone?", zap.String("host", m.tgt.Hostname), zap.String("uuid", m.UUID))
		op.status = InstallerOpFailed
		return
	}
	vm := ref.(*object.VirtualMachine)

	devices, err := vm.Device(ctx)
	if err != nil {
		s.log.Error("vm.Device failed", zap.String("host", m.tgt.Hostname), zap.String("uuid", m.UUID), zap.Error(err))
		op.status = InstallerOpFailed
		return
	}

	// Set netboot
	bo := types.VirtualMachineBootOptions{
		BootOrder: devices.BootOrder([]string{"ethernet"}),
	}
	spec := types.VirtualMachineConfigSpec{
		BootOptions: &bo,
	}

	t, err := vm.Reconfigure(ctx, spec)
	if err != nil {
		s.log.Error("vm.Reconfigure failed", zap.String("host", m.tgt.Hostname), zap.String("uuid", m.UUID), zap.Error(err))
		op.status = InstallerOpFailed
		return
	}

	if err := t.Wait(ctx); err != nil {
		s.log.Error("vm.Reconfigure.Wait failed", zap.String("host", m.tgt.Hostname), zap.String("uuid", m.UUID), zap.Error(err))
		op.status = InstallerOpFailed
		return
	}

	// Reset or power-on VM
	ps, err := vm.PowerState(ctx)
	if err != nil {
		s.log.Error("vm.PowerState failed", zap.String("host", m.tgt.Hostname), zap.String("uuid", m.UUID), zap.Error(err))
		op.status = InstallerOpFailed
		return
	}

	if ps == types.VirtualMachinePowerStatePoweredOn {
		t, err = vm.Reset(ctx)
		if err != nil {
			s.log.Error("vm.Reset failed", zap.String("host", m.tgt.Hostname), zap.String("uuid", m.UUID), zap.Error(err))
			op.status = InstallerOpFailed
			return
		}
		if err := t.Wait(ctx); err != nil {
			s.log.Error("vm.Reset.Wait failed", zap.String("host", m.tgt.Hostname), zap.String("uuid", m.UUID), zap.Error(err))
			op.status = InstallerOpFailed
			return
		}
	} else {
		t, err = vm.PowerOn(ctx)
		if err != nil {
			s.log.Error("vm.PowerOn failed", zap.String("host", m.tgt.Hostname), zap.String("uuid", m.UUID), zap.Error(err))
			op.status = InstallerOpFailed
			return
		}
		if err := t.Wait(ctx); err != nil {
			s.log.Error("vm.PowerOn.Wait failed", zap.String("host", m.tgt.Hostname), zap.String("uuid", m.UUID), zap.Error(err))
			op.status = InstallerOpFailed
			return
		}
	}

	// Wait for the VM to reset before resetting the boot sequence
	// TODO: There might exist some nicer way to see that the machine has booted,
	// but I could not find one by first glance.
	time.Sleep(time.Second * 10)

	ctx, _ = context.WithTimeout(context.Background(), timeout)
	// Set default boot again
	bo.BootOrder = devices.BootOrder([]string{"-"})
	t, err = vm.Reconfigure(ctx, spec)
	if err != nil {
		s.log.Error("vm.Reconfigure 2nd failed", zap.String("host", m.tgt.Hostname), zap.String("uuid", m.UUID), zap.Error(err))
		op.status = InstallerOpFailed
		return
	}

	if err := t.Wait(ctx); err != nil {
		s.log.Error("vm.Reconfigure.Wait 2nd failed", zap.String("host", m.tgt.Hostname), zap.String("uuid", m.UUID), zap.Error(err))
		op.status = InstallerOpFailed
		return
	}
	// All done
	op.status = InstallerOpOK
}

func init() {
	prometheus.MustRegister(mVersionInfo)
	prometheus.MustRegister(mPollDuration)
	prometheus.MustRegister(mPollSuccess)
	prometheus.MustRegister(mMachines)
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

	mVersionInfo.With(prometheus.Labels{"name": "tateru-vsphere", "version": buildVersion}).Inc()
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
	srv.opCache, err = lru.NewWithEvict(128, srv.opCacheEvict)
	if err != nil {
		rawlog.Fatal("Failed to initialize operations cache", zap.Error(err))
	}

	r := mux.NewRouter()
	r.HandleFunc("/", srv.StatusHandler).Methods("GET")
	r.HandleFunc("/v1/machines", srv.MachinesHandler).Methods("GET")
	r.HandleFunc("/v1/machines/{uuid}", srv.SpecificMachineHandler).Methods("GET")
	r.HandleFunc("/v1/machines/{uuid}/boot-installer", srv.BootInstallerHandler).Methods("POST")
	r.Handle("/metrics", promhttp.Handler())

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
