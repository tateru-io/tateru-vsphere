package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"sync"

	"github.com/coreos/go-systemd/daemon"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	//"github.com/vmware/govmomi"
	"github.com/gorilla/mux"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"
	"gopkg.in/yaml.v2"
)

var (
	listenApi    = flag.String("listen-api", "[::]:7707", "Address and port (TCP) to listen for the manager API server")
	confFile     = flag.String("config", "manager.yml", "Configuration file to read endpoints from")
	buildVersion = "unknown"

	mVersionInfo = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "deploy_manager_version_info",
			Help: "Build information about the deploy manager server",
		},
		[]string{"version"},
	)
)

type managerServer struct {
	mu  sync.RWMutex
	cfg *config
}

type config struct {
	Manager map[string]struct {
		Username string
		Password string
	}
}

func (s *managerServer) StatusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("content-type", "text/plain; charset=UTF-8")
	w.Write([]byte("vSphere deployment manager\n"))
	w.Write([]byte(fmt.Sprintf("Version: %s\n", buildVersion)))
}

func (s *managerServer) MachinesHandler(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	http.Error(w, "Not implemented", http.StatusNotImplemented)
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
	flag.Parse()
	mVersionInfo.With(prometheus.Labels{"version": buildVersion}).Inc()

	var rootlog *zap.Logger
	rootlog, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}

	defer rootlog.Sync()
	rawlog := rootlog.With(zap.String("version", buildVersion))
	log := rawlog.Sugar()

	rawlog.Info("vSphere deploy manager starting up")

	srv := &managerServer{}
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
