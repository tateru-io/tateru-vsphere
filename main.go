package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"

	"github.com/coreos/go-systemd/daemon"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	//"github.com/vmware/govmomi"
	"github.com/gorilla/mux"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"
)

var (
	listenApi    = flag.String("listen-api", "[::]:7707", "Address and port (TCP) to listen for the manager API server")
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
}

func (s *managerServer) StatusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("content-type", "text/plain; charset=UTF-8")
	w.Write([]byte("vSphere deployment manager\n"))
	w.Write([]byte(fmt.Sprintf("Version: %s\n", buildVersion)))
}

func (s *managerServer) MachinesHandler(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "Not implemented", http.StatusNotImplemented)
}

func init() {
	prometheus.MustRegister(mVersionInfo)
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
	r := mux.NewRouter()
	r.HandleFunc("/", srv.StatusHandler).Methods("GET")
	r.HandleFunc("/v1/machines", srv.MachinesHandler).Methods("GET")
	r.Handle("/metrics", promhttp.Handler())

	go func() {
		if err := http.ListenAndServe(*listenApi, r); err != nil {
			log.Fatalf("Failed to listen and serve status port: %v", err)
		}
	}()

	daemon.SdNotify(false, daemon.SdNotifyReady)

	// Wait for SIGINT / Ctrl+C
	schan := make(chan os.Signal, 1)
	signal.Notify(schan, unix.SIGINT)
	<-schan
	daemon.SdNotify(false, daemon.SdNotifyStopping)
	log.Infof("SIGINT received! Shutting down")

	// Nothing to do

	log.Infof("Shutdown complete")
}
