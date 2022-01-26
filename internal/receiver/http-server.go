package receiver

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/atzoum/event-delivery/internal/common"

	log "github.com/sirupsen/logrus"
)

type HttpServer struct {
	Fwd    EventForwarder
	server *http.Server
}

func (r *HttpServer) Start() {
	r.server = &http.Server{
		Addr:           ":8080",
		Handler:        r,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}
	go func() {
		log.Info("Server listening for events")
		if err := r.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.WithFields(log.Fields{
				"error": err,
			}).Fatal("listen")
		}
	}()
}

func (r *HttpServer) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	defer r.Fwd.Close()
	if err := r.server.Shutdown(ctx); err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Warn("Server graceful shutdown failed")
		if err := r.server.Close(); err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Fatal("Server forceful shutdown failed")
		}
	}
	log.Info("Server stopped listening")
}

func (r *HttpServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		fmt.Println(err)
		http.Error(w, "Failed to read", 500)
		return
	}
	event := common.Event{}
	if err := json.Unmarshal(body, &event); err != nil {
		fmt.Println(err)
		http.Error(w, "Failed to unmarshal", 500)
		return
	}
	if err := r.Fwd.Forward(&event); err != nil {
		fmt.Println(err)
		http.Error(w, "Failed to forward", 500)
	}

	w.WriteHeader(http.StatusOK)
}
