package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/atzoum/event-delivery/internal/destination"
	"github.com/atzoum/event-delivery/internal/receiver"

	log "github.com/sirupsen/logrus"
)

// A sample process starting one receiver and two destination controllers against a kafka topic named events
func main() {
	const Topic = "events"
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	fwd, err := receiver.KafkaForwarder(Topic)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("KafkaForwarder")
	}
	receiver := receiver.HttpServer{Fwd: fwd}
	// start receiver
	receiver.Start()
	defer receiver.Stop()

	// start 2 destination controllers
	controller1 := destination.KafkaController(Topic, "destination1", destination.MockHandler(100*time.Millisecond, true))
	start(controller1)
	defer controller1.Stop()

	controller2 := destination.KafkaController(Topic, "destination2", destination.MockHandler(10*time.Millisecond, false))
	start(controller2)
	defer controller2.Stop()

	log.Info("awaiting termination signal")
	// wait for a termination signal
	<-sigs
	log.Info("exiting program...")
}

func start(controller destination.Controller) {
	err := controller.Start()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("Controller")
	}

}
