package main

import (
	"amqp"
	"os"
	"os/signal"
	"syscall"
	"time"

	"bitbucket.org/jdbergmann/protobuf-go/contact/contact"
	"github.com/go-godin/log"
	amqp2 "github.com/streadway/amqp"
)

var amqpSession *amqp.Session

func main() {
	logger := log.NewLoggerFromEnv()
	url := "amqp://guest:guest@localhost:5672/"

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	amqpSession = amqp.NewSession(url)
	defer amqpSession.Shutdown()

	// consume: some.topic
	{
		h1 := func(delivery amqp2.Delivery) {
			logger.Info("OHAI")
			delivery.Ack(false)
		}
		err := amqpSession.AddSubscription("some-exchange", "some-queue", "some.topic", h1)
		if err != nil {
			logger.Error("failed to add subscription", "err", err)
			os.Exit(1)
		}
	}

	// consume: some.other.topic
	{
		h2 := func(delivery amqp2.Delivery) {
			logger.Info("DERP")
			delivery.Ack(false)
		}
		err := amqpSession.AddSubscription("some-exchange", "some-queue", "some.other.topic", h2)
		if err != nil {
			logger.Error("failed to add subscription", "err", err)
			os.Exit(1)
		}
	}
	{
		err := amqpSession.AddPublisher("some-exchange", "some.topic")
		if err != nil {
			logger.Error("failed to add publisher", "err", err)
			os.Exit(1)
		}
		err = amqpSession.AddPublisher("some-exchange", "some.other.topic")
		if err != nil {
			logger.Error("failed to add publisher", "err", err)
			os.Exit(1)
		}
	}

	if err := amqpSession.Declare(); err != nil {
		logger.Error("failed to declare amqp bindings", "err", err)
		os.Exit(1)
	}
	go amqpSession.Consume()

	done := make(chan bool)
	go func(stopPublish <-chan bool) {
		for {
			select {
			case <-stopPublish:
				return
			case <-time.After(2 * time.Second):
				if err := amqpSession.Publish("some.topic", &contact.Contact{Name: "derp"}); err != nil {
					logger.Error("publish failure", "err", err)
				}
			}
		}
	}(done)

	done2 := make(chan bool)
	go func(stopPublish <-chan bool) {
		for {
			select {
			case <-stopPublish:
				return
			case <-time.After(2 * time.Second):
				if err := amqpSession.Publish("some.other.topic", &contact.Contact{Name: "derp"}); err != nil {
					logger.Error("publish failure", "err", err)
				}
			}
		}
	}(done2)

	// shutdown handler
	wait := make(chan struct{})
	go func() {
		s := <-sigc
		done <- true
		done2 <- true
		logger.Info("received signal", "signal", s.String())
		wait <- struct{}{}
	}()

	<-wait
}
