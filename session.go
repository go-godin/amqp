package amqp

import (
	"context"
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/streadway/amqp"

	"github.com/go-godin/log"
)

type Publisher interface {
	Publish(routingKey string, event interface{}) error
}

type PublishExchange string

type Session struct {
	addr          string
	ctx           context.Context
	cancel        context.CancelFunc
	subscribers   map[string]Subscriber
	publishers    map[string]PublishExchange
	consumerQueue string
	consumeConn   *Connection
	produceConn   *Connection
	consumerDecls []Declaration
	producerDecls []Declaration
}

func NewSession(addr string) *Session {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Session{
		addr:          addr,
		ctx:           ctx,
		cancel:        cancel,
		subscribers:   make(map[string]Subscriber),
		publishers:    make(map[string]PublishExchange),
		consumerQueue: "",
	}

	return s
}

// AddSubscription is a wrapper which uses the Auto*() functions
// to quickly add an exchange, queue and binding to the declarations list.
// It will also register the subscriber handler function with the subscriber map.
// If no connection for the consumer exist, the connection is established
// at this point. This happens only once, even if you add multiple subscriptions.
func (s *Session) AddSubscription(exchangeName, queueName, routingKey string, handler Subscriber) error {
	if s.consumerQueue != "" && s.consumerQueue != queueName {
		return fmt.Errorf("a consumer queue with name '%s' has already been defined", s.consumerQueue)
	}
	s.consumerQueue = queueName
	s.consumerDecls = append(s.consumerDecls, AutoExchange(exchangeName))
	s.consumerDecls = append(s.consumerDecls, AutoQueue(queueName))
	s.consumerDecls = append(s.consumerDecls, AutoBinding(routingKey, queueName, exchangeName))
	s.subscribers[routingKey] = handler

	log.Info("added subscription", "exchange", exchangeName, "queue", queueName, "routingKey", routingKey)
	return nil
}

// AddPublisher is a wrapper to convenitently prepare the session for publishing on a specific exchange.
// The method ensures that the target exchange is declared when calling Declare().
func (s *Session) AddPublisher(exchangeName, routingKey string) error {
	if _, exists := s.publishers[routingKey]; exists {
		return fmt.Errorf("a publisher with that routingKey is already registered")
	}
	s.producerDecls = append(s.producerDecls, AutoExchange(exchangeName))
	s.publishers[routingKey] = PublishExchange(exchangeName)

	return nil
}

// Publish will take the event, marshall it into a proto.Message and then send it on it's journey
// to the spe
func (s *Session) Publish(routingKey string, event interface{}) error {
	exchange, ok := s.publishers[routingKey]
	if !ok {
		return fmt.Errorf("no publisher with routingKey %s registered, cannot resolve exchange", routingKey)
	}

	protobuf := event.(proto.Message)
	bodyBytes, err := proto.Marshal(protobuf)
	if err != nil {
		return err
	}
	publishing := amqp.Publishing{
		Headers:      amqp.Table{},
		ContentType:  "application/octet-stream",
		DeliveryMode: amqp.Transient,
		Priority:     0,
		Body:         bodyBytes,
	}

	ch, err := s.produceConn.Channel()
	if err != nil {
		return err
	}

	if err := ch.Publish(string(exchange), routingKey, false, false, publishing); err != nil {
		return err
	}

	log.Info(fmt.Sprintf("published message to exchange %s with routingKey %s", exchange, routingKey), "exchange", exchange, "routingKey", routingKey)

	return nil
}

// ensureConnections will ensure that for any configured consumer or producer declarations,
// a connection exists and is online.
func (s *Session) ensureConnections() error {
	if len(s.consumerDecls) > 0 && s.consumeConn == nil {
		s.consumeConn = NewConnection(s.addr)
		if err := s.consumeConn.Connect(); err != nil {
			return fmt.Errorf("failed to create amqp connection: %s", err)
		}
		log.Info("amqp consumer connection established")
	}
	if len(s.producerDecls) > 0 && s.produceConn == nil {
		s.produceConn = NewConnection(s.addr)
		if err := s.produceConn.Connect(); err != nil {
			return fmt.Errorf("failed to create amqp connection: %s", err)
		}
		log.Info("amqp producer connection established")
	}
	return nil
}

// Declare goes through all declarations and uses the consumer/produce connection to
// obtain a channel and perform the declarations.
func (s *Session) Declare() error {
	if err := s.ensureConnections(); err != nil {
		return err
	}

	// declare all the subscriber things!
	if len(s.consumerDecls) > 0 {
		ch, _ := s.consumeConn.Channel()
		for _, declare := range s.consumerDecls {
			if err := declare(ch); err != nil {
				return fmt.Errorf("failed to declare for consumer: %s", err.Error())
			}
		}
	}

	// declare all the consumer things!
	if len(s.producerDecls) > 0 {
		ch, _ := s.produceConn.Channel()
		for _, declare := range s.consumerDecls {
			if err := declare(ch); err != nil {
				return fmt.Errorf("failed to declare for producer: %s", err.Error())
			}
		}
	}

	return nil
}

// Shutdown all existing connections but wait for any in-flight messages to be processed first.
// Finally, the session context is cancelled which will stop any child-goroutines.
func (s *Session) Shutdown() {
	defer s.cancel()

	if s.consumeConn != nil {
		s.consumeConn.Shutdown()
		log.Info("amqp consumer connection closed")
	}
	if s.produceConn != nil {
		s.produceConn.Shutdown()
		log.Info("amqp producer connection closed")
	}
}

func (s *Session) Consume() {
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		if !s.consumeConn.IsConnected() {
			log.Info("consuming halted: connection offline")
			time.Sleep(5 * time.Second)
			continue
		}

		ch, err := s.consumeConn.Channel()
		if err != nil {
			log.Error("failed to fetch channel", "err", err)
			continue
		}

		deliveries, err := ch.Consume(s.consumerQueue, "", false, false, false, false, nil)
		if err != nil {
			log.Error("consumer error", "err", err)
			continue
		}

		for delivery := range deliveries {
			routingKey := delivery.RoutingKey
			log.Info("incoming amqp delivery", "routingKey", routingKey)
			if handler, ok := s.subscribers[routingKey]; ok {
				handler(delivery)
			} else {
				log.Error("delivery has routing key which cannot be processed, NACKing")
				delivery.Nack(false, false)
			}
		}
	}
}
