package destination

import (
	"encoding/json"
	"time"

	"github.com/atzoum/event-delivery/internal/common"
	"github.com/atzoum/event-delivery/internal/config"
	"github.com/confluentinc/confluent-kafka-go/kafka"

	log "github.com/sirupsen/logrus"
)

// Controller controls ingestion of events to destinations
type Controller interface {
	// Start starts the  events ingestion
	Start() error
	// Stop stops the events ingestion
	Stop()
	// GetHandler gets the Handler
	GetHandler() Handler

	// SetHandler sets the Handler
	SetHandler(handler Handler)
}

// KafkaController creates a new controller which consumes events from a kafka topic
func KafkaController(topic string, groupId string, handler Handler) Controller {
	res := kafkaController{topic: topic, groupId: groupId, handler: handler}
	return &res
}

type kafkaController struct {
	topic    string
	groupId  string
	consumer *kafka.Consumer
	handler  Handler
	stopped  bool
}

func (c *kafkaController) Start() error {
	c.stopped = false
	if err := c.createConsumer(); err != nil {
		return err
	}
	return nil
}

func (c *kafkaController) Stop() {
	c.stopped = true
	c.consumer.Unsubscribe()
	c.consumer.Close()
}

func (c *kafkaController) GetHandler() Handler {
	return c.handler
}

func (c *kafkaController) SetHandler(handler Handler) {
	c.handler = handler
}

func (c *kafkaController) createConsumer() error {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        config.GetKafkaBrokers(),
		"group.id":                 c.groupId,
		"auto.offset.reset":        "beginning",
		"max.poll.interval.ms":     300000,
		"enable.auto.offset.store": false,
	})
	if err != nil {
		return err
	}
	c.consumer = consumer
	err = c.consumer.SubscribeTopics([]string{c.topic}, nil)
	if err != nil {
		return err
	}
	go func() {
		for !c.stopped {
			ev := c.consumer.Poll(100)
			if ev == nil {
				continue
			}
			switch e := ev.(type) {
			case *kafka.Message:
				// unmarshall message
				event := common.Event{}
				if err := json.Unmarshal(e.Value, &event); err != nil {
					log.WithFields(log.Fields{
						"error": err,
					}).Error("Unmarshalling error")
					// there is no way recovering from a malformed message
					continue
				}
				c.handleWithRetries(e, event)
			case kafka.Error:
				// Errors should generally be considered
				// informational, the client will try to
				// automatically recover.
				log.WithFields(log.Fields{
					"error": err,
				}).Error("Kafka error", e.Code(), e)
			default:
				log.WithFields(log.Fields{
					"event": e,
				}).Debug("Ignored event")
			}
		}

	}()
	return nil
}

func (c *kafkaController) handleWithRetries(e *kafka.Message, event common.Event) {
	retries := 0
	err := c.handler.Handle(&event)
	for !c.stopped && err != nil && retries < config.GetRetryCount() {
		retries++
		// backoff by retry-times seconds: 1, 2, 3, 4, 5, etc
		time.Sleep(time.Duration(retries) * time.Second)
		log.WithFields(log.Fields{
			"groupId":   c.groupId,
			"key":       string(e.Key),
			"topic":     *e.TopicPartition.Topic,
			"partition": e.TopicPartition.Partition,
			"offset":    e.TopicPartition.Offset,
			"retry":     retries,
		}).Info("Retrying message")
		c.handler.Handle(&event)
	}
	if !c.stopped {
		if err == nil {
			log.WithFields(log.Fields{
				"groupId":   c.groupId,
				"key":       string(e.Key),
				"topic":     *e.TopicPartition.Topic,
				"partition": e.TopicPartition.Partition,
				"offset":    e.TopicPartition.Offset,
				"retries":   retries,
			}).Info("Handled message")
		} else {
			log.WithFields(log.Fields{
				"groupId":   c.groupId,
				"key":       string(e.Key),
				"topic":     *e.TopicPartition.Topic,
				"partition": e.TopicPartition.Partition,
				"offset":    e.TopicPartition.Offset,
				"retries":   retries,
			}).Warn("Skipped message")
		}
		c.consumer.StoreOffsets([]kafka.TopicPartition{{
			Topic:     &c.topic,
			Partition: e.TopicPartition.Partition,
			Metadata:  e.TopicPartition.Metadata,
			Offset:    e.TopicPartition.Offset + 1,
		}})
	}

}
