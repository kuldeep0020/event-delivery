package receiver

import (
	"context"
	"encoding/json"
	"time"

	"github.com/atzoum/event-delivery/internal/common"
	"github.com/atzoum/event-delivery/internal/config"
	"github.com/confluentinc/confluent-kafka-go/kafka"

	log "github.com/sirupsen/logrus"
)

// EventForwarder is responsible to forward an event to the backing message broker
type EventForwarder interface {
	Forward(event *common.Event) error
	Close()
}

// KafkaForwarder creates a new forwarder which forwards events to a kafka topic
func KafkaForwarder(topic string) (EventForwarder, error) {
	res := kafkaForwarder{Topic: topic}
	if err := res.Init(); err != nil {
		return nil, err
	}
	return &res, nil
}

type kafkaForwarder struct {
	Topic    string
	producer *kafka.Producer
}

func (k *kafkaForwarder) Init() error {
	if err := k.createTopic(); err != nil {
		return err
	}
	if err := k.createProducer(); err != nil {
		return err
	}
	return nil
}

func (k *kafkaForwarder) Close() {
	k.deleteTopic()
	k.producer.Close()
}

func (k *kafkaForwarder) Forward(event *common.Event) error {
	body, _ := json.Marshal(event)
	c := make(chan kafka.Event)
	k.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &k.Topic, Partition: kafka.PartitionAny},
		Key:            []byte(event.UserID),
		Value:          body,
	}, c)

	e := <-c
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		log.WithFields(log.Fields{
			"error": m.TopicPartition.Error,
		}).Error("Kafka delivery failed")
		return m.TopicPartition.Error
	}
	log.WithFields(log.Fields{
		"key":       string(m.Key),
		"topic":     *m.TopicPartition.Topic,
		"partition": m.TopicPartition.Partition,
		"offset":    m.TopicPartition.Offset,
	}).Info("Delivered message")
	return nil
}

func (k *kafkaForwarder) createProducer() error {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": config.GetKafkaBrokers()})
	if err != nil {
		return err
	}
	k.producer = p
	return nil
}

func (k *kafkaForwarder) createTopic() error {
	maxDur, _ := time.ParseDuration("60s")
	kafka.SetAdminOperationTimeout(maxDur)
	a, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": config.GetKafkaBrokers()})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err = a.CreateTopics(
		ctx,
		[]kafka.TopicSpecification{{
			Topic:         k.Topic,
			NumPartitions: 20}},
	)
	if err != nil {
		return err
	}
	return nil
}

func (k *kafkaForwarder) deleteTopic() error {
	maxDur, _ := time.ParseDuration("60s")
	kafka.SetAdminOperationTimeout(maxDur)
	a, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": config.GetKafkaBrokers()})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err = a.DeleteTopics(
		ctx,
		[]string{k.Topic},
	)
	if err != nil {
		return err
	}
	return nil
}
