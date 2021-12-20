package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/golang/protobuf/proto"
	config "github.com/tommzn/go-config"
	log "github.com/tommzn/go-log"
	core "github.com/tommzn/hdb-core"
)

var ErrNoNewEvents = errors.New("No new events available")

func New(conf config.Config, logger log.Logger) Client {

	pollSleep := conf.GetAsDuration("kafka.poll_sleep", config.AsDurationPtr(10*time.Minute))
	fetchTimeout := conf.GetAsDuration("kafka.fetch_timeout", config.AsDurationPtr(3*time.Second))
	return &MessageClient{
		logger:        logger,
		pollSleep:     *pollSleep,
		events:        make(map[core.DataSource][]proto.Message),
		subscriptions: getSubscriptions(conf),
		fetchTimeout:  *fetchTimeout,
		kafkaConfig:   newKafkaConfig(conf),
	}
}

func (client *MessageClient) Run(ctx context.Context, waitGroup *sync.WaitGroup) error {

	defer waitGroup.Done()
	defer client.logger.Flush()
	defer client.logger.Info("Done.")

	for {

		client.fetchMessages()
		client.logger.Infof("Finish message receiving, sleep for %s.", client.pollSleep)

		select {
		case <-ctx.Done():
			client.logger.Info("Receive cancelation reuqest.")
			return nil
		case <-time.After(client.pollSleep):
			client.logger.Info("Wake up for message polling.")
		}
	}
}

func (client *MessageClient) fetchMessages() {

	defer client.logger.Flush()

	consumer, err := client.mewMessageConsumer()
	if err != nil {
		client.logger.Error("Unable to create consumer for Kafka, reason: ", err)
		return
	}
	defer consumer.Close()

	topics := topicsToSubscribe(client.subscriptions)
	consumer.SubscribeTopics(topics, nil)
	for {

		msg, err := consumer.ReadMessage(client.fetchTimeout)
		if err != nil {
			if err.(kafka.Error).Code() == kafka.ErrTimedOut {
				client.logger.Infof("No new messages received after %s, stop consuming.", client.fetchTimeout)
			} else {
				client.logger.Error("Unable to fetch messages, reason: ", err)
			}
			return
		}

		client.logger.Info("Message received from topic: ", *msg.TopicPartition.Topic)
		client.processMessage(msg)
	}
}

func (client *MessageClient) processMessage(message *kafka.Message) error {

	topic := message.TopicPartition.Topic
	if topic == nil {
		return errors.New("Unable to get topic.")
	}

	cfg, ok := client.subscriptions[*topic]
	if !ok {
		return fmt.Errorf("No datasource for topics %s defined.", *topic)
	}

	event, err := toEvent(message.Value, cfg.datasource)
	if err == nil {
		client.events[cfg.datasource] = append(client.events[cfg.datasource], event)
		client.removeOldEvents(cfg)
	}
	return err
}

func (client *MessageClient) mewMessageConsumer() (*kafka.Consumer, error) {
	return kafka.NewConsumer(client.kafkaConfig)
}

func (client *MessageClient) removeOldEvents(cfg processConfig) {
	numberOfEvents := len(client.events[cfg.datasource])
	if numberOfEvents > cfg.limit {
		client.events[cfg.datasource] = client.events[cfg.datasource][numberOfEvents-cfg.limit:]
	}
}

func (client *MessageClient) Latest(datasource core.DataSource) (proto.Message, error) {

	if events, ok := client.events[datasource]; ok && len(events) > 0 {
		return events[len(events)-1], nil
	}
	return nil, ErrNoNewEvents
}

func (client *MessageClient) All(datasource core.DataSource) ([]proto.Message, error) {

	if events, ok := client.events[datasource]; ok {
		return events, nil
	}
	return []proto.Message{}, ErrNoNewEvents
}
