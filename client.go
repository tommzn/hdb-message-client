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

// New returns a new client which subsribes to Kafka topics and provides
// received messages for clients.
func New(conf config.Config, logger log.Logger) Client {

	pollSleep := conf.GetAsDuration("kafka.poll_sleep", config.AsDurationPtr(10*time.Minute))
	fetchTimeout := conf.GetAsDuration("kafka.fetch_timeout", config.AsDurationPtr(3*time.Second))
	channelSize := conf.GetAsInt("kafka.channel_size", config.AsIntPtr(10))
	kafkaConfig := newKafkaConfig(conf)

	logger.Debugf("Kafka Config: %+v", *kafkaConfig)
	logger.Flush()

	return &MessageClient{
		logger:        logger,
		pollSleep:     *pollSleep,
		events:        make(map[core.DataSource][]proto.Message),
		subscriptions: getSubscriptions(conf),
		fetchTimeout:  *fetchTimeout,
		kafkaConfig:   kafkaConfig,
		eventChan:     make(chan proto.Message, *channelSize),
		chanFilter:    []core.DataSource{},
	}
}

// Run starts message observing. There's no contimuous subsription for new messages. Message client
// will listen for new message until no new messages come in and will pause messagae receiving for
// configured duration. Pause duration can be set by config key kafka.poll_sleep".
func (client *MessageClient) Run(ctx context.Context, waitGroup *sync.WaitGroup) error {

	defer waitGroup.Done()
	defer client.logger.Flush()
	defer client.logger.Info("Done.")

	for {

		client.fetchMessages()
		client.logger.Infof("Finish message receiving, sleep for %s.", client.pollSleep)
		client.logger.Flush()

		select {
		case <-ctx.Done():
			client.logger.Info("Receive cancelation reuqest.")
			return nil
		case <-time.After(client.pollSleep):
			client.logger.Info("Wake up for message polling.")
		}
	}
}

// FetchMessages will subscribes to Kafka topics defined by config and listens
// as long new messages can be received or timeout is reached. Fetch timeout can be set
// by config key "kafka.fetch_timeout".
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

// ProcessMessage will try to convert passed Kafka message into an event and appends it
// to internal stack if evenrything works well.
func (client *MessageClient) processMessage(message *kafka.Message) error {

	topic := message.TopicPartition.Topic
	if topic == nil {
		return errors.New("Unable to get topic.")
	}

	cfg, ok := client.subscriptions[*topic]
	if !ok {
		return fmt.Errorf("No datasource for topics %s defined.", *topic)
	}

	client.logger.Debugf("Try to unmarshal message from %s, content: %s", message.Value, cfg.datasource)

	event, err := toEvent(message.Value, cfg.datasource)
	client.logger.Debugf("Get Event: %+v", event)
	if err == nil {
		client.events[cfg.datasource] = append(client.events[cfg.datasource], event)
		client.removeOldEvents(cfg)
	}
	client.appendToChannel(event, cfg.datasource)
	return err
}

// NewMessageConsumer returns a new Kafka client.
func (client *MessageClient) mewMessageConsumer() (*kafka.Consumer, error) {
	return kafka.NewConsumer(client.kafkaConfig)
}

// RemoveOldEvents will be move old events if number of existing events exceeds stack size.
func (client *MessageClient) removeOldEvents(cfg processConfig) {
	numberOfEvents := len(client.events[cfg.datasource])
	if numberOfEvents > cfg.limit {
		client.events[cfg.datasource] = client.events[cfg.datasource][numberOfEvents-cfg.limit:]
	}
}

// AppendToChannel will send passed event to internal event channnel. If a filter has been set during
// calling Observe events with datasource which differs from filter will be skipped.
func (client *MessageClient) appendToChannel(event proto.Message, datasource core.DataSource) {

	if len(client.eventChan) < cap(client.eventChan) &&
		(len(client.chanFilter) == 0 || isInFilter(datasource, client.chanFilter)) {
		client.eventChan <- event
	}
}

// Latest returns latest event in local queue.
func (client *MessageClient) Latest(datasource core.DataSource) (proto.Message, error) {

	if events, ok := client.events[datasource]; ok && len(events) > 0 {
		return events[len(events)-1], nil
	}
	return nil, ErrNoNewEvents
}

// All returns all available events for passed datasource.
func (client *MessageClient) All(datasource core.DataSource) ([]proto.Message, error) {

	if events, ok := client.events[datasource]; ok {
		return events, nil
	}
	return []proto.Message{}, ErrNoNewEvents
}

// Observe returns a channel you can use to listen for new messages. Filter can be passed if
// you're interested in a specific type of events.
// If channel sappacitiy is reached new events will be discarded. Channel capacity can be
// defined by config key "kafka.channel_size".
func (client *MessageClient) Observe(filter *[]core.DataSource) <-chan proto.Message {
	if filter != nil {
		client.chanFilter = *filter
	}
	return client.eventChan
}
