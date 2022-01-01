package client

import (
	"fmt"
	"math/rand"
	"strconv"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/golang/protobuf/proto"
	config "github.com/tommzn/go-config"
	core "github.com/tommzn/hdb-core"
	events "github.com/tommzn/hdb-events-go"
)

const runes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func randomId(length int) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = runes[rand.Intn(len(runes))]
	}
	return string(b)
}

func getSubscriptions(conf config.Config) map[string]processConfig {

	subscriptions := make(map[string]processConfig)
	subsriptionConfig := conf.GetAsSliceOfMaps("kafka.subsriptions")
	for _, subsription := range subsriptionConfig {
		if topic, ok := subsription["topic"]; ok {
			cfg := processConfig{
				limit: 1,
			}
			if datasource, ok := subsription["datasource"]; ok {
				cfg.datasource = core.DataSource(datasource)
			}
			if limitStr, ok := subsription["limit"]; ok {
				if limit, err := strconv.Atoi(limitStr); err == nil {
					if limit < 1 {
						limit = 1
					}
					cfg.limit = limit
				}
			}
			subscriptions[topic] = cfg
		}
	}
	return subscriptions
}

func newKafkaConfig(conf config.Config) *kafka.ConfigMap {

	kafkaConfig := kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          "hdb-message-client-" + randomId(4),
		"auto.offset.reset": "earliest",
	}
	if servers := conf.Get("kafka.servers", nil); servers != nil {
		kafkaConfig.SetKey("bootstrap.servers", *servers)
	}
	return &kafkaConfig
}

func topicsToSubscribe(subsriptions map[string]processConfig) []string {
	topics := []string{}
	for topic, _ := range subsriptions {
		topics = append(topics, topic)
	}
	return topics
}

func isInFilter(datasource core.DataSource, filter []core.DataSource) bool {
	for _, filterDataSource := range filter {
		if filterDataSource == datasource {
			return true
		}
	}
	return false
}

func toEvent(messageData []byte, datasource core.DataSource) (proto.Message, error) {

	var event proto.Message
	switch datasource {
	case core.DATASOURCE_BILLINGREPORT:
		event = &events.BillingReport{}
	case core.DATASOURCE_WEATHER:
		event = &events.WeatherData{}
	case core.DATASOURCE_INDOORCLIMATE:
		event = &events.IndoorClimate{}
	case core.DATASOURCE_EXCHANGERATE:
		event = &events.ExchangeRate{}
	default:
		return nil, fmt.Errorf("Unsupported datasource: %s", datasource)
	}
	err := proto.Unmarshal(messageData, event)
	return event, err
}
