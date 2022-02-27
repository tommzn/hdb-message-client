package client

import (
	"encoding/base64"
	"sync"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/golang/protobuf/proto"
	config "github.com/tommzn/go-config"
	log "github.com/tommzn/go-log"
	events "github.com/tommzn/hdb-events-go"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func loadConfigForTest(fileName *string) config.Config {

	configFile := "fixtures/testconfig.yml"
	if fileName != nil {
		configFile = *fileName
	}
	configLoader := config.NewFileConfigSource(&configFile)
	config, _ := configLoader.Load()
	return config
}

func loggerForTest() log.Logger {
	return log.NewLogger(log.Debug, nil, nil)
}

func clientForTest(configFile *string) Client {
	conf := loadConfigForTest(configFile)
	return New(conf, loggerForTest())
}

func exchangeRateForTest() *events.ExchangeRate {
	return &events.ExchangeRate{
		FromCurrency: "USD",
		ToCurrency:   "EUR",
		Rate:         1.23445,
		Timestamp:    asTimeStamp(time.Now()),
	}
}

func billingReportForTest() *events.BillingReport {
	return &events.BillingReport{
		BillingPeriod: "Jan 2021",
		BillingAmount: make(map[string]float64),
		TaxAmount:     make(map[string]float64),
	}
}

func kafkaMessageForTest(topic *string, event proto.Message) *kafka.Message {

	data, _ := proto.Marshal(event)
	return &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     topic,
			Partition: 0,
			Offset:    0,
			Metadata:  nil,
			Error:     nil,
		},
		Value:         []byte(base64.StdEncoding.EncodeToString(data)),
		Key:           []byte("1"),
		Timestamp:     time.Now(),
		TimestampType: kafka.TimestampType(1),
		Opaque:        nil,
		Headers:       []kafka.Header{},
	}
}

func marshalProto(message proto.Message, assert *assert.Assertions) []byte {
	data, err := proto.Marshal(message)
	assert.Nil(err)
	return []byte(base64.StdEncoding.EncodeToString(data))
}

func waitFor(wg *sync.WaitGroup, waitChan chan struct{}) {
	wg.Wait()
	close(waitChan)
}

func asTimeStamp(t time.Time) *timestamppb.Timestamp {
	return timestamppb.New(t)
}
