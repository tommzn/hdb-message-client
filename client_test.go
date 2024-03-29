package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	config "github.com/tommzn/go-config"
	core "github.com/tommzn/hdb-core"
	events "github.com/tommzn/hdb-events-go"
	"google.golang.org/protobuf/proto"
)

type ClientTestSuite struct {
	suite.Suite
}

func TestClientTestSuite(t *testing.T) {
	suite.Run(t, new(ClientTestSuite))
}

type ClientIntegrationTestSuite struct {
	suite.Suite
	topic    string
	conf     config.Config
	producer *kafka.Producer
}

func TestClientIntegrationTestSuite(t *testing.T) {
	suite.Run(t, new(ClientIntegrationTestSuite))
}

func (suite *ClientIntegrationTestSuite) SetupTest() {

	suite.topic = "integration_test_topic"
	suite.conf = loadConfigForTest(config.AsStringPtr("fixtures/testconfig2.yml"))
	kafkaConfig := &kafka.ConfigMap{"bootstrap.servers": "localhost"}
	if servers := suite.conf.Get("kafka.servers", nil); servers != nil {
		kafkaConfig.SetKey("bootstrap.servers", *servers)
	}
	producer, err := kafka.NewProducer(kafkaConfig)
	suite.Nil(err)
	suite.producer = producer
}

func (suite *ClientIntegrationTestSuite) TearDownTest() {
	suite.producer.Flush(15 * 1000)
	suite.producer.Close()
}

func (suite *ClientTestSuite) TestGetLatest() {

	client := clientForTest(nil)
	addEventsForTest(client)

	suite.Len(client.(*MessageClient).events[core.DATASOURCE_EXCHANGERATE], 3)
	latest, err := client.Latest(core.DATASOURCE_EXCHANGERATE)
	suite.Nil(err)
	suite.NotNil(latest)
	exchangeRate, ok := latest.(*events.ExchangeRates)
	suite.True(ok)
	suite.Equal(float64(3), exchangeRate.Rates[0].Rate)

	latest2, err2 := client.Latest(core.DATASOURCE_BILLINGREPORT)
	suite.NotNil(err2)
	suite.Nil(latest2)

	fmt.Println(client.(*MessageClient).String())
}

func (suite *ClientTestSuite) TestGetAll() {

	client := clientForTest(nil)
	addEventsForTest(client)

	allEvents1, err := client.All(core.DATASOURCE_EXCHANGERATE)
	suite.Nil(err)
	suite.Len(allEvents1, 3)

	allEvents2, err2 := client.All(core.DATASOURCE_BILLINGREPORT)
	suite.NotNil(err2)
	suite.Len(allEvents2, 0)
}

func (suite *ClientTestSuite) TestObserve() {

	client := clientForTest(nil)
	evemtChan := client.Observe(nil)

	addProcessedEventsForTest(client)
	suite.True(len(evemtChan) == 3)
	event := <-evemtChan
	suite.NotNil(event)
	exchangeRate, ok := event.(*events.ExchangeRates)
	suite.True(ok)
	suite.Equal(1.0, exchangeRate.Rates[0].Rate)

	addProcessedEventsForTest(client)
	addProcessedEventsForTest(client)
	addProcessedEventsForTest(client)
	suite.True(len(evemtChan) == 10)
}

func (suite *ClientTestSuite) TestObserveFiltered() {

	client := clientForTest(nil)
	filter := []core.DataSource{core.DATASOURCE_BILLINGREPORT}
	evemtChan := client.Observe(&filter)

	addProcessedEventsForTest(client)
	suite.True(len(evemtChan) == 0)
}

func (suite *ClientTestSuite) TestReadyState() {

	client := clientForTest(nil)
	suite.False(client.IsReady())
	addEventsForTest(client)
	suite.True(client.IsReady())

	filter := []core.DataSource{core.DATASOURCE_BILLINGREPORT}
	client.Observe(&filter)
	suite.False(client.IsReady())

	filter2 := []core.DataSource{core.DATASOURCE_EXCHANGERATE}
	client.Observe(&filter2)
	suite.True(client.IsReady())
}

func (suite *ClientTestSuite) TestRemoveOldEvents() {

	client := clientForTest(nil)
	addEventsForTest(client)

	processConf := processConfig{
		datasource: core.DATASOURCE_EXCHANGERATE,
		limit:      1,
	}
	client.(*MessageClient).removeOldEvents(processConf)
	suite.Len(client.(*MessageClient).events[core.DATASOURCE_EXCHANGERATE], 1)
	latest, err := client.Latest(core.DATASOURCE_EXCHANGERATE)
	suite.Nil(err)
	suite.NotNil(latest)
	exchangeRate, ok := latest.(*events.ExchangeRates)
	suite.True(ok)
	suite.Equal(float64(3), exchangeRate.Rates[0].Rate)
}

func (suite *ClientTestSuite) TestProcessMessage() {

	client := clientForTest(nil)
	topic := "topic"

	kafkaNessage1 := kafkaMessageForTest(nil, exchangeRateForTest())
	suite.NotNil(client.(*MessageClient).processMessage(kafkaNessage1))

	kafkaNessage2 := kafkaMessageForTest(&topic, exchangeRateForTest())
	suite.NotNil(client.(*MessageClient).processMessage(kafkaNessage2))

	processConf := processConfig{
		datasource: core.DATASOURCE_EXCHANGERATE,
		limit:      1,
	}
	client.(*MessageClient).subscriptions[topic] = processConf
	kafkaNessage3 := kafkaMessageForTest(&topic, exchangeRateForTest())
	suite.Nil(client.(*MessageClient).processMessage(kafkaNessage3))
}

func (suite *ClientIntegrationTestSuite) TestCanelExecution() {

	client := New(suite.conf, loggerForTest())
	wg := &sync.WaitGroup{}
	ctx, canelFunc := context.WithCancel(context.Background())

	wg.Add(1)
	go client.Run(ctx, wg)

	suite.publishMessages(2)
	time.Sleep(5 * time.Second)
	suite.publishMessages(5)
	time.Sleep(3 * time.Second)

	canelFunc()
	timeout := time.NewTimer(2 * time.Second)
	waitCh := make(chan struct{})
	go waitFor(wg, waitCh)
	select {
	case <-waitCh:
	case <-timeout.C:
		suite.Error(errors.New("Client not stopped as expected."))
	}
	suite.Len(client.(*MessageClient).events[core.DATASOURCE_EXCHANGERATE], 5)
}

func (suite *ClientTestSuite) TestGetMetrics() {

	client := clientForTest(nil)
	addEventsForTest(client)

	metrics := client.Metrics()
	suite.Len(metrics, 1)
}

func (suite *ClientIntegrationTestSuite) publishMessages(numberOfMessages int) {

	for i := 0; i < numberOfMessages; i++ {
		data := marshalProto(exchangeRateForTest(), suite.Assert())
		err := suite.producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &suite.topic, Partition: kafka.PartitionAny},
			Value:          data,
		}, nil)
		suite.Nil(err)
	}
	suite.producer.Flush(15 * 1000)
}

func addEventsForTest(client Client) Client {

	exchangeRate1 := exchangeRateForTest()
	exchangeRate1.Rates[0].Rate = 1.0
	exchangeRate2 := exchangeRateForTest()
	exchangeRate2.Rates[0].Rate = 2.0
	exchangeRate3 := exchangeRateForTest()
	exchangeRate3.Rates[0].Rate = 3.0
	client.(*MessageClient).events[core.DATASOURCE_EXCHANGERATE] = []proto.Message{}
	client.(*MessageClient).events[core.DATASOURCE_EXCHANGERATE] = append(client.(*MessageClient).events[core.DATASOURCE_EXCHANGERATE], exchangeRate1)
	client.(*MessageClient).events[core.DATASOURCE_EXCHANGERATE] = append(client.(*MessageClient).events[core.DATASOURCE_EXCHANGERATE], exchangeRate2)
	client.(*MessageClient).events[core.DATASOURCE_EXCHANGERATE] = append(client.(*MessageClient).events[core.DATASOURCE_EXCHANGERATE], exchangeRate3)
	return client
}

func addProcessedEventsForTest(client Client) Client {

	exchangeRate1 := exchangeRateForTest()
	exchangeRate1.Rates[0].Rate = 1.0
	exchangeRate2 := exchangeRateForTest()
	exchangeRate2.Rates[0].Rate = 2.0
	exchangeRate3 := exchangeRateForTest()
	exchangeRate3.Rates[0].Rate = 3.0
	client.(*MessageClient).appendToChannel(exchangeRate1, core.DATASOURCE_EXCHANGERATE)
	client.(*MessageClient).appendToChannel(exchangeRate2, core.DATASOURCE_EXCHANGERATE)
	client.(*MessageClient).appendToChannel(exchangeRate3, core.DATASOURCE_EXCHANGERATE)
	return client
}
