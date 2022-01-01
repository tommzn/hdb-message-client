package client

import (
	"github.com/stretchr/testify/suite"
	"testing"
	"time"

	config "github.com/tommzn/go-config"
	core "github.com/tommzn/hdb-core"
	events "github.com/tommzn/hdb-events-go"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type UtilsTestSuite struct {
	suite.Suite
}

func TestUtilsTestSuite(t *testing.T) {
	suite.Run(t, new(UtilsTestSuite))
}

func (suite *UtilsTestSuite) TestConvertToEvent() {

	exchangeRate := exchangeRateForTest()
	exchangeRateData := marshalProto(exchangeRate, suite.Assert())
	event, err := toEvent(exchangeRateData, core.DATASOURCE_EXCHANGERATE)
	suite.Nil(err)
	exchangeRate2, ok := event.(*events.ExchangeRate)
	suite.True(ok)
	suite.Equal(exchangeRate.FromCurrency, exchangeRate2.FromCurrency)
	suite.Equal(exchangeRate.Rate, exchangeRate2.Rate)
	suite.timestampEqual(exchangeRate.Timestamp, exchangeRate2.Timestamp)

	indoorClimate := &events.IndoorClimate{
		DeviceId:  "e775bf78-1713-492b-8f07-cd853649073b",
		Timestamp: timestamppb.New(time.Now()),
		Type:      events.MeasurementType_TEMPERATURE,
		Value:     "45.6",
	}
	indoorClimateData := marshalProto(indoorClimate, suite.Assert())
	event2, err2 := toEvent(indoorClimateData, core.DATASOURCE_INDOORCLIMATE)
	suite.Nil(err2)
	indoorClimate2, ok2 := event2.(*events.IndoorClimate)
	suite.True(ok2)
	suite.Equal(indoorClimate.DeviceId, indoorClimate2.DeviceId)
	suite.timestampEqual(indoorClimate.Timestamp, indoorClimate2.Timestamp)
	suite.Equal(indoorClimate.Type, indoorClimate2.Type)
	suite.Equal(indoorClimate.Value, indoorClimate2.Value)

	weather := &events.WeatherData{
		Location: &events.Location{
			Longitude: 123.456,
			Latitude:  654.321,
		},
		Current: &events.CurrentWeather{
			Timestamp:   timestamppb.New(time.Now()),
			Temperature: 23.5,
			WindSpeed:   21.6,
			Weather: &events.WeatherDetails{
				ConditionId: 111,
				Group:       "222",
				Description: "333",
				Icon:        "444",
			},
		},
		Forecast: []*events.ForecastWeather{},
	}
	weatherData := marshalProto(weather, suite.Assert())
	event3, err3 := toEvent(weatherData, core.DATASOURCE_WEATHER)
	suite.Nil(err3)
	weatherData3, ok3 := event3.(*events.WeatherData)
	suite.True(ok3)
	suite.Equal(weather.Current.Temperature, weatherData3.Current.Temperature)
	suite.timestampEqual(weather.Current.Timestamp, weatherData3.Current.Timestamp)
	suite.Equal(weather.Current.WindSpeed, weatherData3.Current.WindSpeed)
	suite.Equal(weather.Current.Weather.ConditionId, weatherData3.Current.Weather.ConditionId)
	suite.Equal(weather.Current.Weather.Group, weatherData3.Current.Weather.Group)
	suite.Equal(weather.Current.Weather.Description, weatherData3.Current.Weather.Description)
	suite.Equal(weather.Current.Weather.Icon, weatherData3.Current.Weather.Icon)

	billingReport := billingReportForTest()
	billingReport.BillingAmount["xxx"] = 12.43
	billingReport.TaxAmount["xxx"] = 4.65
	billingReportData := marshalProto(billingReport, suite.Assert())
	event4, err4 := toEvent(billingReportData, core.DATASOURCE_BILLINGREPORT)
	suite.Nil(err4)
	billingReport4, ok4 := event4.(*events.BillingReport)
	suite.True(ok4)
	suite.Equal(billingReport.BillingPeriod, billingReport4.BillingPeriod)
	suite.Equal(billingReport.BillingAmount, billingReport4.BillingAmount)
	suite.Equal(billingReport.TaxAmount, billingReport4.TaxAmount)

	nilEvent, err := toEvent(exchangeRateData, "xxx")
	suite.NotNil(err)
	suite.Nil(nilEvent)
}

func (suite *UtilsTestSuite) TestGenerateRandomId() {

	length := 6
	id := randomId(length)
	suite.NotNil(id)
	suite.True(len(id) == length)
}

func (suite *UtilsTestSuite) TestGetTopicSubscriptions() {

	expectedTopics := []string{"topic1", "topics3", "Topic"}
	subsriptions := make(map[string]processConfig)
	for _, topic := range expectedTopics {
		subsriptions[topic] = processConfig{}
	}
	topics := topicsToSubscribe(subsriptions)
	suite.Len(topics, 3)
	for _, topic := range expectedTopics {
		suite.True(contains(topics, topic))
	}
}

func (suite *UtilsTestSuite) TestGetKafkaConfig() {

	conf1 := loadConfigForTest(nil)
	kafkaConfig1 := newKafkaConfig(conf1)
	val1, err1 := kafkaConfig1.Get("bootstrap.servers", nil)
	suite.Nil(err1)
	suite.Equal("localhost", val1)

	conf2 := loadConfigForTest(config.AsStringPtr("fixtures/testconfig1.yml"))
	kafkaConfig2 := newKafkaConfig(conf2)
	val2, err2 := kafkaConfig2.Get("bootstrap.servers", nil)
	suite.Nil(err2)
	suite.Equal("1.2.3.4", val2.(string))
}

func (suite *UtilsTestSuite) TestGetSubscriptions() {

	conf1 := loadConfigForTest(nil)
	subsriptions1 := getSubscriptions(conf1)
	suite.Len(subsriptions1, 0)

	conf2 := loadConfigForTest(config.AsStringPtr("fixtures/testconfig1.yml"))
	subsriptions2 := getSubscriptions(conf2)
	suite.Len(subsriptions2, 4)

	topicConf1, ok1 := subsriptions2["Topic"]
	suite.True(ok1)
	suite.Equal(3, topicConf1.limit)
	suite.Equal(core.DATASOURCE_WEATHER, topicConf1.datasource)

	topicConf2, ok2 := subsriptions2["topic4"]
	suite.True(ok2)
	suite.Equal(1, topicConf2.limit)
}

func (suite *UtilsTestSuite) timestampEqual(expextec, curent *timestamppb.Timestamp) {
	suite.Equal(expextec.AsTime().Format(time.RFC3339), curent.AsTime().Format(time.RFC3339))
}

func contains(list []string, expectedElement string) bool {
	for _, item := range list {
		if item == expectedElement {
			return true
		}
	}
	return false
}
