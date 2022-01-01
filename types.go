package client

import (
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/golang/protobuf/proto"
	log "github.com/tommzn/go-log"
	core "github.com/tommzn/hdb-core"
)

type MessageClient struct {
	logger        log.Logger
	pollSleep     time.Duration
	events        map[core.DataSource][]proto.Message
	subscriptions map[string]processConfig
	datasourceMap map[string]core.DataSource
	fetchTimeout  time.Duration
	kafkaConfig   *kafka.ConfigMap
	eventChan     chan proto.Message
	chanFilter    []core.DataSource
}

type processConfig struct {
	datasource core.DataSource
	limit      int
}
