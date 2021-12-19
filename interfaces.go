package client

import (
	"github.com/golang/protobuf/proto"
	core "github.com/tommzn/hdb-core"
)

type Client interface {

	// Runable core interface. Used to run message fetch n background.
	core.Runable

	// Latest willreturn latest element for given datasource.
	Latest(core.DataSource) (proto.Message, error)

	// All returns all available message for given datasource.
	All(core.DataSource) ([]proto.Message, error)
}
