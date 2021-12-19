[![Go Reference](https://pkg.go.dev/badge/github.com/tommzn/hdb-message-client.svg)](https://pkg.go.dev/github.com/tommzn/hdb-message-client)
![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/tommzn/hdb-message-client)
![GitHub release (latest by date)](https://img.shields.io/github/v/release/tommzn/hdb-message-client)
[![Go Report Card](https://goreportcard.com/badge/github.com/tommzn/hdb-message-client)](https://goreportcard.com/report/github.com/tommzn/hdb-message-client)
[![Actions Status](https://github.com/tommzn/hdb-message-client/actions/workflows/go.pkg.auto-ci.yml/badge.svg)](https://github.com/tommzn/hdb-message-client/actions)

# HomeDashboard Message Client
Message client receives messages from given Kafka topics. Consumers, e.g. a renderer, can use this client to get latest or all messages.

## Config
By config you can define Kafka servers and a queue to topic forwarding. If no topic is specified given queue name is taken.
```yaml
kafka:
  servers: localhost
  poll_sleep: 1s
  fetch_timeout: 1s
  subsriptions:
    - topic: data.
      limit: "5"
      datasource: exchangerate
```
### fetch_timeout
Timeout to fetch new messages. Client will pause message consumption if there're no new messages. Default is 3 seconds.

### poll_sleep
If there're no new message client will pause message consumption for given time. Default is 10 minutes.

### subsriptions
Defines topics you want to listen to. For HomeDashboard topics retrieved messages are converted to [events](https://github.com/tommzn/hdb-events). Target event will be defined by datasource. For available datasources see [Datasources](https://github.com/tommzn/hdb-core). Number of which will be hold in a stack can be defined by limit param, default is 1 message.

# Links
- [HomeDashboard Documentation](https://github.com/tommzn/hdb-docs/wiki)
