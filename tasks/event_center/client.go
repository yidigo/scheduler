package event_center

import (
	"context"
	"github.com/pkg/errors"
	"log"
	"scheduler/config"
	"sync"
)

type EventClient struct {
	connector *CenterConnector
	ctx       context.Context
}

func NewEventClient(opt *Option) (c *EventClient, err error) {
	if opt == nil {
		return nil, errors.New("未获取到配置")
	}
	c = new(EventClient)
	c.ctx = context.TODO()
	if opt.Connect == nil {
		return c, errors.New("没有Connector配置")
	}
	c.connector, err = NewCenterConnector(opt.Connect.Username, opt.Connect.Addr, c.ctx)
	if err != nil {
		return c, errors.WithMessage(err, "创建Connector失败")
	}
	return c, nil
}

var once sync.Once
var eventClient *EventClient

func GetEC() *EventClient {
	eventCenterConfig, err := config.GetConfig()
	if err != nil {
		log.Fatalf("Failed to get configuration: %v", err)
	}
	once.Do(func() {
		newEventClient, err := NewEventClient(&Option{

			Connect: &ConnectOption{
				Addr:     eventCenterConfig.EventCenterConfig.Addr,
				Username: eventCenterConfig.EventCenterConfig.Username,
			},
		})
		if err != nil {
			log.Fatal("创建NewEventClient错误:", err)
			return
		}
		eventClient = newEventClient
	})

	return eventClient
}

func (x EventClient) GetHistoricalEventsPage(query Query) ([]Event, int64, error) {
	return x.connector.GetHistoricalEventsPage(query)
}
func (x EventClient) GetRealtimeEvents(query Query) ([]Event, error) {
	return x.connector.GetRealtimeEvents(query)
}
