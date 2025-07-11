package event_center

import (
	"context"
	"github.com/pkg/errors"
	proto "gitlab.sanywind.net/sany/scada3/go-common/proto/event_center"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type CenterConnector struct {
	username      string
	serviceClient proto.EventMessageServiceClient
	ctx           context.Context
}

func NewCenterConnector(username, target string, ctx context.Context) (*CenterConnector, error) {
	conn, err := grpc.Dial(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, errors.WithMessage(err, "创建grpc连接错误")
	}
	return &CenterConnector{
		username:      username,
		serviceClient: proto.NewEventMessageServiceClient(conn),
		ctx:           ctx,
	}, nil
}

func (x CenterConnector) GetRealtimeEvents(query Query) (events []Event, err error) {
	username := query.Username
	if username == "" {
		username = x.username
	}
	response, err := x.serviceClient.GetRealtimeEvent(x.ctx, &proto.GetRealtimeEventRequest{
		Username:    username,
		System:      query.System,
		Category:    query.Category,
		FarmCode:    query.FarmCode,
		DeviceCodes: query.Devices,
		FirstEvent:  query.FirstEvent,
		Confirmed:   query.Confirmed,
		EventLevels: query.EventLevels,
		Order:       query.Order,
	})
	if err != nil {
		return nil, errors.WithMessage(err, "调用接口错误")
	}
	for _, message := range response.Messages {
		events = append(events, x.MessageToEvents(message)...)
	}
	return events, nil
}

// GetHistoricalEventsPage 获取历史事件（分页）
func (x CenterConnector) GetHistoricalEventsPage(query Query) ([]Event, int64, error) {
	var events []Event
	var total int64 = 0
	response, err := x.serviceClient.GetHistoricalEventPage(x.ctx, &proto.GetHistoricalEventRequest{
		Username:          x.username,
		System:            query.System,
		FarmDeviceEntries: query.FarmDeviceEntries,
		Category:          query.Category,
		StartTime:         EventTimeToMessageTime(query.StartTime),
		EndTime:           EventTimeToMessageTime(query.EndTime),
		FirstEvent:        query.FirstEvent,
		Confirmed:         query.Confirmed,
		EventLevels:       query.EventLevels,
		Page:              query.Page,
		Limit:             query.PageSize,
		Order:             query.Order,
	})
	if err != nil {
		return nil, total, err
	}
	total = response.Total
	for _, msg := range response.Messages {
		events = append(events, x.MessageToEvents(msg)...)
	}
	return events, total, nil
}

func (x CenterConnector) MessageToEvents(msg *proto.EventMessage) (events []Event) {
	return MessageToEvents(msg)
}
