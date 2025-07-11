package event_center

import (
	"github.com/spf13/cast"
	"time"

	pb "gitlab.sanywind.net/sany/scada3/go-common/proto/event_center"
)

type EventMessage = pb.EventMessage

// EventTimeToMessageTime 将Time转换成Message需要的Time
func EventTimeToMessageTime(t time.Time) int64 {
	return t.UnixMilli()
}

func MessageToEvents(msg *pb.EventMessage) (events []Event) {
	startTime := time.UnixMilli(msg.StartTime)
	endTime := time.UnixMilli(msg.EndTime)
	confirmedTime := time.UnixMilli(msg.ConfirmedTime)

	firstEventStr := parseFirstEvent(msg.FirstEvent)
	confirmedStr := parseConfirmed(msg.Confirmed)

	event := Event{
		MessageId:     msg.MessageId,
		FarmCode:      msg.FarmCode,
		System:        msg.System,
		Category:      msg.Category,
		Device:        msg.DeviceName,
		DeviceType:    msg.DeviceType,
		Part:          msg.Part,
		StartTime:     startTime,
		EndTime:       endTime,
		EventLevel:    EventLevel(msg.EventLevel),
		EventLevelStr: EventLevel(msg.EventLevel).String(),
		EventCode:     msg.EventCode,
		EventDesc:     msg.EventDesc,
		FirstEvent:    int(msg.FirstEvent),
		FirstEventStr: firstEventStr,
		RelevantId:    msg.RelevantId,
		Attachment:    msg.Attachment,
		Confirmed:     int(msg.Confirmed),
		ConfirmedStr:  confirmedStr,
		Confirmer:     msg.Confirmor,
		ConfirmedTime: confirmedTime,
		Remark:        msg.Remark,
	}
	events = append(events, event)
	for _, message := range msg.Relevant {
		events = append(events, MessageToEvents(message)...)
	}
	return events
}

// fixme 国际化？
func parseConfirmed(flag int32) string {
	if flag == 1 {
		return "已确认"
	}
	return "未确认"
}

// fixme 国际化？
func parseFirstEvent(flag int32) string {
	if flag == 1 {
		return "是"
	}
	return "否"
}

type EventLeveler interface {
	String() string
	FooLevel(value interface{}) EventLevel
}

const (
	Prompt EventLevel = 1 //提示
	Alarm  EventLevel = 2 //告警
	Warn   EventLevel = 3 //预警
	Fault  EventLevel = 4 //故障
)

// fixme 国际化？
func (l EventLevel) String() string {
	switch l {
	case Prompt:
		return "提示"
	case Alarm:
		return "告警"
	case Warn:
		return "预警"
	case Fault:
		return "故障"
	default:
		return ""
	}
}

func (l EventLevel) FooLevel(value interface{}) EventLevel {
	switch value.(type) {
	case string:
		switch value {
		case "提示":
			return Prompt
		case "告警":
			return Alarm
		case "预警":
			return Warn
		case "故障":
			return Fault
		default:
			return 0
		}
	default:
		switch cast.ToInt(value) {
		case int(Prompt):
			return Prompt
		case int(Alarm):
			return Alarm
		case int(Warn):
			return Warn
		case int(Fault):
			return Fault
		default:
			return 0
		}
	}
}

type FarmDeviceEntry struct {
	FarmCode string `json:"farmCode" dc:"风场码"`
	// 风机代码数组，空数组表示查询该风场全部风机
	DeviceNames []string `json:"deviceNames" dc:"设备名称数组"`
}

func ToProtoFarmDeviceEntry(entry *FarmDeviceEntry) *pb.FarmDeviceEntry {
	return &pb.FarmDeviceEntry{
		FarmCode:    entry.FarmCode,
		DeviceCodes: entry.DeviceNames,
	}
}

func ToProtoFarmDeviceEntries(entries []*FarmDeviceEntry) []*pb.FarmDeviceEntry {
	if len(entries) == 0 {
		return make([]*pb.FarmDeviceEntry, 0)
	}

	result := make([]*pb.FarmDeviceEntry, 0, len(entries))
	for _, e := range entries {
		result = append(result, ToProtoFarmDeviceEntry(e))
	}
	return result
}
