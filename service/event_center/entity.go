package event_center

import (
	proto "gitlab.sanywind.net/sany/scada3/go-common/proto/event_center"
	"time"
)

type Event struct {
	MessageId        string     //消息全局唯一id，创建时无需赋值，自动生成
	FarmCode         string     //风场编号
	System           string     //所属系统，如tb：风机，syz：升压站
	Category         string     //事件类别，如event、status
	Device           string     //设备编号
	DeviceType       string     //设备类型
	Part             string     //发生部件
	StartTime        time.Time  //开始时间
	EndTime          time.Time  //结束时间
	EventLevel       EventLevel //事件级别
	EventLevelStr    string     //事件级别中文
	EventCode        string     //事件码
	EventDesc        string     //事件描述
	FirstEvent       int        //首发标识
	FirstEventStr    string     //首发标识中文
	RelevantId       string     //关联消息id，创建时无需赋值，自动生成
	Confirmed        int
	ConfirmedStr     string // 状态中文
	Confirmer        string
	ConfirmedTime    time.Time
	ConfirmedTimeStr string // 未确认直接导出空字符串 fixme
	Remark           string // 确认事件备注
	Attachment       string
	Relevant         []Event
}

type EventLevel int

type Query struct {
	StartTime         time.Time
	EndTime           time.Time
	FarmCode          string
	Category          string
	System            string
	Devices           []string
	Username          string
	FarmDeviceEntries []*proto.FarmDeviceEntry
	FirstEvent        int32
	Confirmed         int32
	EventLevels       []int32
	Page              int64
	PageSize          int64
	Order             string
}
