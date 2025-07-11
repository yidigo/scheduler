package event_center

type Option struct {
	Connect *ConnectOption
}

// ConnectOption 事件中心连接相关配置
type ConnectOption struct {
	Addr     string //请求事件中心的地址
	Username string
}
