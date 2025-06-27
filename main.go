package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/hibiken/asynq"
	"gopkg.in/natefinch/lumberjack.v2"
	"log"
	"net/http"         // 用于 pprof
	_ "net/http/pprof" // 注册 pprof 路由
	"os"
	"os/signal"
	"scheduler/config" // 引入配置包
	"scheduler/logger" // 引入日志包
	"scheduler/tasks"
	"sync"
	"syscall"
)

// AppLogger 将作为全局日志记录器
var AppLogger *logger.Logger
var appConfig *config.AppConfig // 全局配置

func main() {
	var err error
	// 1. 加载配置
	appConfig, err = config.GetConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}
	// 输出 appConfig 的内容
	configJSON, _ := json.MarshalIndent(appConfig, "", "  ") // 使用 MarshalIndent 格式化输出
	fmt.Println("Application configuration loaded: \n%s", string(configJSON))

	// 2. 初始化日志记录器
	lumberjackLogger := &lumberjack.Logger{
		Filename:   appConfig.LogFilePath,
		MaxSize:    appConfig.LogMaxSizeMB,
		MaxBackups: appConfig.LogMaxBackups,
		MaxAge:     appConfig.LogMaxAgeDays,
		Compress:   appConfig.LogCompress,
	}
	// defer lumberjackLogger.Close() // Close 将在应用结束时处理

	AppLogger = logger.New(
		logger.WithOutput(lumberjackLogger),
		logger.WithLevel(appConfig.LogLevel), // 使用配置中的日志级别
		logger.WithCaller(true),
	)
	AppLogger.Info("Scheduler service starting...")

	// (可选) 启动 pprof 服务器，用于性能分析
	go func() {
		AppLogger.Info("Starting pprof server on :6060")
		if err := http.ListenAndServe("localhost:6060", nil); err != nil {
			AppLogger.Error("pprof server failed to start:", err)
		}
	}()

	// 3. 设置 Asynq Redis 客户端选项
	redisClientOpt := asynq.RedisClientOpt{
		Addr:     appConfig.RedisAddr,
		Password: appConfig.RedisPassword, // 使用配置中的密码
		DB:       appConfig.RedisDB,
	}

	// 4. 创建 Asynq 服务端
	srv := asynq.NewServer(
		redisClientOpt,
		asynq.Config{
			Concurrency: appConfig.AsynqConcurrency,
			Queues: map[string]int{ // 可以从配置中动态构建
				appConfig.CriticalQueue:                              10, // 优先级示例
				appConfig.CalculateReportQueue:                       60,
				appConfig.MergeParquetFileQueue:                      30,
				appConfig.DefaultQueue:                               10,
				appConfig.SecondQueue:                                5,
				appConfig.ReportQueues["SummaryReport"]:              5,
				appConfig.ReportQueues["TurbineAvailabilityMetrics"]: 5,
				appConfig.ReportQueues["EfficiencyMetrics"]:          5,
				appConfig.EventQueues["HistoricEvents"]:              5, // 历史事件下载
				appConfig.EventQueues["RealtimeEvents"]:              5, // 实时事件下载
			},
			StrictPriority: true,
			// 日志记录器 (Asynq V0.24.0+ 支持自定义 logger)
			// Logger: NewAsynqLoggerAdapter(AppLogger), // 需要一个适配器
			ErrorHandler: asynq.ErrorHandlerFunc(func(ctx context.Context, task *asynq.Task, err error) {
				AppLogger.Errorf("Asynq task error: type=%s, payload=%s, err=%v", task.Type(), string(task.Payload()), err)
			}),
			// ... 其他配置
		},
	)

	// 5. 创建任务路由
	mux := asynq.NewServeMux()
	// 传递配置和日志记录器到任务处理函数 (如果需要)
	// 例如: tasks.InitializeHandlers(appConfig, AppLogger)
	// 然后在 tasks 包中使用这些初始化过的实例
	// 或者，如果 task handler 是方法，则在创建 handler 实例时注入

	// 确保在 tasks 包中可以访问到 AppLogger 和 appConfig
	// 一种简单方式是 tasks 包定义 SetLogger 和 SetConfig 函数
	tasks.SetGlobalLogger(AppLogger)
	tasks.SetGlobalConfig(appConfig)

	tasks.InitHTTPClient() // 初始化 tasks 包中的 HTTP 客户端

	mux.HandleFunc(tasks.TypeParquetMerge, tasks.HandleMergeParquetTask)
	mux.HandleFunc(tasks.TypeDownloadSecond, tasks.HandleDownloadParquetTask)
	mux.HandleFunc(tasks.TypeDownloadReport, tasks.HandleDownloadReportTask)
	mux.HandleFunc(tasks.TypeCalculateReport, tasks.HandleCalculateReportTask)
	mux.HandleFunc(tasks.TypeDownloadEvent, tasks.HandleDownloadEventTask) // 历史、实时事件下载

	// 6. 启动服务并处理优雅停机
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := srv.Run(mux); err != nil {
			AppLogger.Fatalf("Could not run Asynq server: %v", err)
		}
		AppLogger.Info("Asynq server stopped.")
	}()

	AppLogger.Info("Scheduler service started successfully.")

	// 监听退出信号
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit // 阻塞直到接收到信号

	AppLogger.Info("Scheduler service shutting down...")

	// 优雅关闭 Asynq 服务
	srv.Shutdown() // Asynq v0.20.0+
	// srv.Stop() // 旧版本可能使用 Stop()，但 Shutdown() 通常更优雅

	// 等待 goroutines 完成 (如果 srv.Run 是阻塞的，上面 go func 里的 wg.Done 会在Run结束后执行)
	// 如果 srv.Run 不是阻塞的，或者有其他需要等待的 goroutines，确保它们被正确处理

	wg.Wait() // 等待 Asynq 服务器完全停止

	// 关闭 lumberjack logger
	if err := lumberjackLogger.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to close lumberjack logger: %v\n", err)
	}

	AppLogger.Info("Scheduler service shutdown complete.")
}
