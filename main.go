package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/hibiken/asynq"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
	"gopkg.in/natefinch/lumberjack.v2"
	"net/http"         // 用于 pprof
	_ "net/http/pprof" // 注册 pprof 路由
	"os"
	"os/signal"
	"scheduler/config" // 引入配置包
	"scheduler/logger" // 引入日志包
	"scheduler/tasks"
	"scheduler/tasks/utils"
	"sync"
	"syscall"
	"time"
)

const (
	// 定义资源阈值
	maxCpuUsage = 60.0
	maxMemUsage = 60.0
)

func ResourceLimiterMiddleware(next asynq.Handler) asynq.Handler {
	return asynq.HandlerFunc(func(ctx context.Context, t *asynq.Task) error {
		// 1. 获取 CPU 使用率
		// 第一个参数是时间间隔，0表示与上次调用之间的时间间隔
		// 第二个参数是 percpu，false 表示获取总体使用率
		cpuPercent, err := cpu.Percent(time.Second, false)
		if err != nil {
			AppLogger.Errorf("Could not get CPU usage: %v", err)
			// 如果无法获取资源信息，可以选择继续执行或返回错误
			// 这里我们选择继续执行，避免监控问题导致任务失败
			return next.ProcessTask(ctx, t)
		}

		// 2. 获取内存使用率
		memInfo, err := mem.VirtualMemory()
		if err != nil {
			AppLogger.Errorf("Could not get Memory usage: %v", err)
			return next.ProcessTask(ctx, t)
		}

		cpuUsage := cpuPercent[0] // gopsutil/cpu.Percent 返回一个切片
		memUsage := memInfo.UsedPercent

		AppLogger.Infof("Current system resource: CPU=%.2f%%, Memory=%.2f%%", cpuUsage, memUsage)

		// 3. 检查资源是否超过阈值
		if cpuUsage > maxCpuUsage || memUsage > maxMemUsage {
			// 如果资源超限，记录日志并返回一个错误
			// asynq 会根据重试设置重新调度此任务
			errMsg := fmt.Sprintf(
				"Task processing deferred due to high system load. CPU: %.2f%% > %.0f%% or Memory: %.2f%% > %.0f%%",
				cpuUsage, maxCpuUsage, memUsage, maxMemUsage,
			)
			AppLogger.Errorf(errMsg)
			// 返回错误，asynq 会自动进行重试
			return fmt.Errorf(errMsg)
		}

		// 4. 如果资源充足，则调用下一个处理器执行任务
		AppLogger.Infof("Resource check passed. Processing task type=%s, id=%s", t.Type(), t.ResultWriter().TaskID())
		return next.ProcessTask(ctx, t)
	})
}

// AppLogger 将作为全局日志记录器
var AppLogger *logger.Logger
var appConfig *config.AppConfig // 全局配置

func main() {
	var err error
	var lumberjackLogger *lumberjack.Logger
	// 1. 加载配置
	appConfig, err = config.GetConfig()
	if err != nil {
		fmt.Println("Failed to load configuration: %v", err)
		panic(1)
	}
	// 输出 appConfig 的内容
	configJSON, _ := json.MarshalIndent(appConfig, "", "  ") // 使用 MarshalIndent 格式化输出
	fmt.Println("Application configuration loaded: \n%s", string(configJSON))
	lumberjackLogger, AppLogger = logger.GenerateNewLogger(appConfig.LogFilePath, appConfig.LogMaxSizeMB, appConfig.LogMaxBackups, appConfig.LogMaxAgeDays, appConfig.LogCompress, appConfig.LogLevel)
	AppLogger.Info("Scheduler service starting...")

	// 2. 初始化日志记录器
	//lumberjackLogger := &lumberjack.Logger{
	//	Filename:   appConfig.LogFilePath,
	//	MaxSize:    appConfig.LogMaxSizeMB,
	//	MaxBackups: appConfig.LogMaxBackups,
	//	MaxAge:     appConfig.LogMaxAgeDays,
	//	Compress:   appConfig.LogCompress,
	//}
	//// defer lumberjackLogger.Close() // Close 将在应用结束时处理
	//
	//AppLogger = logger.New(
	//	logger.WithOutput(lumberjackLogger),
	//	logger.WithLevel(appConfig.LogLevel), // 使用配置中的日志级别
	//	logger.WithCaller(true),
	//)
	//AppLogger.Info("Scheduler service starting...")

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
				appConfig.CriticalQueue:                   10, // 优先级示例
				appConfig.CalculateReportQueue:            60,
				appConfig.MergeParquetFileQueue:           30,
				appConfig.DefaultQueue:                    10,
				appConfig.SecondDownloadQueue:             5,
				appConfig.SummaryReportQueue:              5,
				appConfig.TurbineAvailabilityMetricsQueue: 5,
				appConfig.EfficiencyMetricsQueue:          5,
				appConfig.HistoricEventsQueue:             5, // 历史事件下载
				appConfig.RealtimeEventsQueue:             5, // 实时事件下载
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
	//tasks.SetGlobalLogger(AppLogger)
	tasks.SetGlobalConfig(appConfig)

	utils.InitHTTPClient(appConfig) // 初始化 tasks 包中的 HTTP 客户端
	mux.Use(ResourceLimiterMiddleware)
	mux.HandleFunc(tasks.TypeParquetMerge, tasks.HandleMergeParquetTask)
	mux.HandleFunc(tasks.TypeDownloadSecond, tasks.HandleDownloadParquetTask)
	mux.HandleFunc(tasks.TypeDownloadReport, tasks.HandleDownloadReportTask)
	mux.HandleFunc(tasks.TypeCalculateReport, tasks.HandleCalculateReportTask)
	mux.HandleFunc(tasks.TypeDownloadEvent, tasks.HandleDownloadEventTask)

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
