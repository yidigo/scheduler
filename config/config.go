package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"scheduler/logger" // 引入你的 logger 包
)

// AppConfig 保存应用的所有配置
type AppConfig struct {
	RedisAddr             string
	RedisDB               int
	RedisPassword         string // 新增 Redis 密码配置
	ClickHouseURL         string
	ClickHouseUser        string
	ClickHousePass        string
	LogFilePath           string
	LogMaxSizeMB          int
	LogMaxBackups         int
	LogMaxAgeDays         int
	LogCompress           bool
	LogLevel              logger.Level // 使用 logger 包中定义的 Level 类型
	DefaultQueue          string
	CriticalQueue         string
	MergeParquetFileQueue string
	CalculateReportQueue  string
	SecondQueue           string
	ReportQueues          map[string]string // 将原 Queues map 中的报表队列也纳入配置
	AsynqConcurrency      int
	HTTPClientTimeout     time.Duration
	SecondParquetPath     string
}

// LoadConfig 从环境变量加载配置 (如果存在 .env 文件，则优先加载)
func LoadConfig() (*AppConfig, error) {
	//_ = godotenv.Load() // 尝试加载 .env 文件，忽略错误

	redisDB, err := strconv.Atoi(getEnv("REDIS_DB", "8"))
	if err != nil {
		return nil, fmt.Errorf("invalid REDIS_DB: %w", err)
	}

	logMaxSize, err := strconv.Atoi(getEnv("LOG_MAX_SIZE_MB", "10"))
	if err != nil {
		return nil, fmt.Errorf("invalid LOG_MAX_SIZE_MB: %w", err)
	}
	// ... (为其他数字和布尔类型的配置添加类似的错误处理) ...
	logMaxBackups, _ := strconv.Atoi(getEnv("LOG_MAX_BACKUPS", "3"))
	logMaxAge, _ := strconv.Atoi(getEnv("LOG_MAX_AGE_DAYS", "28"))
	logCompress, _ := strconv.ParseBool(getEnv("LOG_COMPRESS", "true"))
	asynqConcurrency, _ := strconv.Atoi(getEnv("ASYNQ_CONCURRENCY", "3"))
	httpClientTimeoutSec, _ := strconv.Atoi(getEnv("HTTP_CLIENT_TIMEOUT_SECONDS", "30"))

	logLevel, err := logger.StringToLevel(getEnv("LOG_LEVEL", "INFO")) // 使用 StringToLevel
	if err != nil {
		return nil, fmt.Errorf("invalid LOG_LEVEL: %w", err)
	}

	return &AppConfig{
		RedisAddr:             getEnv("REDIS_ADDR", "127.0.0.1:59010"),
		RedisDB:               redisDB,
		RedisPassword:         getEnv("REDIS_PASSWORD", ""), // 默认为空密码
		ClickHouseURL:         getEnv("CLICKHOUSE_URL", "http://127.0.0.1:59011/"),
		ClickHouseUser:        getEnv("CLICKHOUSE_USER", "default"),
		ClickHousePass:        getEnv("CLICKHOUSE_PASS", ""),
		LogFilePath:           getEnv("LOG_FILE_PATH", "./app_rotated.log"),
		LogMaxSizeMB:          logMaxSize,
		LogMaxBackups:         logMaxBackups,
		LogMaxAgeDays:         logMaxAge,
		LogCompress:           logCompress,
		LogLevel:              logLevel,
		DefaultQueue:          getEnv("QUEUE_DEFAULT", "default"),
		CriticalQueue:         getEnv("QUEUE_CRITICAL", "critical"),
		MergeParquetFileQueue: getEnv("QUEUE_MERGE_PARQUET_FILE", "MergeParquetFile"),
		CalculateReportQueue:  getEnv("QUEUE_CALCULATE_REPORT", "CalculateReport"),
		SecondQueue:           getEnv("QUEUE_SECOND", "second"),
		ReportQueues: map[string]string{ // 示例，可以根据需要扩展
			"SummaryReport":              getEnv("QUEUE_SUMMARY_REPORT", "SummaryReport"),
			"TurbineAvailabilityMetrics": getEnv("QUEUE_TURBINE_AVAILABILITY_METRICS", "TurbineAvailabilityMetrics"),
			"EfficiencyMetrics":          getEnv("QUEUE_EFFICIENCY_METRICS", "EfficiencyMetrics"),
		},
		AsynqConcurrency:  asynqConcurrency,
		HTTPClientTimeout: time.Duration(httpClientTimeoutSec) * time.Second,
		SecondParquetPath: getEnv("SECOND_PARQUET_PATH", "/home/data/parquet/"),
	}, nil
}

// getEnv 获取环境变量，如果不存在则返回 fallback 值
func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}
