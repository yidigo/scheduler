package logger

import (
	"encoding/json"
	"fmt"
	"io"
	"log" // 使用标准库 log 进行 FATAL 级别处理和内部错误
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Level 定义了日志级别类型
type Level uint8

const (
	// LevelDebug 定义 Debug 日志级别
	LevelDebug Level = iota
	// LevelInfo 定义 Info 日志级别
	LevelInfo
	// LevelWarn 定义 Warn 日志级别
	LevelWarn
	// LevelError 定义 Error 日志级别
	LevelError
	// LevelFatal 定义 Fatal 日志级别 (将导致程序退出)
	LevelFatal
)

// levelToString 将 Level 类型转换为字符串表示
var levelToString = map[Level]string{
	LevelDebug: "DEBUG",
	LevelInfo:  "INFO",
	LevelWarn:  "WARN",
	LevelError: "ERROR",
	LevelFatal: "FATAL",
}

// String 实现 Stringer 接口，返回级别的字符串表示
func (l Level) String() string {
	if str, ok := levelToString[l]; ok {
		return str
	}
	return "UNKNOWN"
}

// Fields 类型用于传递自定义的结构化日志字段
type Fields map[string]interface{}

// entry 是内部使用的日志条目结构
type entry struct {
	Level     string                 `json:"level"`
	Timestamp string                 `json:"timestamp"`
	Caller    string                 `json:"caller,omitempty"`
	Message   string                 `json:"message"`
	Fields    map[string]interface{} `json:"fields,omitempty"`
}

// Logger 是我们的日志记录器结构
type Logger struct {
	mu         sync.Mutex // 用于并发安全的互斥锁
	out        io.Writer  // 日志输出目标
	level      Level      // 日志记录的最低级别
	timeFormat string     // 时间戳格式
	showCaller bool       // 是否显示调用者信息
	fields     Fields     // Logger 实例的全局字段
	isTerminal bool       // 输出目标是否是终端 (用于可选的彩色输出)
}

// Option 是用于配置 Logger 的函数类型
type Option func(*Logger)

// New 创建一个新的 Logger 实例
// 默认情况下，日志级别为 INFO，输出到 os.Stdout
func New(opts ...Option) *Logger {
	hostname, _ := os.Hostname()
	logger := &Logger{
		out:        os.Stdout,
		level:      LevelInfo,
		timeFormat: time.RFC3339Nano, // 纳秒级精度的时间戳
		showCaller: true,
		fields:     Fields{"hostname": hostname}, // 默认添加 hostname
		isTerminal: isTerminal(os.Stdout),
	}

	for _, opt := range opts {
		opt(logger)
	}
	return logger
}

// --- Logger 配置选项 ---

// WithOutput 设置日志输出目标
func WithOutput(writer io.Writer) Option {
	return func(l *Logger) {
		l.out = writer
		l.isTerminal = isTerminal(writer)
	}
}

// WithLevel 设置日志记录的最低级别
func WithLevel(level Level) Option {
	return func(l *Logger) {
		l.level = level
	}
}

// WithTimeFormat 设置时间戳的格式
func WithTimeFormat(format string) Option {
	return func(l *Logger) {
		l.timeFormat = format
	}
}

// WithCaller 是否显示调用者信息（文件名:行号）
func WithCaller(show bool) Option {
	return func(l *Logger) {
		l.showCaller = show
	}
}

// WithGlobalFields 添加在 Logger 实例生命周期内所有日志都包含的字段
func WithGlobalFields(fields Fields) Option {
	return func(l *Logger) {
		for k, v := range fields {
			l.fields[k] = v
		}
	}
}

// --- 日志记录方法 ---

// log 是实际执行日志记录的内部方法
func (l *Logger) log(level Level, message string, fields Fields, callerSkip int) {
	if level < l.level {
		return
	}

	mergedFields := make(Fields)
	for k, v := range l.fields {
		mergedFields[k] = v
	}
	if fields != nil {
		for k, v := range fields {
			mergedFields[k] = v
		}
	}

	logEntry := entry{
		Level:     level.String(),
		Timestamp: time.Now().Format(l.timeFormat),
		Message:   message,
		Fields:    mergedFields,
	}

	if l.showCaller {
		_, file, line, ok := runtime.Caller(callerSkip) // 使用传入的 callerSkip
		if ok {
			logEntry.Caller = filepath.Base(file) + ":" + strconv.Itoa(line)
		}
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	useColor := l.isTerminal
	var colorPrefix, colorSuffix string
	if useColor {
		switch level {
		case LevelDebug:
			colorPrefix = "\033[36m" // Cyan
		case LevelInfo:
			colorPrefix = "\033[32m" // Green
		case LevelWarn:
			colorPrefix = "\033[33m" // Yellow
		case LevelError, LevelFatal:
			colorPrefix = "\033[31m" // Red
		}
		if colorPrefix != "" {
			colorSuffix = "\033[0m" // Reset color
		}
	}

	jsonData, err := json.Marshal(logEntry)
	if err != nil {
		log.Printf("gologger: JSON marshal error: %v, original entry: %+v", err, logEntry)
		return
	}

	if _, err := fmt.Fprintln(l.out, colorPrefix+string(jsonData)+colorSuffix); err != nil {
		log.Printf("gologger: Output write error: %v, original json: %s", err, string(jsonData))
	}

	if level == LevelFatal {
		os.Exit(1)
	}
}

// Debug 记录 Debug 级别的日志
func (l *Logger) Debug(message string, fields ...Fields) {
	var f Fields
	if len(fields) > 0 {
		f = fields[0]
	}
	l.log(LevelDebug, message, f, 2) // callerSkip = 2 for direct methods
}

// Debugf 记录 Debug 级别的格式化日志
func (l *Logger) Debugf(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	l.log(LevelDebug, message, nil, 2)
}

// Info 记录 Info 级别的日志
func (l *Logger) Info(message string, fields ...Fields) {
	var f Fields
	if len(fields) > 0 {
		f = fields[0]
	}
	l.log(LevelInfo, message, f, 2)
}

// Infof 记录 Info 级别的格式化日志
func (l *Logger) Infof(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	l.log(LevelInfo, message, nil, 2) // callerSkip = 2
}

// Warn 记录 Warn 级别的日志
func (l *Logger) Warn(message string, fields ...Fields) {
	var f Fields
	if len(fields) > 0 {
		f = fields[0]
	}
	l.log(LevelWarn, message, f, 2)
}

// Warnf 记录 Warn 级别的格式化日志
func (l *Logger) Warnf(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	l.log(LevelWarn, message, nil, 2)
}

// Error 记录 Error 级别的日志
func (l *Logger) Error(message string, err error, fields ...Fields) {
	var f Fields
	if len(fields) > 0 {
		f = fields[0]
	} else {
		f = make(Fields)
	}
	if err != nil {
		f["error"] = err.Error()
	}
	l.log(LevelError, message, f, 2)
}

// Errorf 记录 Error 级别的格式化日志
// 注意：Errorf 通常不直接接受 error 对象作为参数，错误信息应包含在格式化字符串中，
// 或者你可以设计一个变体 ErrorfWithError(err error, format string, args...interface{})
// 这里我们遵循标准 Errorf 的模式，如果需要记录 error 对象的详细信息，应在格式化字符串中完成。
// 或者，你可以让 Errorf 自动从 args 中查找 error 类型的参数。
// 为了简单起见，我们这里不自动查找 error。如果需要 error 字段，请使用 Error 方法。
func (l *Logger) Errorf(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	// 如果想在 Errorf 中也包含 error 字段，需要从 args 中提取 error，或者让用户显式处理
	// 例如：
	var errField Fields
	for _, arg := range args {
		if e, ok := arg.(error); ok {
			if errField == nil {
				errField = make(Fields)
			}
			// 注意：这可能会覆盖 fields 中的 "error" (如果有的话)。
			// 或者将所有错误串联起来。简单起见，这里只取第一个找到的错误。
			if _, exists := errField["error"]; !exists {
				errField["error"] = e.Error()
			}
		}
	}
	l.log(LevelError, message, errField, 2) // callerSkip = 2
}

// Fatal 记录 Fatal 级别的日志，然后程序退出
func (l *Logger) Fatal(message string, err error, fields ...Fields) {
	var f Fields
	if len(fields) > 0 {
		f = fields[0]
	} else {
		f = make(Fields)
	}
	if err != nil {
		f["error"] = err.Error()
	}
	l.log(LevelFatal, message, f, 2)
}

// Fatalf 记录 Fatal 级别的格式化日志，然后程序退出
func (l *Logger) Fatalf(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	// 类似于 Errorf，处理 error 字段
	var errField Fields
	for _, arg := range args {
		if e, ok := arg.(error); ok {
			if errField == nil {
				errField = make(Fields)
			}
			if _, exists := errField["error"]; !exists {
				errField["error"] = e.Error()
			}
		}
	}
	l.log(LevelFatal, message, errField, 2)
}

// --- 辅助函数 (保持不变) ---

// isTerminal 检查 writer 是否是终端
func isTerminal(writer io.Writer) bool {
	if f, ok := writer.(*os.File); ok {
		stat, err := f.Stat()
		if err == nil {
			return (stat.Mode() & os.ModeCharDevice) == os.ModeCharDevice
		}
	}
	return false
}

// StringToLevel 将字符串转换为 Level 类型
func StringToLevel(levelStr string) (Level, error) {
	switch strings.ToUpper(levelStr) {
	case "DEBUG":
		return LevelDebug, nil
	case "INFO":
		return LevelInfo, nil
	case "WARN":
		return LevelWarn, nil
	case "ERROR":
		return LevelError, nil
	case "FATAL":
		return LevelFatal, nil
	default:
		return LevelInfo, fmt.Errorf("未知的日志级别: %s", levelStr)
	}
}
