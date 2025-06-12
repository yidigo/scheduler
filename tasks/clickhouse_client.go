package tasks

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"time"
	// "encoding/base64" // 如果需要动态构建 Basic Auth
)

var httpClient *http.Client

// InitHTTPClient 初始化共享的 HTTP 客户端
// 应该在 main 函数中，配置加载之后调用
func InitHTTPClient() {
	if taskConfig == nil {
		// 通常 InitHTTPClient 会在 taskConfig 初始化后调用，这里可以加个日志或panic
		// taskLogger.Fatal("Task configuration not initialized before HTTP client")
		// For now, assume taskConfig is initialized
	}
	httpClient = &http.Client{
		Timeout: taskConfig.HTTPClientTimeout, // 从配置获取超时
	}
}

// doClickHouseRequest 是一个通用的 ClickHouse 请求函数
func doClickHouseRequest(query string, format string) ([]byte, error) {
	if httpClient == nil {
		return nil, fmt.Errorf("http client not initialized")
	}
	if taskConfig == nil {
		return nil, fmt.Errorf("task configuration not initialized")
	}

	// 构建 URL，可以加入更多参数如 default_format
	// CHDBURL 应该以 / 结尾
	url := fmt.Sprintf("%s?add_http_cors_header=1&default_format=%s&max_result_rows=10000&max_result_bytes=100000000&result_overflow_mode=break", taskConfig.ClickHouseURL, format)
	// 注意：原先的 max_result_rows 和 bytes 比较小，这里适当增大了，请根据实际调整

	req, err := http.NewRequest("POST", url, bytes.NewBufferString(query))
	if err != nil {
		taskLogger.Errorf("Error creating ClickHouse request: %v", err, nil)
		return nil, fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9") // 可选
	req.Header.Set("Connection", "keep-alive")          // HTTP/1.1 默认 keep-alive
	req.Header.Set("Content-Type", "text/plain;charset=UTF-8")
	// req.Header.Set("DNT", "1") // 可选
	// req.Header.Set("Origin", taskConfig.ClickHouseURL) // Origin 可能需要更精确
	// req.Header.Set("Referer", taskConfig.ClickHouseURL + "play?user=" + taskConfig.ClickHouseUser) // Referer 同上

	// Basic Authentication
	if taskConfig.ClickHouseUser != "" {
		// log.Printf("Using ClickHouse user: %s, pass: %s", taskConfig.ClickHouseUser, taskConfig.ClickHousePass)
		req.SetBasicAuth(taskConfig.ClickHouseUser, taskConfig.ClickHousePass)
	}

	startTime := time.Now()
	resp, err := httpClient.Do(req)
	if err != nil {
		taskLogger.Errorf("Error making ClickHouse request: %v", err, nil)
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	duration := time.Since(startTime)
	taskLogger.Debugf("ClickHouse query executed. Format: %s, Duration: %s, Status: %s", format, duration, resp.Status)

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		taskLogger.Errorf("ClickHouse request failed: status=%s, body=%s, query=%s", resp.Status, string(bodyBytes), query, nil)
		return nil, fmt.Errorf("clickhouse error (status %d): %s", resp.StatusCode, string(bodyBytes))
	}

	content, err := io.ReadAll(resp.Body)
	if err != nil {
		taskLogger.Errorf("Error reading ClickHouse response body: %v", err, nil)
		return nil, fmt.Errorf("reading response body: %w", err)
	}
	return content, nil
}

// ExecuteCHQueryValues 执行 ClickHouse 查询并期望 Values 格式响应 (通常是影响行数或简单结果)
func ExecuteCHQueryValues(query string) ([]byte, error) {
	return doClickHouseRequest(query, "Values")
}

// ExecuteCHQueryJSON 执行 ClickHouse 查询并期望 JSON 格式响应
func ExecuteCHQueryJSON(query string) ([]byte, error) {
	return doClickHouseRequest(query, "JSON")
}
