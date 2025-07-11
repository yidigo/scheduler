package utils

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"scheduler/config"
	// "encoding/base64" // 如果需要动态构建 Basic Auth
)

var httpClient *http.Client

type Statistics struct {
	Elapsed   float64 `json:"elapsed"`
	RowsRead  uint64  `json:"rows_read"`
	BytesRead uint64  `json:"bytes_read"`
}

type CHDBJsonStruct struct {
	Meta []struct {
		Name string `json:"name"`
		Type string `json:"type"`
	} `json:"meta"`
	Data       []map[string]interface{} `json:"data"` // Expecting data as slice of maps
	Rows       int                      `json:"rows"`
	Statistics struct {
		Elapsed   float64 `json:"elapsed"`
		RowsRead  uint64  `json:"rows_read"`
		BytesRead uint64  `json:"bytes_read"`
	} `json:"statistics"`
}
type Meta struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// InitHTTPClient 初始化共享的 HTTP 客户端
// 应该在 main 函数中，配置加载之后调用
func InitHTTPClient(c *config.AppConfig) {
	httpClient = &http.Client{
		Timeout: c.HTTPClientTimeout, // 从配置获取超时
	}
}

// doClickHouseRequest 是一个通用的 ClickHouse 请求函数
func doClickHouseRequest(c *config.AppConfig, query string, format string) ([]byte, error) {
	if httpClient == nil {
		return nil, fmt.Errorf("http client not initialized")
	}

	if c == nil {
		return nil, fmt.Errorf("task configuration not initialized")
	}
	// 构建 URL，可以加入更多参数如 default_format
	// CHDBURL 应该以 / 结尾
	url := fmt.Sprintf("%s?add_http_cors_header=1&default_format=%s&max_result_rows=10000&max_result_bytes=100000000&result_overflow_mode=break", c.ClickHouseURL, format)
	// 注意：原先的 max_result_rows 和 bytes 比较小，这里适当增大了，请根据实际调整

	req, err := http.NewRequest("POST", url, bytes.NewBufferString(query))
	if err != nil {
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
	if c.ClickHouseUser != "" {
		// log.Printf("Using ClickHouse user: %s, pass: %s", taskConfig.ClickHouseUser, taskConfig.ClickHousePass)
		req.SetBasicAuth(c.ClickHouseUser, c.ClickHousePass)
	}

	//startTime := time.Now()
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	//duration := time.Since(startTime)

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("clickhouse error (status %d): %s", resp.StatusCode, string(bodyBytes))
	}

	content, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body: %w", err)
	}
	return content, nil
}

// ExecuteCHQueryValues 执行 ClickHouse 查询并期望 Values 格式响应 (通常是影响行数或简单结果)
func ExecuteCHQueryValues(c *config.AppConfig, query string) ([]byte, error) {
	return doClickHouseRequest(c, query, "Values")
}

// ExecuteCHQueryJSON 执行 ClickHouse 查询并期望 JSON 格式响应
func ExecuteCHQueryJSON(c *config.AppConfig, query string) ([]byte, error) {
	return doClickHouseRequest(c, query, "JSON")
}
