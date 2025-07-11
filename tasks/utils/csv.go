package utils

import (
	"encoding/csv"
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"
)

// ColumnConfig 定义列配置
type ColumnConfig struct {
	Field      string // 字段名
	Title      string // 列标题（国际化）
	TimeFormat string // 时间格式（可选）
}

// MapToColumnConfigs 将map切片转换为ColumnConfig切片
// input: []map[string]string 格式的列配置
// 返回值: []ColumnConfig 和 error
func MapToColumnConfigs(input []map[string]string) ([]ColumnConfig, error) {
	result := make([]ColumnConfig, 0, len(input))

	for _, m := range input {
		// 检查必要的字段
		if m["field"] == "" {
			return nil, fmt.Errorf("missing required 'field' in column config")
		}
		if m["title"] == "" {
			return nil, fmt.Errorf("missing required 'title' in column config for field: %s", m["field"])
		}

		config := ColumnConfig{
			Field:      m["field"],
			Title:      m["title"],
			TimeFormat: m["time_format"], // 可选字段
		}

		result = append(result, config)
	}

	return result, nil
}

// ConvertMapListToCSV 将map列表导出为CSV文件
// data: map切片数据
// columns: 列配置数组，定义字段名、标题和时间格式
// filePath: CSV文件保存路径
func ConvertMapListToCSV(data []map[string]interface{}, rawColumns []map[string]string, filePath string) error {
	// 创建CSV文件
	//file, err := os.Create("exports/events-realtime.csv") // debug
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// 添加UTF-8 BOM头解决中文乱码
	file.WriteString("\xEF\xBB\xBF")

	writer := csv.NewWriter(file)
	defer writer.Flush()

	columns, err := MapToColumnConfigs(rawColumns)
	if err != nil {
		return err
	}

	// 仅在列配置不为空时导出表头
	if len(columns) > 0 {
		headerRow := make([]string, len(columns))
		for i, col := range columns {
			headerRow[i] = col.Title
		}

		if err := writer.Write(headerRow); err != nil {
			return err
		}
	}

	// 如果数据为空，只导出表头（如果有）
	if len(data) == 0 {
		return nil
	}

	// 写入数据行
	for _, row := range data {
		record := make([]string, len(columns))
		for i, col := range columns {
			val, exists := row[col.Field]
			if !exists {
				record[i] = "" // 字段不存在留空
				continue
			}

			// 类型安全转换
			record[i] = convertToString(val, col.TimeFormat)
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}

	return nil
}

// convertToString 将任意类型转换为字符串，支持自定义时间格式
func convertToString(val interface{}, timeFormat string) string {
	switch v := val.(type) {
	case string:
		return v
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprint(v)
	case float32, float64:
		return fmt.Sprint(v)
	case bool:
		return fmt.Sprint(v)
	case time.Time:
		return formatTime(v, timeFormat)
	case *time.Time:
		if v == nil {
			return ""
		}
		return formatTime(*v, timeFormat)
	default:
		// 尝试处理嵌套结构体
		if str, ok := tryConvertStruct(val); ok {
			return str
		}
		// 最后尝试使用默认格式化
		return fmt.Sprintf("%v", val)
	}
}

// formatTime 格式化时间，支持自定义格式
func formatTime(t time.Time, format string) string {
	if format == "" {
		// 默认格式：RFC3339 (包含日期和时间)
		return t.Format(time.RFC3339)
	}
	return t.Format(format)
}

// tryConvertStruct 尝试转换结构体为字符串
func tryConvertStruct(v interface{}) (string, bool) {
	val := reflect.ValueOf(v)
	if val.Kind() == reflect.Struct {
		var sb strings.Builder
		t := val.Type()

		sb.WriteString("{")
		for i := 0; i < val.NumField(); i++ {
			if i > 0 {
				sb.WriteString(", ")
			}
			fieldName := t.Field(i).Name
			fieldValue := val.Field(i).Interface()
			sb.WriteString(fieldName + ":" + fmt.Sprint(fieldValue))
		}
		sb.WriteString("}")
		return sb.String(), true
	}
	return "", false
}

// ConvertToMapList 通用方法：将实体列表转换为map列表
// entities: 任意结构体切片
// fieldNames: 需要包含的字段列表（可选，为空时包含所有字段）
// 返回值: []map[string]interface{}
func ConvertToMapList(entities interface{}, fieldNames ...string) ([]map[string]interface{}, error) {
	val := reflect.ValueOf(entities)
	if val.Kind() != reflect.Slice {
		return nil, fmt.Errorf("input must be a slice")
	}

	length := val.Len()
	result := make([]map[string]interface{}, length)

	// 处理空切片
	if length == 0 {
		return result, nil
	}

	// 获取第一个元素类型以确定字段
	first := val.Index(0)
	if first.Kind() == reflect.Ptr {
		first = first.Elem()
	}
	if first.Kind() != reflect.Struct {
		return nil, fmt.Errorf("slice elements must be structs or pointers to structs")
	}

	// 确定要包含的字段
	fieldsToInclude := make(map[string]bool)
	if len(fieldNames) > 0 {
		for _, name := range fieldNames {
			fieldsToInclude[name] = true
		}
	} else {
		// 未指定字段时包含所有可导出字段
		t := first.Type()
		for i := 0; i < first.NumField(); i++ {
			field := t.Field(i)
			if field.PkgPath == "" { // 仅导出公共字段
				fieldsToInclude[field.Name] = true
			}
		}
	}

	// 遍历切片
	for i := 0; i < length; i++ {
		item := val.Index(i)
		if item.Kind() == reflect.Ptr {
			if item.IsNil() {
				result[i] = make(map[string]interface{})
				continue
			}
			item = item.Elem()
		}

		itemMap := make(map[string]interface{})
		t := item.Type()

		// 遍历结构体字段
		for j := 0; j < item.NumField(); j++ {
			field := t.Field(j)
			fieldValue := item.Field(j)

			// 跳过未指定字段
			if !fieldsToInclude[field.Name] {
				continue
			}

			// 处理指针字段
			if fieldValue.Kind() == reflect.Ptr {
				if fieldValue.IsNil() {
					itemMap[field.Name] = nil
				} else {
					itemMap[field.Name] = fieldValue.Elem().Interface()
				}
			} else {
				itemMap[field.Name] = fieldValue.Interface()
			}
		}

		result[i] = itemMap
	}

	return result, nil
}
