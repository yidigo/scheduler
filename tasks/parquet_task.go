package tasks

import (
	"archive/zip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/apache/arrow/go/v18/parquet/file"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
	"github.com/hibiken/asynq"
	"io"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"scheduler/logger"
	"strings"
	"time"
)

//
//// var CHDBURL = "http://10.162.4.16:8123/"
////var CHDBURL = "http://10.162.74.33:8123/"
//
//var CHDBURL = "http://127.0.0.1:59011/"
//
//// var REDISRUL = "10.162.74.33:6379"
//// var REDISRUL = "127.0.0.1:6379"
//var REDISRUL = "127.0.0.1:59010"
//var SECONDPARQUETPATH = "/home/data/parquet/"

const (
	TypeParquetMerge    = "parquet:merge"
	TypeParquetMergeCpp = "parquet:mergeCpp"
	TypeDownloadSecond  = "parquet:download"
	TypeDownloadReport  = "report:download"
	TypeCalculateReport = "report:calculate"
)

type MergeParquetPayload struct {
	UserID     int
	FilePath1  string
	FilePath2  string
	TargetPath string
	DataStr    string
}

type SecondOperation struct {
	Resample  string              `json:"resample" dc:"resample type 10min hour "`
	Merge     bool                `json:"merge" dc:"whether merge into one file "`
	PointType map[string][]string `json:"point_resample" dc:"resample type for each point,point name,avg max min"`
}

type DownloadParquetPayload struct {
	Devices       []string          `v:"required" dc:"device name"`
	Start         time.Time         `v:"required" dc:"trend start time"`
	End           time.Time         `v:"required" dc:"trend end time"`
	Columns       []string          `v:"required" dc:"columns name"`
	TranColumns   map[string]string `v:"required" json:"tran_columns"  dc:"standard name to nick name"`
	Operation     SecondOperation   `v:"required" dc:"data process method"`
	FilePath      string            `dc:"output file path"`
	TaskStartTime time.Time         `dc:"task start time"`
	Language      string            `dc:"language"`
}

//func NewMergeParquetCppTask(userID int, FilePath, ParquetPath, dataStr string) (*asynq.Task, error) {
//	payload, err := json.Marshal(MergeParquetPayload{UserID: userID, FilePath1: FilePath, ParquetPath: ParquetPath, DataStr: dataStr})
//	if err != nil {
//		fmt.Println(err)
//		return nil, err
//	}
//	return asynq.NewTask("parquet:merge", payload), nil
//}
//
//func HandleMergeParquetCppTask(ctx context.Context, t *asynq.Task) error {
//	//接收任务数据.
//	var p MergeParquetPayload
//	if err := json.Unmarshal(t.Payload(), &p); err != nil {
//		return fmt.Errorf("json.Unmarshal failed: %v: %w", err, asynq.SkipRetry)
//	}
//
//	//cmd := exec.Command("/usr/bin/python3", "/home/ccm/package/example11.py", p.FilePath, p.ParquetPath)
//
//	kk := strings.Split(p.FilePath, `/`)
//	kk1 := kk[len(kk)-1]
//	kk2 := p.ParquetPath + kk1[0:len(kk1)-14] + ".parquet"
//	fmt.Println("=================")
//	fmt.Println("cmd:")
//	fmt.Println("/home/dbservice/dll/arrow_table", p.FilePath, kk2)
//	fmt.Println("=================")
//	cmd := exec.Command("/home/dbservice/dll/arrow_table", p.FilePath, kk2)
//
//	ret, err := cmd.Output()
//	if err != nil {
//		fmt.Println("err: ", err)
//	} else {
//		fmt.Println(string(ret))
//	}
//
//	log.Printf("Mergeing Parquet File : user_id=%d, template_id=%s data_str:%s", p.UserID, p.FilePath, p.DataStr)
//	return nil
//}

var fileNameRegex = regexp.MustCompile(`^([A-Z0-9]+)_([0-9]+)_.*`)

func extractIDRegex(filePath string) (string, error) {
	if filePath == "" {
		return "", fmt.Errorf("file path is empty")
	}

	fileName := filepath.Base(filePath)
	matches := fileNameRegex.FindStringSubmatch(fileName)

	// matches[0] is the full matched string
	// matches[1] is the first capture group (e.g., "F1240")
	// matches[2] is the second capture group (e.g., "079")
	if len(matches) < 3 {
		return "", fmt.Errorf("filename format does not match regex: %s", fileName)
	}

	id := matches[1] + "_" + matches[2]
	return id, nil
}

func HandleMergeParquetTask(ctx context.Context, t *asynq.Task) error {
	var p MergeParquetPayload
	//fmt.Println(string(t.Payload()))
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		taskLogger.Error("json.Unmarshal failed for MergeParquetPayload:", err) // 使用 taskLogger
		return fmt.Errorf("json.Unmarshal failed: %v: %w", err, asynq.SkipRetry)
	}

	var sc1, sc2 *arrow.Schema
	var err error
	if FileExists(p.FilePath1) {
		sc1, err = GetParquetSchema(p.FilePath1)
		if err != nil {
			taskLogger.Errorf("Error get file schema : %s\n,%v\n", err)
			return err
		}
	}

	if FileExists(p.FilePath2) {
		sc2, err = GetParquetSchema(p.FilePath2)
		if err != nil {
			taskLogger.Errorf("Error get file schema : %s\n,%v\n", p.FilePath2, err)
			return err
		}
	}

	allSchema, err := MergeSchema(sc1, sc2)
	if err != nil {
		taskLogger.Errorf("Error merge file schema : %s   %s\n,%v\n", p.FilePath1, p.FilePath2, err)
		return err
	}
	deviceID, err1 := extractIDRegex(p.FilePath1)
	if err1 != nil {
		taskLogger.Errorf("Error for device id regex: %v\n", err1)
		return err
	}
	tableName := "temporary_table_" + deviceID
	generateSql := SchemaToClickhouseSQL(allSchema, tableName)
	if FileExists(p.FilePath1) {
		generateSql = generateSql + fmt.Sprintf("INSERT INTO %s FROM INFILE '%s' FORMAT Parquet;\n", tableName, p.FilePath1)
	}
	if FileExists(p.FilePath2) {
		generateSql = generateSql + fmt.Sprintf("INSERT INTO %s FROM INFILE '%s' FORMAT Parquet;\n", tableName, p.FilePath2)
	}
	generateSql = generateSql + fmt.Sprintf("SELECT * FROM %s order by time INTO OUTFILE '%s' TRUNCATE FORMAT Parquet;", tableName, p.TargetPath)
	generateSql = generateSql + fmt.Sprintf("DROP table %s", tableName)
	//fmt.Println(generateSql)

	taskLogger.Debugf("Executing merge Parquet SQL: %s", generateSql)
	result, err := ExecuteCHQueryValues(generateSql) // 使用新的 HTTP client 函数
	if err != nil {
		taskLogger.Errorf("Error executing merge Parquet SQL: %v", err, logger.Fields{"sql": generateSql})
		return err // 返回错误，让 Asynq 处理重试 (如果配置了)
	}
	//check if the generate file influence the original select
	if len(result) == 0 {
		err := os.Remove(p.FilePath2)
		if err != nil {
			taskLogger.Errorf("Error deleting file:", err)
		}
		return nil
	} else {
		taskLogger.Errorf("Error when merge parquet file :%s", result)
		taskLogger.Errorf(string(result))
		return errors.New(string(result))
	}
}

func HandleMergeParquetTaskwithUnion(ctx context.Context, t *asynq.Task) error {
	var p MergeParquetPayload
	//fmt.Println(string(t.Payload()))
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		taskLogger.Error("json.Unmarshal failed for MergeParquetPayload:", err) // 使用 taskLogger
		return fmt.Errorf("json.Unmarshal failed: %v: %w", err, asynq.SkipRetry)
	}
	deviceID, err := extractIDRegex(p.TargetPath)
	if err != nil {
		taskLogger.Errorf("Error for device id regex: %v\n", err)
		return err
	}
	tmpPath := filepath.Dir(p.TargetPath) + "/" + deviceID + ".parquet"
	generateSql := fmt.Sprintf("INSERT INTO FUNCTION file('%s', 'Parquet')\nSELECT * FROM file('%s', 'Parquet')\nUNION ALL\nSELECT * FROM file('%s', 'Parquet');", tmpPath, p.FilePath1, p.FilePath2)
	taskLogger.Infof(generateSql)
	result, err := ExecuteCHQueryValues(generateSql) // 使用新的 HTTP client 函数
	if err != nil {
		taskLogger.Errorf("Error executing merge Parquet SQL: %v", err, logger.Fields{"sql": generateSql})
		return err // 返回错误，让 Asynq 处理重试 (如果配置了)
	}
	//check if the generate file influence the original select
	if len(result) == 0 {
		err := moveFile(tmpPath, p.TargetPath)
		if err != nil {
			taskLogger.Errorf("Error deleting file:", err)
		}
		err = os.Remove(p.FilePath2)
		if err != nil {
			taskLogger.Errorf("Error deleting file:", err)
		}
		return nil
	} else {
		taskLogger.Errorf("Error when merge parquet file :%s", result)
		taskLogger.Errorf(string(result))
		return errors.New(string(result))
	}
}

func moveFile(sourcePath, destPath string) error {
	// 1. Ensure the destination directory exists
	destDir := filepath.Dir(destPath)
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return fmt.Errorf("failed to create destination directory %s: %w", destDir, err)
	}

	// 2. Try to rename (move) the file directly. This is atomic on the same filesystem.
	err := os.Rename(sourcePath, destPath)
	if err == nil {
		fmt.Printf("Successfully moved (renamed) '%s' to '%s'\n", sourcePath, destPath)
		return nil // Success!
	}

	return fmt.Errorf("failed to move file using os.Rename from '%s' to '%s': %w", sourcePath, destPath, err)
}

func HandleDownloadParquetTask(ctx context.Context, t *asynq.Task) error {
	//接收任务数据.
	var p DownloadParquetPayload
	//fmt.Println(t.Payload())
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		taskLogger.Error("json.Unmarshal failed for DownloadParquetPayload:", err)
		return fmt.Errorf("json.Unmarshal failed: %v: %w", err, asynq.SkipRetry)
	}
	if !isValidResampleInterval(p.Operation.Resample) {
		taskLogger.Errorf("Invalid resample interval: %s", p.Operation.Resample)
		return errors.New("invalid resample interval")
	}
	fileDirectory := filepath.Join("/tmp/", "scheduler_downloads", time.Now().Format("20060102150405")+"_"+generateRandomString(8)) // 添加随机后缀增加唯一性
	defer os.RemoveAll(fileDirectory)                                                                                               // 确保临时目录在函数结束时被清理，即使发生错误

	if _, err := os.Stat(fileDirectory); os.IsNotExist(err) {
		err := os.MkdirAll(fileDirectory, os.ModeDir|0755) // Creates parent directories if needed
		if err != nil {
			taskLogger.Errorf("Error creating directory:", err)
			return err
		}
	}

	fileList := make([]string, 0)
	columns := ""
	k := 0
	for _, column := range p.Columns {
		value1, ok1 := p.Operation.PointType[column]
		if ok1 {
			for _, v := range value1 {
				if k > 0 {
					columns = columns + "," + v + "(" + column + ") AS " + p.TranColumns[column] + v
				} else {
					columns += v + "(" + column + ") AS " + p.TranColumns[column] + v
				}
				k = k + 1
			}
		}
	}

	if p.Operation.Merge {
		sql := fmt.Sprintf(`SELECT toStartOfInterval(toDateTime(time), INTERVAL %s) AS interval, %s, turbine FROM file('%s*/*.parquet', Parquet) WHERE time BETWEEN '%s' AND '%s' AND turbine IN (%s) GROUP BY interval,turbine ORDER BY interval,turbine INTO OUTFILE '%s' TRUNCATE FORMAT CSVWithNames`, p.Operation.Resample, columns, taskConfig.SecondParquetPath, p.Start.Format("2006-01-02 15:04:05"), p.End.Format("2006-01-02 15:04:05"), "'"+strings.Join(p.Devices, "','")+"'", fileDirectory+"/turbines.csv")
		result, err := ExecuteCHQueryValues(sql) // 使用新的 HTTP client 函数
		if err != nil {
			taskLogger.Errorf("Error executing download Parquet SQL for device %s: %v", err, logger.Fields{"sql": sql})

		}
		//fmt.Println(sql)
		fileList = append(fileList, fileDirectory+"/turbines.csv")
		taskLogger.Infof("file8path:%s   result:%s  \n", p.FilePath, result)
	} else {
		for _, d := range p.Devices {
			sql := fmt.Sprintf(`SELECT toStartOfInterval(toDateTime(time), INTERVAL %s) AS interval, %s, turbine FROM file('%s*/*.parquet', Parquet) WHERE time BETWEEN '%s' AND '%s' AND turbine IN (%s) GROUP BY interval,turbine ORDER BY interval,turbine INTO OUTFILE '%s' TRUNCATE FORMAT CSVWithNames`, p.Operation.Resample, columns, taskConfig.SecondParquetPath, p.Start.Format("2006-01-02 15:04:05"), p.End.Format("2006-01-02 15:04:05"), "'"+d+"'", fileDirectory+"/"+d+".csv")
			result, err := ExecuteCHQueryValues(sql) // 使用新的 HTTP client 函数
			if err != nil {
				taskLogger.Errorf("Error executing download Parquet SQL for device %s: %v", d, err, logger.Fields{"sql": sql})
				continue // 跳过当前设备
			}
			//fmt.Println(sql)
			fileList = append(fileList, fileDirectory+"/"+d+".csv")
			taskLogger.Infof("filepath:%s   result:%s  \n", p.FilePath, result)
		}

	}

	zipFile, err := os.Create(p.FilePath)
	if err != nil {
		taskLogger.Errorf("Error creating zip file:", err)
		return err
	}

	// Create a zip writer.
	zipWriter := zip.NewWriter(zipFile)

	// Add each file to the zip archive.
	for _, filename := range fileList {
		err := addFileToZip(zipWriter, filename)
		if err != nil {
			taskLogger.Errorf("Error adding file to zip:", err)
			return err
		}
	}

	taskLogger.Infof("Successfully created zip archive:", p.FilePath)
	zipWriter.Close()
	zipFile.Close()
	for _, filepath := range fileList {
		err := os.Remove(filepath) // Use os.Remove to delete a file
		if err != nil {
			taskLogger.Errorf("Error removing file %s: %v\n", filepath, err)
		} else {
			taskLogger.Infof("Successfully removed file: %s\n", filepath)
		}
	}
	return nil
}

// generateRandomString 生成指定长度的随机字符串
func generateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	seededRand := rand.New(rand.NewSource(time.Now().UnixNano()))
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}
func isValidResampleInterval(interval string) bool {
	// 示例：允许 "1 HOUR", "10 MINUTE", "1 DAY" 等
	// 可以用正则表达式或更复杂的解析
	// "1 HOUR", "2 HOUR", "10 MINUTE"
	matched, _ := regexp.MatchString(`^\d+\s+(HOUR|MINUTE|DAY|WEEK|MONTH|QUARTER|YEAR)$`, strings.ToUpper(interval))
	return matched
}

//func GetMetaDataSql(data string) []byte {
//	t1 := time.Now()
//	url := CHDBURL + "?add_http_cors_header=1&default_format=Json&max_result_rows=1000&max_result_bytes=10000000&result_overflow_mode=break"
//
//	req, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(data)))
//	if err != nil {
//		taskLogger.Errorf("Error creating request:", err)
//		return nil
//	}
//
//	req.Header.Set("Accept", "*/*")
//	req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9")
//	req.Header.Set("Authorization", "Basic ZGVmYXVsdDo=")
//	req.Header.Set("Connection", "keep-alive")
//	req.Header.Set("Content-Type", "text/plain;charset=UTF-8")
//	req.Header.Set("DNT", "1")
//	req.Header.Set("Origin", "http://10.162.4.16:59011")
//	req.Header.Set("Referer", "http://10.162.4.16:59011/play?user=default")
//	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36")
//	client := &http.Client{}
//	resp, err := client.Do(req)
//	if err != nil {
//		taskLogger.Errorf("Error making request:", err)
//		return nil
//	}
//	defer resp.Body.Close()
//	content, _ := io.ReadAll(resp.Body)
//
//	//fmt.Println(content)
//	taskLogger.Infof("time cost %s", time.Now().Sub(t1))
//
//	return content
//
//}
//
//func DoSqlProcess(data string) []byte {
//	t1 := time.Now()
//	req, err := http.NewRequest("POST", CHDBURL+"?add_http_cors_header=1&default_format=Values&max_result_rows=1000&max_result_bytes=10000000&result_overflow_mode=break", bytes.NewBuffer([]byte(data)))
//	if err != nil {
//		taskLogger.Errorf("Error creating request:", err)
//		return nil
//	}
//	req.Header.Set("Accept", "*/*")
//	req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9")
//	req.Header.Set("Connection", "keep-alive")
//	req.Header.Set("Content-Type", "text/plain;charset=UTF-8")
//	client := &http.Client{}
//	resp, err := client.Do(req)
//	if err != nil {
//		taskLogger.Errorf("Error making request:", err)
//		return nil
//	}
//	defer resp.Body.Close()
//	content, _ := io.ReadAll(resp.Body)
//	fmt.Println(time.Now().Sub(t1))
//
//	return content
//
//}

func addFileToZip(zipWriter *zip.Writer, filename string) error {
	// Open the file to be added to the zip archive.
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("error opening file %s: %w", filename, err)
	}
	defer file.Close()

	// Get the file information.
	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("error getting file info for %s: %w", filename, err)
	}

	// Create a zip header.  Important:  Set the Name field.
	header, err := zip.FileInfoHeader(fileInfo)
	if err != nil {
		return fmt.Errorf("error creating zip header for %s: %w", filename, err)
	}
	header.Name = filepath.Base(filename) // Use just the filename, not the full path.
	header.Method = zip.Deflate           // Use compression.  zip.Store is no compression.

	// Create a writer for the file within the zip archive.
	writer, err := zipWriter.CreateHeader(header)
	if err != nil {
		return fmt.Errorf("error creating zip entry for %s: %w", filename, err)
	}

	// Copy the file contents to the zip writer.
	_, err = io.Copy(writer, file)
	if err != nil {
		return fmt.Errorf("error copying file content for %s: %w", filename, err)
	}

	return nil
}

func arrowSchemaToClickhouseSQL(arrowSchema *arrow.Schema, tableName string, engine string, orderBy string) string {
	columnDefinitions := make([]string, 0, len(arrowSchema.Fields()))

	for _, field := range arrowSchema.Fields() {
		//to skip columns lens <=1 like % /
		if len(field.Name) <= 1 {
			continue
		}
		columnName := field.Name
		arrowType := field.Type
		clickhouseType := mapArrowTypeToClickhouse(arrowType)
		nullable := field.Nullable

		if nullable && !strings.HasPrefix(clickhouseType, "Nullable(") {
			clickhouseType = fmt.Sprintf("Nullable(%s)", clickhouseType)
		}

		columnDefinitions = append(columnDefinitions, fmt.Sprintf("    %s %s default NULL", columnName, clickhouseType))
	}

	createTableSQL := fmt.Sprintf("CREATE TABLE \"%s\" (%s) ENGINE = %s;\n", tableName, strings.Join(columnDefinitions, ",\n"), engine)

	//if strings.HasPrefix(engine, "MergeTree") && orderBy != "" {
	//	createTableSQL += fmt.Sprintf("ORDER BY %s", orderBy)
	//}

	return createTableSQL
}

func mapArrowTypeToClickhouse(arrowType arrow.DataType) string {
	switch arrowType.ID() {
	case arrow.INT8:
		return "Int8"
	case arrow.INT16:
		return "Int16"
	case arrow.INT32:
		return "Int32"
	case arrow.INT64:
		return "Int64"
	case arrow.UINT8:
		return "UInt8"
	case arrow.UINT16:
		return "UInt16"
	case arrow.UINT32:
		return "UInt32"
	case arrow.UINT64:
		return "UInt64"
	case arrow.FLOAT16:
		return "Float32" // ClickHouse doesn't have Float16
	case arrow.FLOAT32:
		return "Float32"
	case arrow.FLOAT64:
		return "Float64"
	case arrow.BOOL:
		return "Bool"
	case arrow.STRING:
		return "String"
	case arrow.BINARY:
		return "String" // Or FixedString(length) if known length
	case arrow.DATE32:
		return "Date"
	case arrow.DATE64:
		return "DateTime"
	case arrow.TIMESTAMP:
		return "DateTime64" // Consider specifying timezone
	case arrow.NULL:
		return "Nullable(String)" // Sensible Default
	default:
		log.Printf("Unsupported Arrow data type: %v", arrowType)
		return "String" // Sensible default, can error instead
	}
}

func SchemaToClickhouseSQL(arrowSchema *arrow.Schema, tableName string) string {
	// Generate the ClickHouse CREATE TABLE statement
	//tableName := "temporary_table"
	createTableSQL := arrowSchemaToClickhouseSQL(arrowSchema, tableName, "Memory", "")
	//createTableSQL := arrowSchemaToClickhouseSQL(arrowSchema, tableName, "MergeTree()", "")
	//fmt.Println(createTableSQL)
	return createTableSQL
	//// Example usage with Decimal128
	//decimalVal := decimal128.New(123456789, 2) // Example decimal value
	//_ = decimalVal                             // Use decimalVal to prevent "declared and not used" error
	//
	//// Example usage with Float16
	//float16Val := float16.New(1.23)
	//_ = float16Val // Use float16Val to prevent "declared and not used" error
}

func MergeSchema(schema1, schema2 *arrow.Schema) (*arrow.Schema, error) {
	fields := make([]arrow.Field, 0)
	fieldMap := make(map[string]arrow.Field)

	// Add fields from the first schema
	if schema1 != nil {
		for _, field := range schema1.Fields() {
			fields = append(fields, field)
			fieldMap[field.Name] = field
		}
	}

	// Add fields from the second schema that are not already in the first schema
	if schema2 != nil {
		for _, field := range schema2.Fields() {
			if _, exists := fieldMap[field.Name]; !exists {
				fields = append(fields, field)
				fieldMap[field.Name] = field // Ensure the field is added to the map even if not present in the first schema.
			} else {
				// Check for type compatibility if the field exists in both schemas.
				existingField := fieldMap[field.Name]
				if existingField.Type.ID() != field.Type.ID() {
					taskLogger.Errorf("Warning: Conflicting types for field '%s'. Using type from first schema: %s, ignoring type from second schema: %s",
						field.Name, existingField.Type.String(), field.Type.String())
					// Ideally, resolve the type conflict by promoting to a common type, casting, or other strategies.
					// For simplicity, we use the type from the first schema as-is.
				}
			}
		}
	}

	// Create a new schema from the combined fields
	return arrow.NewSchema(fields, nil), nil
}

func GetParquetSchema(parquetPath string) (*arrow.Schema, error) {
	f, err := os.Open(parquetPath)
	if err != nil {
		taskLogger.Errorf("failed to open Parquet file: %w", err)
		return nil, err
	}
	defer f.Close()

	// Create a new ParquetReader
	reader, err := file.NewParquetReader(f)
	if err != nil {
		taskLogger.Errorf("Error creating Parquet reader: %w", err)
		return nil, err
	}
	defer reader.Close()
	mem := memory.NewGoAllocator()

	// Create a ParquetFileReader to Arrow converter
	arrowReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{}, mem)
	if err != nil {
		taskLogger.Errorf("Error creating Arrow reader: %v\n", err)
		return nil, err
	}
	// Get the Arrow schema
	arrowSchema, err := arrowReader.Schema()
	return arrowSchema, nil
}

//func GetParquetSchemaFromCHDB(parquetPath string) (*arrow.Schema, error) {
//	sql1 := fmt.Sprintf("set input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference=1;SELECT columns FROM file('%s', ParquetMetadata)", parquetPath)
//	content1 := GetMetaDataSql(sql1)
//
//	var response ResponseData
//	err := json.Unmarshal(content1, &response)
//	if err != nil {
//		taskLogger.Errorf("Error unmarshalling JSON:", err)
//		return response, err
//	}
//
//	return response, nil
//
//	return arrowSchema, nil
//}

func GetParquetSchemaCHDBDict(parquetPath string) (ResponseData, error) {
	if !isSafePath(parquetPath, taskConfig.SecondParquetPath) && !isSafePath(parquetPath, "/mnt/another_safe_path/") { // 允许多个安全基础路径
		taskLogger.Errorf("Unsafe path for GetParquetSchemaCHDBDict: %s", parquetPath)
		return ResponseData{}, errors.New("unsafe path for schema retrieval")
	}

	sql1 := fmt.Sprintf("set input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference=1;SELECT columns FROM file('%s', ParquetMetadata)", parquetPath)
	content1, err := ExecuteCHQueryJSON(sql1) // 假设元数据以 JSON 返回
	if err != nil {
		taskLogger.Errorf("Error getting Parquet schema from CHDB: %v", err, logger.Fields{"path": parquetPath, "sql": sql1})
		return ResponseData{}, err
	}

	var response ResponseData
	err = json.Unmarshal(content1, &response)
	if err != nil {
		taskLogger.Errorf("Error unmarshalling JSON:", err)
		return response, err
	}
	return response, nil
}

func isSafePath(filePath string, allowedBasePath string) bool {
	absAllowedPath, err := filepath.Abs(allowedBasePath)
	if err != nil {
		return false // 或者 log error
	}
	absFilePath, err := filepath.Abs(filePath)
	if err != nil {
		return false
	}
	return strings.HasPrefix(absFilePath, absAllowedPath) && !strings.Contains(filePath, "..")
}

func FileExists(filename string) bool {
	_, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return true
}

//func ContinuousQueries(parquet1, parquetOut string, interval string) error {
//	sc1, err := GetParquetSchemaCHDBDict(parquet1)
//	if err != nil {
//		taskLogger.Errorf("Error get file schema : %s\n,%v\n", parquet1, err)
//		return err
//	}
//
//	columns := ""
//	k := 0
//	for _, dataItem := range sc1.Data {
//		for _, column := range dataItem.Columns {
//			if column.Name == "time" || column.Name == "/" {
//				continue
//			}
//			if column.LogicalType == "Null" {
//				continue
//			}
//
//			//if k > 0 {
//			//	columns = columns + "," + "avg(coalesce(" + column.Name + ", 0)) AS " + column.Name
//			//} else {
//			//	columns += "avg(coalesce(" + column.Name + ", 0)) AS " + column.Name
//			//}
//			if k > 0 {
//				if column.LogicalType == "String" {
//					//topK(1)(turbine)[1] AS most_frequent_turbine
//					//argMin(turbine,time) AS most_frequent_A
//					columns = columns + "," + "argMin(" + column.Name + ",time) AS " + column.Name
//				} else {
//					columns = columns + "," + "avg(" + column.Name + ") AS " + column.Name
//				}
//
//			} else {
//				if column.LogicalType == "String" {
//					columns += "argMin(" + column.Name + ",time) AS " + column.Name
//				} else {
//					columns += "avg(" + column.Name + ") AS " + column.Name
//				}
//
//			}
//			k = k + 1
//		}
//
//	}
//
//	//avg(coalesce(CnvBMSEnergy, 0)) AS CnvBMSEnergy
//
//	sql := fmt.Sprintf("SELECT toStartOfInterval(time, INTERVAL %s) AS time, %s FROM file('%s', Parquet) GROUP BY time ORDER BY time  INTO OUTFILE '%s' TRUNCATE FORMAT Parquet;", interval, columns, parquet1, parquetOut)
//
//	result := DoSqlProcess(sql)
//	//check if the generate file influence the original select
//	if len(result) == 0 {
//		return nil
//	} else {
//		return errors.New(string(result))
//	}
//}

// ResponseData represents the entire JSON structure.
type ResponseData struct {
	Meta       []MetaItem     `json:"meta"`
	Data       []DataItem     `json:"data"`
	Rows       int            `json:"rows"`
	Statistics StatisticsInfo `json:"statistics"`
}

// MetaItem represents an object within the "meta" array.
type MetaItem struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// DataItem represents an object within the "data" array.
type DataItem struct {
	Columns []ColumnInfo `json:"columns"`
}

// ColumnInfo represents the detailed information for each column.
type ColumnInfo struct {
	Name                  string   `json:"name"`
	Path                  string   `json:"path"`
	MaxDefinitionLevel    uint64   `json:"max_definition_level"`
	MaxRepetitionLevel    uint64   `json:"max_repetition_level"`
	PhysicalType          string   `json:"physical_type"`
	LogicalType           string   `json:"logical_type"`
	Compression           string   `json:"compression"`
	TotalUncompressedSize uint64   `json:"total_uncompressed_size"`
	TotalCompressedSize   uint64   `json:"total_compressed_size"`
	SpaceSaved            string   `json:"space_saved"`
	Encodings             []string `json:"encodings"`
}

// StatisticsInfo represents the "statistics" object.
type StatisticsInfo struct {
	Elapsed   float64 `json:"elapsed"`
	RowsRead  int     `json:"rows_read"`
	BytesRead int     `json:"bytes_read"`
}
