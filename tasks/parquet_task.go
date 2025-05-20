package tasks

import (
	"archive/zip"
	"bytes"
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
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// var CHDBURL = "http://10.162.4.16:8123/"
//var CHDBURL = "http://10.162.74.33:8123/"

var CHDBURL = "http://127.0.0.1:59011/"

// var REDISRUL = "10.162.74.33:6379"
// var REDISRUL = "127.0.0.1:6379"
var REDISRUL = "127.0.0.1:59010"
var SECONDPARQUETPATH = "/home/data/parquet/"

const (
	TypeParquetMerge    = "parquet:merge"
	TypeParquetMergeCpp = "parquet:mergeCpp"
	TypeDownloadSecond  = "parquet:download"
	TypeDownloadReport  = "report:download"
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

func HandleMergeParquetTask(ctx context.Context, t *asynq.Task) error {
	var p MergeParquetPayload
	//fmt.Println(string(t.Payload()))
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("json.Unmarshal failed: %v: %w", err, asynq.SkipRetry)
	}

	var sc1, sc2 *arrow.Schema
	var err error
	if FileExists(p.FilePath1) {
		sc1, err = GetParquetSchema(p.FilePath1)
		if err != nil {
			fmt.Printf("Error get file schema : %s\n,%v\n", p.FilePath1, err)
			return err
		}
	}

	if FileExists(p.FilePath2) {
		sc2, err = GetParquetSchema(p.FilePath2)
		if err != nil {
			fmt.Printf("Error get file schema : %s\n,%v\n", p.FilePath2, err)
			return err
		}
	}

	allSchema, err := MergeSchema(sc1, sc2)
	if err != nil {
		fmt.Printf("Error merge file schema : %s   %s\n,%v\n", p.FilePath1, p.FilePath2, err)
		return err
	}
	tableName := "temporary_table_" + string(rand.Intn(1000))
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

	result := DoSqlProcess(generateSql)
	//check if the generate file influence the original select
	if len(result) == 0 {
		err := os.Remove(p.FilePath2)
		if err != nil {
			fmt.Println("Error deleting file:", err)
		}
		return nil
	} else {
		return errors.New(string(result))
	}
}

func HandleDownloadParquetTask(ctx context.Context, t *asynq.Task) error {
	//接收任务数据.
	var p DownloadParquetPayload
	//fmt.Println(t.Payload())
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("json.Unmarshal failed: %v: %w", err, asynq.SkipRetry)
	}

	fileDirectory := filepath.Join("/tmp/", time.Now().Format("20060102150405"))
	if _, err := os.Stat(fileDirectory); os.IsNotExist(err) {
		err := os.MkdirAll(fileDirectory, os.ModeDir|0755) // Creates parent directories if needed
		if err != nil {
			fmt.Println("Error creating directory:", err)
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
		sql := fmt.Sprintf(`SELECT toStartOfInterval(toDateTime(time), INTERVAL %s) AS interval, %s, turbine FROM file('%s*/*.parquet', Parquet) WHERE time BETWEEN '%s' AND '%s' AND turbine IN (%s) GROUP BY interval,turbine ORDER BY interval,turbine INTO OUTFILE '%s' TRUNCATE FORMAT CSVWithNames`, p.Operation.Resample, columns, SECONDPARQUETPATH, p.Start.Format("2006-01-02 15:04:05"), p.End.Format("2006-01-02 15:04:05"), "'"+strings.Join(p.Devices, "','")+"'", fileDirectory+"/turbines.csv")
		result := DoSqlProcess(sql)
		//fmt.Println(sql)
		fileList = append(fileList, fileDirectory+"/turbines.csv")
		fmt.Printf("file8path:%s   result:%s  \n", p.FilePath, result)
	} else {
		for _, d := range p.Devices {
			sql := fmt.Sprintf(`SELECT toStartOfInterval(toDateTime(time), INTERVAL %s) AS interval, %s, turbine FROM file('%s*/*.parquet', Parquet) WHERE time BETWEEN '%s' AND '%s' AND turbine IN (%s) GROUP BY interval,turbine ORDER BY interval,turbine INTO OUTFILE '%s' TRUNCATE FORMAT CSVWithNames`, p.Operation.Resample, columns, SECONDPARQUETPATH, p.Start.Format("2006-01-02 15:04:05"), p.End.Format("2006-01-02 15:04:05"), "'"+d+"'", fileDirectory+"/"+d+".csv")
			result := DoSqlProcess(sql)
			//fmt.Println(sql)
			fileList = append(fileList, fileDirectory+"/"+d+".csv")
			fmt.Printf("filepath:%s   result:%s  \n", p.FilePath, result)
		}

	}

	zipFile, err := os.Create(p.FilePath)
	if err != nil {
		fmt.Println("Error creating zip file:", err)
		return err
	}

	// Create a zip writer.
	zipWriter := zip.NewWriter(zipFile)

	// Add each file to the zip archive.
	for _, filename := range fileList {
		err := addFileToZip(zipWriter, filename)
		if err != nil {
			fmt.Println("Error adding file to zip:", err)
			return err
		}
	}

	fmt.Println("Successfully created zip archive:", p.FilePath)
	zipWriter.Close()
	zipFile.Close()
	for _, filepath := range fileList {
		err := os.Remove(filepath) // Use os.Remove to delete a file
		if err != nil {
			fmt.Printf("Error removing file %s: %v\n", filepath, err)
		} else {
			fmt.Printf("Successfully removed file: %s\n", filepath)
		}
	}
	return nil
}

func GetMetaDataSql(data string) []byte {
	t1 := time.Now()
	url := CHDBURL + "?add_http_cors_header=1&default_format=Json&max_result_rows=1000&max_result_bytes=10000000&result_overflow_mode=break"

	req, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(data)))
	if err != nil {
		fmt.Println("Error creating request:", err)
		return nil
	}

	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9")
	req.Header.Set("Authorization", "Basic ZGVmYXVsdDo=")
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("Content-Type", "text/plain;charset=UTF-8")
	req.Header.Set("DNT", "1")
	req.Header.Set("Origin", "http://10.162.4.16:59011")
	req.Header.Set("Referer", "http://10.162.4.16:59011/play?user=default")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Error making request:", err)
		return nil
	}
	defer resp.Body.Close()
	content, _ := io.ReadAll(resp.Body)

	//fmt.Println(content)
	fmt.Println(time.Now().Sub(t1))

	return content

}

func DoSqlProcess(data string) []byte {
	t1 := time.Now()
	req, err := http.NewRequest("POST", CHDBURL+"?add_http_cors_header=1&default_format=Values&max_result_rows=1000&max_result_bytes=10000000&result_overflow_mode=break", bytes.NewBuffer([]byte(data)))
	if err != nil {
		fmt.Println("Error creating request:", err)
		return nil
	}
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9")
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("Content-Type", "text/plain;charset=UTF-8")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Error making request:", err)
		return nil
	}
	defer resp.Body.Close()
	content, _ := io.ReadAll(resp.Body)
	fmt.Println(time.Now().Sub(t1))

	return content

}

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

	if strings.HasPrefix(engine, "MergeTree") && orderBy != "" {
		createTableSQL += fmt.Sprintf("ORDER BY %s", orderBy)
	}

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
					log.Printf("Warning: Conflicting types for field '%s'. Using type from first schema: %s, ignoring type from second schema: %s",
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
		fmt.Errorf("failed to open Parquet file: %w", err)
		return nil, err
	}
	defer f.Close()

	// Create a new ParquetReader
	reader, err := file.NewParquetReader(f)
	if err != nil {
		fmt.Errorf("Error creating Parquet reader: %w", err)
		return nil, err
	}
	defer reader.Close()
	mem := memory.NewGoAllocator()

	// Create a ParquetFileReader to Arrow converter
	arrowReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{}, mem)
	if err != nil {
		fmt.Printf("Error creating Arrow reader: %v\n", err)
		return nil, err
	}
	// Get the Arrow schema
	arrowSchema, err := arrowReader.Schema()
	return arrowSchema, nil
}

func GetParquetSchemaCHDBDict(parquetPath string) (ResponseData, error) {
	sql1 := fmt.Sprintf("set input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference=1;SELECT columns FROM file('%s', ParquetMetadata)", parquetPath)
	content1 := GetMetaDataSql(sql1)

	var response ResponseData
	err := json.Unmarshal(content1, &response)
	if err != nil {
		fmt.Println("Error unmarshalling JSON:", err)
		return response, err
	}

	return response, nil
}

func FileExists(filename string) bool {
	_, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return true
}

func ContinuousQueries(parquet1, parquetOut string, interval string) error {
	sc1, err := GetParquetSchemaCHDBDict(parquet1)
	if err != nil {
		fmt.Printf("Error get file schema : %s\n,%v\n", parquet1, err)
		return err
	}

	columns := ""
	k := 0
	for _, dataItem := range sc1.Data {
		for _, column := range dataItem.Columns {
			if column.Name == "time" || column.Name == "/" {
				continue
			}
			if column.LogicalType == "Null" {
				continue
			}

			//if k > 0 {
			//	columns = columns + "," + "avg(coalesce(" + column.Name + ", 0)) AS " + column.Name
			//} else {
			//	columns += "avg(coalesce(" + column.Name + ", 0)) AS " + column.Name
			//}
			if k > 0 {
				if column.LogicalType == "String" {
					//topK(1)(turbine)[1] AS most_frequent_turbine
					//argMin(turbine,time) AS most_frequent_A
					columns = columns + "," + "argMin(" + column.Name + ",time) AS " + column.Name
				} else {
					columns = columns + "," + "avg(" + column.Name + ") AS " + column.Name
				}

			} else {
				if column.LogicalType == "String" {
					columns += "argMin(" + column.Name + ",time) AS " + column.Name
				} else {
					columns += "avg(" + column.Name + ") AS " + column.Name
				}

			}
			k = k + 1
		}

	}

	//avg(coalesce(CnvBMSEnergy, 0)) AS CnvBMSEnergy

	sql := fmt.Sprintf("SELECT toStartOfInterval(time, INTERVAL %s) AS time, %s FROM file('%s', Parquet) GROUP BY time ORDER BY time  INTO OUTFILE '%s' TRUNCATE FORMAT Parquet;", interval, columns, parquet1, parquetOut)

	result := DoSqlProcess(sql)
	//check if the generate file influence the original select
	if len(result) == 0 {
		return nil
	} else {
		return errors.New(string(result))
	}
}

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
