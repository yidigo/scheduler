package tasks

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/hibiken/asynq"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type DownloadReportPayload struct {
	Groups        []map[string][]string `v:"required" dc:"device name"`
	ReportType    string                `v:"required" dc:"report name" summary:"SummaryReport,TurbineAvailabilityMetrics,EfficiencyMetrics"`
	Start         time.Time             `v:"required" dc:"trend start time"`
	End           time.Time             `v:"required" dc:"trend end time"`
	Columns       []string              `v:"required" dc:"columns name"`
	TranColumns   map[string]string     `v:"required" json:"tran_columns" dc:"standard name to nick name"`
	Operation     ReportOperation       `v:"required" dc:"data process method"`
	FilePath      string                `dc:"output file path"`
	TaskStartTime time.Time             `dc:"task start time"`
	Language      string                `dc:"language"`
}

type ReportOperation struct {
	Resample string `json:"resample" dc:"column name"`
	Merge    bool   `json:"merge" dc:"column name"`
}

type ReportData struct {
	Time                     string `json:"Time"`
	DeviceName               string `json:"DeviceName"`
	FaultShutdownDuration    uint64 `json:"FaultShutdownDuration"`
	OverhaulDuration         uint64 `json:"OverhaulDuration"`
	MaintenanceDuration      uint64 `json:"MaintenanceDuration"`
	GridShutdownDuration     uint64 `json:"GridShutdownDuration"`
	LocalShutdownDuration    uint64 `json:"LocalShutdownDuration"`
	RemoteShutdownDuration   uint64 `json:"RemoteShutdownDuration"`
	WeatherShutdownDuration  uint64 `json:"WeatherShutdownDuration"`
	LimitPowerDuration       uint64 `json:"LimitPowerDuration"`
	LimitShutdownDuration    uint64 `json:"LimitShutdownDuration"`
	StandbyDuration          uint64 `json:"StandbyDuration"`
	NormalGenerationDuration uint64 `json:"NormalGenerationDuration"`
	InterruptionDuration     int64  `json:"InterruptionDuration"`

	FaultShutdownTimes    uint64 `json:"FaultShutdownTimes"`
	OverhaulTimes         uint64 `json:"OverhaulTimes"`
	MaintenanceTimes      uint64 `json:"MaintenanceTimes"`
	GridShutdownTimes     uint64 `json:"GridShutdownTimes"`
	LocalShutdownTimes    uint64 `json:"LocalShutdownTimes"`
	RemoteShutdownTimes   uint64 `json:"RemoteShutdownTimes"`
	WeatherShutdownTimes  uint64 `json:"WeatherShutdownTimes"`
	LimitPowerTimes       uint64 `json:"LimitPowerTimes"`
	LimitShutdownTimes    uint64 `json:"LimitShutdownTimes"`
	StandbyTimes          uint64 `json:"StandbyTimes"`
	NormalGenerationTimes uint64 `json:"NormalGenerationTimes"`
	InterruptionTimes     uint64 `json:"InterruptionTimes"`

	TheoreticalGeneration float64 `json:"TheoreticalGeneration"`

	GriActivePowerTotalAvg float64 `json:"GriActivePowerTotal_avg"`
	GriActivePowerTotalMax float64 `json:"GriActivePowerTotal_max"`
	GriActivePowerTotalMin float64 `json:"GriActivePowerTotal_min"`

	GriReactivePowerTotalAvg float64 `json:"GriReactivePowerTotal_avg"`
	GriReactivePowerTotalMax float64 `json:"GriReactivePowerTotal_max"`
	GriReactivePowerTotalMin float64 `json:"GriReactivePowerTotal_min"`

	CnvGenPower30sAvg float64 `json:"CnvGenPower30s_avg"`
	CnvGenPower30sMax float64 `json:"CnvGenPower30s_max"`
	CnvGenPower30sMin float64 `json:"CnvGenPower30s_min"`

	CnvGenPower600sAvg float64 `json:"CnvGenPower600s_avg"`
	CnvGenPower600sMax float64 `json:"CnvGenPower600s_max"`
	CnvGenPower600sMin float64 `json:"CnvGenPower600s_min"`

	AllCount     uint64  `json:"AllCount"`
	MTBFDuration float64 `json:"MTBFDuration"`
	MTTRDuration float64 `json:"MTTRDuration"`
	RatedPower   float64 `json:"RatedPower"`

	PreTotalGenerationFirst float64 `json:"preTotalGeneration_first"`

	GriActiveEnergyDelLast  float64 `json:"GriActiveEnergyDel_last"`
	GriActiveEnergyDelTotal float64 `json:"GriActiveEnergyDel_total"`

	PreTotalEleConsumptionFirst float64 `json:"preTotalEleConsumption_first"`

	GriActiveEnergyRcvLast    float64 `json:"GriActiveEnergyRcv_last"`
	GriActiveEnergyRcvSection float64 `json:"GriActiveEnergyRcv_section"`

	Availability float64 `json:"Availability"`
}

func formatList(list []string) string {
	if len(list) == 0 {
		return "()" // Handle empty list case
	}

	result := "("
	for i, item := range list {
		result += fmt.Sprintf("'%s'", item)
		if i < len(list)-1 {
			result += ","
		}
	}
	result += ")"
	return result
}

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

func DoSqlProcessJson(data string) []byte {
	t1 := time.Now()
	url := CHDBURL + "?add_http_cors_header=1&default_format=JSON&max_result_rows=1000&max_result_bytes=10000000&result_overflow_mode=break&"

	req, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(data)))
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

func HandleDownloadReportTask(ctx context.Context, t *asynq.Task) error {
	//接收任务数据.
	fileList := make([]string, 0)
	fileDirectory := filepath.Join("/tmp/", time.Now().Format("20060102150405"), "/")
	if _, err := os.Stat(fileDirectory); os.IsNotExist(err) {
		err := os.MkdirAll(fileDirectory, os.ModeDir|0755) // Creates parent directories if needed
		if err != nil {
			fmt.Println("Error creating directory:", err)
			return err
		}
	}
	var p DownloadReportPayload
	//fmt.Println(t.Payload())
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("json.Unmarshal failed: %v: %w", err, asynq.SkipRetry)
	}

	timePoints := make([]string, 0)
	switch p.Operation.Resample {
	case "10min":
		currentTime := time.Date(p.Start.Year(), p.Start.Month(), p.Start.Day(), p.Start.Hour(), p.Start.Minute(), 0, 0, p.Start.Location()) // Start at the beginning of t1's month
		for currentTime.Before(p.End) {
			timePoints = append(timePoints, currentTime.Format("2006-01-02 15:04:05"))
			currentTime = currentTime.Add(10 * time.Minute)
		}
		timePoints = append(timePoints, currentTime.Format("2006-01-02 15:04:05"))
	case "hour":
		currentTime := time.Date(p.Start.Year(), p.Start.Month(), p.Start.Day(), p.Start.Hour(), 0, 0, 0, p.Start.Location()) // Start at the beginning of t1's month
		for currentTime.Before(p.End) {
			timePoints = append(timePoints, currentTime.Format("2006-01-02 15:04:05"))
			currentTime = currentTime.Add(time.Hour)
		}
		timePoints = append(timePoints, currentTime.Format("2006-01-02 15:04:05"))

	case "day":
		currentTime := time.Date(p.Start.Year(), p.Start.Month(), p.Start.Day(), 0, 0, 0, 0, p.Start.Location()) // Start at the beginning of t1's month
		for currentTime.Before(p.End) {
			timePoints = append(timePoints, currentTime.Format("2006-01-02 15:04:05"))
			currentTime = currentTime.Add(24 * time.Hour)
		}
		timePoints = append(timePoints, currentTime.Format("2006-01-02 15:04:05"))
	case "month":
		currentTime := time.Date(p.Start.Year(), p.Start.Month(), 1, 0, 0, 0, 0, p.Start.Location()) // Start at the beginning of t1's month
		for currentTime.Before(p.End) {
			timePoints = append(timePoints, currentTime.Format("2006-01-02 15:04:05"))
			currentTime = currentTime.AddDate(0, 1, 0)
		}
		timePoints = append(timePoints, currentTime.Format("2006-01-02 15:04:05"))
	case "year":
		currentTime := time.Date(p.Start.Year(), p.Start.Month(), 1, 0, 0, 0, 0, p.Start.Location()) // Start at the beginning of t1's month
		for currentTime.Before(p.End) {
			timePoints = append(timePoints, currentTime.Format("2006-01-02 15:04:05"))
			currentTime = currentTime.AddDate(1, 1, 0)
		}
		timePoints = append(timePoints, currentTime.Format("2006-01-02 15:04:05"))
	}
	mergeResult := make([]map[string]interface{}, 0)
	//iterate the single group like {"farm1":["001","002","003"]}
	fields := strings.Join(p.Columns, ",")
	for _, item := range p.Groups {
		filename := ""
		//iterate the time
		for i := 0; i < len(timePoints)-1; i++ {
			for key, values := range item {
				//deviceDataList := make([]tools.ReportData, 0)

				sql := fmt.Sprintf("select %s from db_report.%s where DeviceName in %s and Time>='%s' and Time<'%s';", fields, p.ReportType, formatList(values), timePoints[i], timePoints[i+1])
				jsonData := DoSqlProcessJson(sql)
				var response CHDBJsonStruct
				err := json.Unmarshal(jsonData, &response)
				if err != nil {
					log.Fatalf("Error unmarshaling JSON: %v", err)
				}
				//deviceDataList = append(deviceDataList, response.Data...)

				group_result := MergeData(response.Data)
				date, _ := time.Parse("2006-01-02 15:04:05", timePoints[i])

				group_result["Time"] = date
				group_result["DeviceName"] = key

				filename = key
				mergeResult = append(mergeResult, group_result)
			}
		}
		if p.Operation.Merge == false {
			file, err := os.Create(fileDirectory + "/" + filename + ".csv")
			if err != nil {
				fmt.Println("Error creating CSV file:", err)
				continue
			}

			err = ReportDataToCSV(mergeResult, p.TranColumns, file)
			if err != nil {
				fmt.Println("Error writing to CSV:", err)
				continue
			}
			fileList = append(fileList, fileDirectory+"/"+filename+".csv")
			mergeResult = make([]map[string]interface{}, 0)
			file.Close()
		}
	}
	if p.Operation.Merge == true {
		file, err := os.Create(fileDirectory + "/" + "all.csv")
		if err != nil {
			fmt.Println("Error creating CSV file:", err)
		}

		err = ReportDataToCSV(mergeResult, p.TranColumns, file)
		if err != nil {
			fmt.Println("Error writing to CSV:", err)
		}
		mergeResult = make([]map[string]interface{}, 0)
		fileList = append(fileList, fileDirectory+"/"+"all.csv")
		file.Close()

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

func MergeData(dataList []map[string]interface{}) map[string]interface{} {
	if len(dataList) == 0 {
		return make(map[string]interface{}) // Return empty map for empty input
	}

	result := make(map[string]interface{})
	keyValues := make(map[string][]interface{}) // Stores all values for a given key
	allUniqueKeys := make(map[string]struct{})  // To get a unique set of all keys across maps

	// 1. Collect all values for each key from all maps
	for _, dataMap := range dataList {
		for key, value := range dataMap {
			keyValues[key] = append(keyValues[key], value)
			allUniqueKeys[key] = struct{}{}
		}
	}

	// Sort keys for deterministic output order (optional, but good for testing)
	sortedKeys := make([]string, 0, len(allUniqueKeys))
	for k := range allUniqueKeys {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	// 2. Process each key
	for _, key := range sortedKeys {
		values := keyValues[key]
		if len(values) == 0 { // Should ideally not happen if allUniqueKeys was populated correctly
			continue
		}

		lowerKey := strings.ToLower(key)

		// Rule 2: Special handling for "DeviceName"
		if key == "DeviceName" {
			var names []string
			for _, v := range values {
				if s, ok := v.(string); ok {
					// Avoid adding duplicate names if they are consecutive or if you want unique names
					// For this example, we just append all found names.
					names = append(names, s)
				}
			}
			result[key] = strings.Join(names, ", ")
			continue
		}

		// Rule 2: Special handling for "Time" - take the latest time
		if key == "Time" {
			var latestTime time.Time
			var timeFound bool
			for _, v := range values {
				if t, ok := v.(time.Time); ok {
					if !timeFound || t.After(latestTime) {
						latestTime = t
						timeFound = true
					}
				}
			}
			if timeFound {
				result[key] = latestTime
			} else if len(values) > 0 { // Fallback if no time.Time found but values exist
				result[key] = values[0]
			}
			continue
		}

		// Attempt to get numeric values for aggregation
		numericValues := getNumericValues(values)

		if len(numericValues) == 0 {
			// Rule 4: If no numeric values could be extracted for this key (and it's not Time/DeviceName),
			// take the first available raw value as a fallback.
			if len(values) > 0 {
				result[key] = values[0]
			}
			continue
		}

		// At this point, numericValues has at least one element.
		// Rule 3: Apply aggregation rules based on key name
		processed := false
		if strings.Contains(lowerKey, "max") {
			maxVal := numericValues[0]
			for i := 1; i < len(numericValues); i++ {
				if numericValues[i] > maxVal {
					maxVal = numericValues[i]
				}
			}
			result[key] = maxVal
			processed = true
		} else if strings.Contains(lowerKey, "min") {
			minVal := numericValues[0]
			for i := 1; i < len(numericValues); i++ {
				if numericValues[i] < minVal {
					minVal = numericValues[i]
				}
			}
			result[key] = minVal
			processed = true
		} else if strings.Contains(lowerKey, "avg") { // Explicit "avg" in key
			sum := 0.0
			for _, num := range numericValues {
				sum += num
			}
			result[key] = sum / float64(len(numericValues))
			processed = true
		} else if strings.Contains(lowerKey, "sum") || strings.Contains(lowerKey, "total") { // "sum" or "total" in key
			sum := 0.0
			for _, num := range numericValues {
				sum += num
			}
			result[key] = sum
			processed = true
		}

		if !processed {
			// Rule 3 (Default): Default aggregation for numeric fields is average
			sum := 0.0
			for _, num := range numericValues {
				sum += num
			}
			result[key] = sum / float64(len(numericValues))
		}
	}

	return result
}

func getNumericValues(values []interface{}) []float64 {
	var nums []float64
	for _, v := range values {
		switch val := v.(type) {
		case int:
			nums = append(nums, float64(val))
		case int8:
			nums = append(nums, float64(val))
		case int16:
			nums = append(nums, float64(val))
		case int32:
			nums = append(nums, float64(val))
		case int64:
			nums = append(nums, float64(val))
		case uint:
			nums = append(nums, float64(val))
		case uint8:
			nums = append(nums, float64(val))
		case uint16:
			nums = append(nums, float64(val))
		case uint32:
			nums = append(nums, float64(val))
		case uint64:
			nums = append(nums, float64(val))
		case float32:
			nums = append(nums, float64(val))
		case float64:
			nums = append(nums, val)
			// Add other numeric types if necessary, or log/error for unexpected types
		}
	}
	return nums
}

func ReportDataToCSV(data []map[string]interface{}, columns map[string]string, writer io.Writer) error {
	csvWriter := csv.NewWriter(writer)

	// Define the specific data keys for prioritized columns
	const timeDataKey = "Time"
	const devicesDataKey = "DeviceName"

	var orderedDataKeys []string
	var csvHeaders []string
	processedKeys := make(map[string]bool) // To keep track of keys already added

	// 1. Handle "time" column (first priority)
	if headerName, ok := columns[timeDataKey]; ok {
		orderedDataKeys = append(orderedDataKeys, timeDataKey)
		csvHeaders = append(csvHeaders, headerName)
		processedKeys[timeDataKey] = true
	}

	// 2. Handle "devices" column (second priority)
	// Ensure it's not the same as timeDataKey and hasn't been processed
	if headerName, ok := columns[devicesDataKey]; ok {
		if !processedKeys[devicesDataKey] {
			orderedDataKeys = append(orderedDataKeys, devicesDataKey)
			csvHeaders = append(csvHeaders, headerName)
			processedKeys[devicesDataKey] = true
		}
	}

	// 3. Handle remaining columns
	// Collect and sort them for consistent order
	var remainingDataKeys []string
	for dataKey := range columns {
		if !processedKeys[dataKey] {
			remainingDataKeys = append(remainingDataKeys, dataKey)
		}
	}
	sort.Strings(remainingDataKeys) // Sort by data key for predictable order

	for _, dataKey := range remainingDataKeys {
		orderedDataKeys = append(orderedDataKeys, dataKey)
		csvHeaders = append(csvHeaders, columns[dataKey])
	}

	// Write header row if there are any headers
	if len(csvHeaders) > 0 {
		if err := csvWriter.Write(csvHeaders); err != nil {
			return fmt.Errorf("error writing CSV header: %w", err)
		}
	}

	// Write data rows
	for _, rowMap := range data {
		record := make([]string, len(orderedDataKeys))
		for i, dataKey := range orderedDataKeys {
			value, found := rowMap[dataKey]
			if !found {
				record[i] = "" // Use empty string for missing values
			} else {
				// Special handling for time.Time for better formatting
				if t, ok := value.(time.Time); ok {
					record[i] = t.Format(time.RFC3339) // Example: "2006-01-02T15:04:05Z07:00"
				} else {
					record[i] = fmt.Sprintf("%v", value)
				}
			}
		}
		if err := csvWriter.Write(record); err != nil {
			// It's often better to log this error and continue, or collect errors
			// For simplicity here, we return on the first error.
			return fmt.Errorf("error writing CSV record: %w", err)
		}
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return fmt.Errorf("error flushing CSV writer: %w", err)
	}

	return nil
}
