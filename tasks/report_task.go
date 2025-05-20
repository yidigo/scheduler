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
	"reflect"
	"strconv"
	"strings"
	"time"
)

type DownloadReportPayload struct {
	Groups        []map[string][]string `v:"required" dc:"device name"`
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
	Meta                   []Meta       `json:"meta"`
	Data                   []ReportData `json:"data"`
	Rows                   int          `json:"rows"`
	RowsBeforeLimitAtLeast int          `json:"rows_before_limit_at_least"`
	Statistics             Statistics   `json:"statistics"`
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
	mergeResult := make([]ReportData, 0)
	//iterate the single group like {"farm1":["001","002","003"]}
	fields := strings.Join(p.Columns, ",")
	for _, item := range p.Groups {
		filename := ""
		//iterate the time
		for i := 0; i < len(timePoints)-1; i++ {
			for key, values := range item {
				//deviceDataList := make([]tools.ReportData, 0)

				sql := fmt.Sprintf("select %s from db_report.MinutesReportData where DeviceName in %s and Time>='%s' and Time<'%s';", fields, formatList(values), timePoints[i], timePoints[i+1])
				jsonData := DoSqlProcessJson(sql)
				var response CHDBJsonStruct
				err := json.Unmarshal(jsonData, &response)
				if err != nil {
					log.Fatalf("Error unmarshaling JSON: %v", err)
				}
				//deviceDataList = append(deviceDataList, response.Data...)

				group_result := MergeData(response.Data)
				group_result.Time = timePoints[i]
				group_result.DeviceName = key
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
			mergeResult = make([]ReportData, 0)
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
		mergeResult = make([]ReportData, 0)
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

func MergeData(dataList []ReportData) ReportData {
	result := ReportData{}
	result.GriActivePowerTotalMin = 999999999999999999999999999
	result.GriReactivePowerTotalMin = 9999999999999999999999999
	result.CnvGenPower30sMin = 999999999999999999999999999
	result.CnvGenPower600sMin = 999999999999999999999999999
	result.PreTotalGenerationFirst = 999999999999999999999999999
	result.PreTotalEleConsumptionFirst = 999999999999999999999999999
	for _, data := range dataList {

		// Uint64 and Uint32
		result.AllCount += data.AllCount

		// Duration fields
		result.FaultShutdownDuration += data.FaultShutdownDuration

		result.OverhaulDuration += data.OverhaulDuration

		result.MaintenanceDuration += data.MaintenanceDuration

		result.GridShutdownDuration += data.GridShutdownDuration

		result.LocalShutdownDuration += data.LocalShutdownDuration

		result.RemoteShutdownDuration += data.RemoteShutdownDuration

		result.WeatherShutdownDuration += data.WeatherShutdownDuration

		result.LimitPowerDuration += data.LimitPowerDuration

		result.LimitShutdownDuration += data.LimitShutdownDuration

		result.StandbyDuration += data.StandbyDuration

		result.NormalGenerationDuration += data.NormalGenerationDuration

		result.InterruptionDuration += data.InterruptionDuration

		// Times fields

		result.FaultShutdownTimes += data.FaultShutdownTimes

		result.OverhaulTimes += data.OverhaulTimes

		result.MaintenanceTimes += data.MaintenanceTimes

		result.GridShutdownTimes += data.GridShutdownTimes

		result.LocalShutdownTimes += data.LocalShutdownTimes

		result.RemoteShutdownTimes += data.RemoteShutdownTimes

		result.WeatherShutdownTimes += data.WeatherShutdownTimes

		result.LimitPowerTimes += data.LimitPowerTimes

		result.LimitShutdownTimes += data.LimitShutdownTimes

		result.StandbyTimes += data.StandbyTimes

		result.NormalGenerationTimes += data.NormalGenerationTimes

		result.InterruptionTimes += data.InterruptionTimes

		// Float64 fields

		result.TheoreticalGeneration += data.TheoreticalGeneration

		result.GriActivePowerTotalAvg = result.GriActivePowerTotalAvg + data.GriActivePowerTotalAvg*float64(data.AllCount)

		if data.GriActivePowerTotalMax > result.GriActivePowerTotalMax {
			result.GriActivePowerTotalMax = data.GriActivePowerTotalMax
		}

		if data.GriActivePowerTotalMin < result.GriActivePowerTotalMin {
			result.GriActivePowerTotalMin = data.GriActivePowerTotalMin
		}

		result.GriReactivePowerTotalAvg = result.GriReactivePowerTotalAvg + data.GriReactivePowerTotalAvg*float64(data.AllCount)

		if data.GriReactivePowerTotalMax > result.GriReactivePowerTotalMax {
			result.GriReactivePowerTotalMax = data.GriReactivePowerTotalMax
		}

		if data.GriReactivePowerTotalMin < result.GriReactivePowerTotalMin {
			result.GriReactivePowerTotalMin = data.GriReactivePowerTotalMin
		}

		result.CnvGenPower30sAvg = result.CnvGenPower30sAvg + data.CnvGenPower30sAvg*float64(data.AllCount)

		if data.CnvGenPower30sMax > result.CnvGenPower30sMax {
			result.CnvGenPower30sMax = data.CnvGenPower30sMax
		}

		if data.CnvGenPower30sMin < result.CnvGenPower30sMin {
			result.CnvGenPower30sMin = data.CnvGenPower30sMin
		}

		result.CnvGenPower600sAvg = result.CnvGenPower600sAvg + data.CnvGenPower600sAvg*float64(data.AllCount)

		if data.CnvGenPower600sMax > result.CnvGenPower600sMax {
			result.CnvGenPower600sMax = data.CnvGenPower600sMax
		}

		if data.CnvGenPower600sMin < result.CnvGenPower600sMin {
			result.CnvGenPower600sMin = data.CnvGenPower600sMin
		}

		result.MTBFDuration = result.MTBFDuration + data.MTBFDuration*float64(data.AllCount)

		result.MTTRDuration = result.MTTRDuration + data.MTTRDuration*float64(data.AllCount)

		result.Availability = result.Availability + data.Availability*float64(data.AllCount)

		if data.PreTotalGenerationFirst < result.PreTotalGenerationFirst {
			result.PreTotalGenerationFirst = data.PreTotalGenerationFirst
		}

		if data.GriActiveEnergyDelLast > result.GriActiveEnergyDelLast {
			result.GriActiveEnergyDelLast = data.GriActiveEnergyDelLast
		}

		result.GriActiveEnergyDelTotal = result.GriActiveEnergyDelTotal + data.GriActiveEnergyDelTotal*float64(data.AllCount)

		if data.PreTotalEleConsumptionFirst < result.PreTotalEleConsumptionFirst {
			result.PreTotalEleConsumptionFirst = data.PreTotalEleConsumptionFirst
		}

		if data.GriActiveEnergyRcvLast > result.GriActiveEnergyRcvLast {
			result.GriActiveEnergyRcvLast = data.GriActiveEnergyRcvLast
		}

		result.GriActiveEnergyRcvSection = result.GriActiveEnergyRcvSection + data.GriActiveEnergyRcvSection*float64(data.AllCount)

		result.RatedPower = result.RatedPower + data.RatedPower*float64(data.AllCount)

	}

	result.GriActivePowerTotalAvg = result.GriActivePowerTotalAvg / float64(result.AllCount)

	result.GriReactivePowerTotalAvg = result.GriReactivePowerTotalAvg / float64(result.AllCount)

	result.CnvGenPower30sAvg = result.CnvGenPower30sAvg / float64(result.AllCount)

	result.CnvGenPower600sAvg = result.CnvGenPower600sAvg / float64(result.AllCount)

	result.MTBFDuration = result.MTBFDuration / float64(result.AllCount)

	result.MTTRDuration = result.MTTRDuration / float64(result.AllCount)

	result.Availability = result.Availability / float64(result.AllCount)

	result.GriActiveEnergyDelTotal = result.GriActiveEnergyDelTotal / float64(result.AllCount)

	result.GriActiveEnergyRcvSection = result.GriActiveEnergyRcvSection / float64(result.AllCount)

	result.RatedPower = result.RatedPower / float64(result.AllCount)

	return result
}

func ReportDataToCSV(data []ReportData, columns map[string]string, writer io.Writer) error {
	csvWriter := csv.NewWriter(writer)
	oriColumn := make([]string, 0)
	headerRow := make([]string, 0)
	for key, value := range columns {
		oriColumn = append(oriColumn, key)
		headerRow = append(headerRow, value)

	}

	if err := csvWriter.Write(headerRow); err != nil {
		return fmt.Errorf("error writing header row to csv: %w", err)
	}

	// Reflect the ReportData struct type to access fields by name
	reportDataType := reflect.TypeOf(ReportData{})

	// Iterate through each ReportData record
	for _, record := range data {
		recordValue := reflect.ValueOf(record)
		row := make([]string, 0, len(oriColumn))

		// Iterate through the desired columns
		for _, col := range oriColumn {
			// Find the field in the struct based on the JSON tag
			fieldValue := reflect.Value{}
			for i := 0; i < reportDataType.NumField(); i++ {
				field := reportDataType.Field(i)
				jsonTag := field.Tag.Get("json")
				if jsonTag == col {
					fieldValue = recordValue.Field(i)
					break
				}
			}

			// If the field is not found (JSON tag doesn't match the column), add an empty string
			if !fieldValue.IsValid() {
				row = append(row, "")
				continue
			}

			// Convert the field value to a string based on its type
			var stringValue string
			switch fieldValue.Kind() {
			case reflect.String:
				stringValue = fieldValue.String()
			case reflect.Uint64:
				stringValue = strconv.FormatUint(fieldValue.Uint(), 10)
			case reflect.Int64:
				stringValue = strconv.FormatInt(fieldValue.Int(), 10)
			case reflect.Float64:
				stringValue = strconv.FormatFloat(fieldValue.Float(), 'f', -1, 64) // Use 'f' format for floating-point numbers
			default:
				stringValue = fmt.Sprintf("%v", fieldValue.Interface()) // Fallback to general formatting
			}
			row = append(row, stringValue)
		}

		// Write the row to the CSV file
		if err := csvWriter.Write(row); err != nil {
			return fmt.Errorf("error writing row to csv: %w", err)
		}
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return fmt.Errorf("error flushing csv writer: %w", err)
	}

	return nil
}
