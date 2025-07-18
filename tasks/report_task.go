package tasks

import (
	"archive/zip"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/Knetic/govaluate"
	"github.com/hibiken/asynq"
	"gopkg.in/natefinch/lumberjack.v2"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"scheduler/logger"
	event_center2 "scheduler/tasks/event_center"
	"scheduler/tasks/utils"
	"sort"
	"strings"
	"time"
)

var ReportLogger *logger.Logger

func init() {
	lumberjackLogger := &lumberjack.Logger{
		Filename:   "./app_rotated.log",
		MaxSize:    10, // megabytes
		MaxBackups: 3,
		MaxAge:     28,   //days
		Compress:   true, // disabled by default
	}
	defer lumberjackLogger.Close()

	ReportLogger = logger.New(
		logger.WithOutput(lumberjackLogger),
		logger.WithLevel(logger.LevelInfo),
	)

	ReportLogger.Info("scheduler service restart")

}

type DownloadReportPayload struct {
	Groups        []map[string][]string `v:"required" dc:"device name"`
	ReportType    string                `v:"required" dc:"report name" summary:"SummaryReport,TurbineAvailabilityMetrics,EfficiencyMetrics"`
	Start         time.Time             `v:"required" dc:"trend start time"`
	End           time.Time             `v:"required" dc:"trend end time"`
	Columns       []string              `v:"required" dc:"columns name"`
	TranColumns   map[string]string     `v:"required" json:"tran_columns" dc:"standard name to nick name"`
	Operation     ReportOperation       `v:"required" dc:"data process method"`
	Expression    map[string]string     `dc:"expression for some variable"`
	FilePath      string                `dc:"output file path"`
	TaskStartTime time.Time             `dc:"task start time"`
	Language      string                `dc:"language"`
}

type ReportOperation struct {
	Resample string `json:"resample" dc:"column name"`
	Merge    bool   `json:"merge" dc:"column name"`
}

type CalculateReportPayload struct {
	ReportKey string    `v:"required" dc:"report name" summary:"SummaryReport,TurbineAvailabilityMetrics,EfficiencyMetrics"`
	FilePath  string    `dc:"output file path"`
	DataStr   time.Time `dc:"task start time"`
}

type DownloadEventPayload struct {
	System        string                           `json:"system" dc:"system name"`
	DeviceType    []string                         `json:"deviceType" dc:"device type"`
	Devices       []*event_center2.FarmDeviceEntry `json:"devices" dc:"farmCode and deviceNames"`
	EventLevels   []int32                          `json:"eventLevels" dc:"event level"`
	FirstEvent    int32                            `json:"firstEvent" dc:"is first event"`
	Confirmed     int32                            `json:"confirmed" dc:"is confirmed"`
	From          time.Time                        `json:"from" dc:"query start time"`
	To            time.Time                        `json:"to" dc:"query end time"`
	Page          int64                            `json:"page" dc:"page number"`
	PageSize      int64                            `json:"pageSize" dc:"page size"`
	Order         string                           `json:"order" dc:"order"`
	FilePath      string                           `json:"filePath" dc:"output file path"`
	TaskStartTime time.Time                        `json:"taskStartTime" dc:"task start time"`
	EventType     string                           `json:"eventType" dc:"event type"`
	Username      string                           `json:"username" dc:"user name"`
	Category      string                           `json:"category" dc:"category"`
	Columns       []map[string]string              `json:"columns" dc:"columns"`
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

//func DoSqlProcessJson(data string) []byte {
//	t1 := time.Now()
//	url := taskConfig.ClickHouseURL + "?add_http_cors_header=1&default_format=JSON&max_result_rows=1000&max_result_bytes=10000000&result_overflow_mode=break&"
//
//	req, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(data)))
//	if err != nil {
//		ReportLogger.Errorf("Error creating request:", err)
//		return nil
//	}
//	req.Header.Set("Accept", "*/*")
//	req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9")
//	req.Header.Set("Connection", "keep-alive")
//	req.Header.Set("Content-Type", "text/plain;charset=UTF-8")
//	client := &http.Client{}
//	resp, err := client.Do(req)
//	if err != nil {
//		ReportLogger.Errorf("Error making request:", err)
//		return nil
//	}
//	defer resp.Body.Close()
//	content, _ := io.ReadAll(resp.Body)
//	ReportLogger.Infof("time cost:%f", time.Now().Sub(t1))
//
//	return content
//
//}

func HandleDownloadReportTask(ctx context.Context, t *asynq.Task) error {

	lumberjackLogger, taskLogger := logger.GenerateNewLogger("./task_log/download_report.log", taskConfig.LogMaxSizeMB, taskConfig.LogMaxBackups, taskConfig.LogMaxAgeDays, taskConfig.LogCompress, taskConfig.LogLevel)
	defer lumberjackLogger.Close()

	//接收任务数据.
	fileList := make([]string, 0)
	fileDirectory := filepath.Join("/tmp/", "scheduler_reports", time.Now().Format("20060102150405")+"_"+generateRandomString(8))
	defer os.RemoveAll(fileDirectory)
	if _, err := os.Stat(fileDirectory); os.IsNotExist(err) {
		err := os.MkdirAll(fileDirectory, os.ModeDir|0755) // Creates parent directories if needed
		if err != nil {
			ReportLogger.Errorf("Error creating directory:", err)
			return err
		}
	}
	var p DownloadReportPayload
	//fmt.Println(t.Payload())
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		taskLogger.Error("json.Unmarshal failed for DownloadReportPayload:", err)
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
	case "time":
		timePoints = append(timePoints, p.Start.Format("2006-01-02 15:04:05"))
		currentTime := time.Date(p.Start.Year(), p.Start.Month(), p.Start.Day(), 0, 0, 0, 0, p.Start.Location()) // Start at the beginning of t1's month
		currentTime = currentTime.Add(24 * time.Hour)
		for currentTime.Before(p.End) {
			timePoints = append(timePoints, currentTime.Format("2006-01-02 15:04:05"))
			currentTime = currentTime.Add(24 * time.Hour)
		}
		timePoints = append(timePoints, p.End.Format("2006-01-02 15:04:05"))

	}
	if p.Operation.Resample == "time" {
		mergeResult := make([]map[string]interface{}, 0)
		for _, item := range p.Groups {

			sql := ""
			//iterate the time
			for key, values := range item {
				thisResult := make([]map[string]interface{}, 0)

				for i := 0; i < len(timePoints)-1; i++ {
					for _, vvv := range values {
						parquetfile := taskConfig.SecondParquetPath + strings.ReplaceAll(timePoints[i][0:10], "-", "") + "/" + vvv + "*"
						switch p.ReportType {
						case "SummaryReport":
							sql = fmt.Sprintf("SELECT\n    COUNT(MC082) AS Count, \n    SUM(IsEffectWind) AS EffectWindHours,\n    AVG(MC004) AS Met1sWSpdAvg,\n    MAX(MC004) AS Met1sWSpdMax,\n    MIN(MC004) AS Met1sWSpdMin,\n    AVG(MC082) AS Met30sWSpdAvg,\n    MAX(MC082) AS Met30sWSpdMax,\n    MIN(MC082) AS Met30sWSpdMin,\n    AVG(MC083) AS Met600sWSpdAvg,\n    MAX(MC083) AS Met600sWSpdMax,\n    MIN(MC083) AS Met600sWSpdMin,\n    AVG(MC093) AS MetAirDensityAvg,\n\n    (MAX(MC006) - MIN(MC006)) / ANY_VALUE(RatedPower) * 3600 AS EquivalentHours, \n    ANY_VALUE(RatedPower) AS RatedPowerData,\n    MIN(MC006) AS PreTotalGenerationFirst,\n    MAX(MC006) AS GriActiveEnergyDelLast,\n    (MAX(MC006) - MIN(MC006)) AS GriActiveEnergyDelTotal,\n    SUM(TheoreticalGeneration) AS TheoreticalGeneration,\n    MIN(MC062) AS PreTotalEleConsumptionFirst,\n    MAX(MC062) AS GriActiveEnergyRcvLast,\n    (MAX(MC062) - MIN(MC062)) AS GriActiveEnergyRcvSection,\n    AVG(MC061) AS GriActivePowerTotalAvg,\n    MAX(MC061) AS GriActivePowerTotalMax,\n    MIN(MC061) AS GriActivePowerTotalMin,\n    AVG(MD002) AS GriReactivePowerTotalAvg,\n    MAX(MD002) AS GriReactivePowerTotalMax,\n    MIN(MD002) AS GriReactivePowerTotalMin,\n    AVG(MC085) AS CnvGenPower30sAvg,\n    MAX(MC085) AS CnvGenPower30sMax,\n    MIN(MC085) AS CnvGenPower30sMin,\n    AVG(MC086) AS CnvGenPower600sAvg,\n    MAX(MC086) AS CnvGenPower600sMax,\n    MIN(MC086) AS CnvGenPower600sMin,\n    \n    COUNT(CASE WHEN MD001 >= RatedPower THEN 1 END) AS FullLoadDurationDay, \n    SUM(Generation) / 10000.0 AS GriActiveEnergyDelUnit,\n    SUM(Consumption) / 10000.0 AS GriActiveEnergyRcvUnit,\n\n\n    AVG(MC015) AS MetTmpAvg,\n    MAX(MC015) AS MetTmpMax,\n    MIN(MC015) AS MetTmpMin,\n    AVG(MC014) AS NacTmpAvg,\n    MAX(MC014) AS NacTmpMax,\n    MIN(MC014) AS NacTmpMin,\n    AVG(MC091) AS TowCbtTmpAvg,\n    MAX(MC091) AS TowCbtTmpMax,\n    MIN(MC091) AS TowCbtTmpMin,\n    SUM(CASE WHEN MA022 = 1 THEN 1 ELSE 0 END) AS YawAcwHours,\n    SUM(CASE WHEN YawCcwTimes = 1 THEN 1 ELSE 0 END) AS YawAcwTimes, \n    SUM(CASE WHEN MA021 = 1 THEN 1 ELSE 0 END) AS YawCwHours,\n    SUM(YawCwTimes) AS YawCwTimesSum, \n    (SUM(CASE WHEN MA022 = 1 THEN 1 ELSE 0 END) + SUM(CASE WHEN MA021 = 1 THEN 1 ELSE 0 END)) AS YawHoursTotal,\n    SUM(CASE WHEN MA2353 = 1 THEN 1 ELSE 0 END) AS GenReactQLimitStateDuration,\n    SUM(GenReactQLimitStateTimes) AS GenReactQLimitStateTimesSum\nFROM\n    file('%s', Parquet) \nWHERE\n time>='%s' and time<'%s';", parquetfile, timePoints[i], timePoints[i+1])
						case "TurbineAvailabilityMetrics":
							sql = fmt.Sprintf("WITH AggregatedData AS (\n    SELECT\n        SUM(CASE WHEN MC143 = 40 THEN 1 ELSE 0 END) AS FaultShutdownDuration,\n        SUM(CASE WHEN MC143 = 50 THEN 1 ELSE 0 END) AS OverhaulDuration,\n        SUM(CASE WHEN MC143 = 60 THEN 1 ELSE 0 END) AS MaintenanceDuration, \n        SUM(CASE WHEN MC143 = 70 THEN 1 ELSE 0 END) AS GridShutdownDuration,\n        SUM(CASE WHEN MC143 = 81 THEN 1 ELSE 0 END) AS LocalShutdownDuration,\n        SUM(CASE WHEN MC143 = 80 THEN 1 ELSE 0 END) AS RemoteShutdownDuration,\n        SUM(CASE WHEN MC143 = 90 THEN 1 ELSE 0 END) AS WeatherShutdownDuration,\n        SUM(CASE WHEN MC143 = 110 THEN 1 ELSE 0 END) AS LimitPowerDuration,\n        SUM(CASE WHEN MC143 = 111 THEN 1 ELSE 0 END) AS LimitShutdownDuration,\n        SUM(CASE WHEN MC143 = 100 THEN 1 ELSE 0 END) AS StandbyDuration,\n        SUM(CASE WHEN MC143 = 120 THEN 1 ELSE 0 END) AS NormalGenerationDuration,\n        SUM(FaultShutdownTimes) AS FaultShutdownTimes,\n        SUM(OverhaulTimes) AS OverhaulTimes,\n        SUM(MaintenanceTimes) AS MaintenanceTimes,\n        SUM(GridShutdownTimes) AS GridShutdownTimes,\n        SUM(LocalShutdownTimes) AS LocalShutdownTimes,\n        SUM(RemoteShutdownTimes) AS RemoteShutdownTimes,\n        SUM(WeatherShutdownTimes) AS WeatherShutdownTimes,\n        SUM(LimitPowerTimes) AS LimitPowerTimes,\n        SUM(LimitShutdownTimes) AS LimitShutdownTimes,\n        SUM(StandbyTimes) AS StandbyTimes,\n        SUM(NormalGenerationTimes) AS NormalGenerationTimes,\n        SUM(InterruptionTimes) AS InterruptionTimes,\n        COUNT(*) AS Count\n    FROM file('%s', Parquet)\nWHERE\n time>='%s' and time<'%s')\nSELECT\n    FaultShutdownDuration,\n    OverhaulDuration,\n    MaintenanceDuration,\n    GridShutdownDuration,\n    LocalShutdownDuration,\n    RemoteShutdownDuration,\n    WeatherShutdownDuration,\n    LimitPowerDuration,\n    LimitShutdownDuration,\n    StandbyDuration,\n    NormalGenerationDuration,\n    FaultShutdownTimes,\n    OverhaulTimes,\n    MaintenanceTimes,\n    GridShutdownTimes,\n    LocalShutdownTimes,\n    RemoteShutdownTimes,\n    WeatherShutdownTimes,\n    LimitPowerTimes,\n    LimitShutdownTimes,\n    StandbyTimes,\n    NormalGenerationTimes,\n    InterruptionTimes,\n    Count,\n    (MaintenanceDuration+GridShutdownDuration +LocalShutdownDuration +RemoteShutdownDuration +WeatherShutdownDuration +LimitPowerDuration +LimitShutdownDuration +StandbyDuration +NormalGenerationDuration) as Availabletime,\n    (FaultShutdownDuration + OverhaulDuration) as UnAvailabletime,\n    (1.0 - (FaultShutdownDuration + OverhaulDuration) * 1.0 / NULLIF(Count, 0)) AS Availability,\n    (600 - Count) AS InterruptionDuration_Calculated, \n    ((Count - FaultShutdownDuration) * 1.0 / NULLIF(FaultShutdownTimes, 0)) AS MTBFDuration,\n    (FaultShutdownDuration * 1.0 / NULLIF(FaultShutdownTimes, 0)) AS MTTRDuration\nFROM\n    AggregatedData;", parquetfile, timePoints[i], timePoints[i+1])
						case "EfficiencyMetrics":
							sql = fmt.Sprintf("WITH SourceData AS (\n    SELECT\n        MC006,\n        preTotalGeneration,\n        FaultLossGeneration,\n        OverhaulLossGeneration,\n        MaintainLossGeneration,\n        GridLossGeneration,\n        RemoteLossGeneration,\n        LocalLossGeneration,\n        WeatherLossGeneration,\n        LimitLossGeneration,\n        TheoreticalGeneration,\n        ROW_NUMBER() OVER (ORDER BY time DESC) AS rn_desc,\n        ROW_NUMBER() OVER (ORDER BY time ASC) AS rn_asc\n    FROM file('%s', Parquet)\nWHERE\n time>='%s' and time<'%s' ),\nAggregatedValues AS (\n    SELECT\n        count(MC006) AS Count,\n        MAX(CASE WHEN rn_desc = 1 THEN MC006 END) AS latest_mc006,\n        MAX(CASE WHEN rn_asc = 1 THEN preTotalGeneration END) AS earliest_preTotalGeneration,\n        SUM(FaultLossGeneration) AS sum_fault_loss,\n        SUM(OverhaulLossGeneration) AS sum_overhaul_loss,\n        SUM(MaintainLossGeneration) AS sum_maintain_loss,\n        SUM(GridLossGeneration) AS sum_grid_loss,\n        SUM(RemoteLossGeneration) AS sum_remote_loss,\n        SUM(LocalLossGeneration) AS sum_local_loss,\n        SUM(WeatherLossGeneration) AS sum_weather_loss,\n        SUM(LimitLossGeneration) AS sum_limit_loss,\n        SUM(TheoreticalGeneration) AS sum_theoretical_gen\n    FROM SourceData\n),\nCalculatedMetrics AS (\n    SELECT\n        Count,\n        latest_mc006,\n        earliest_preTotalGeneration,\n        sum_theoretical_gen,\n        (latest_mc006 - earliest_preTotalGeneration) AS actual_generation_delta,\n        (sum_fault_loss + sum_overhaul_loss + sum_maintain_loss + \n         sum_grid_loss + sum_remote_loss + sum_local_loss + \n         sum_weather_loss) / 3600.0 AS total_loss_div_3600,\n        sum_fault_loss / 3600.0 / 10000.0 AS FaultLossGeneration,\n        sum_overhaul_loss / 3600.0 / 10000.0 AS OverhaulLossGeneration,\n        sum_maintain_loss / 3600.0 / 10000.0 AS MaintainLossGeneration,\n        sum_grid_loss / 3600.0 / 10000.0 AS GridLossGeneration,\n        sum_local_loss / 3600.0 / 10000.0 AS LocalLossGeneration,\n        sum_remote_loss / 3600.0 / 10000.0 AS RemoteLossGeneration,\n        sum_weather_loss / 3600.0 / 10000.0 AS WeatherLossGeneration,\n        sum_limit_loss / 3600.0 / 10000.0 AS LimitLossGeneration\n    FROM AggregatedValues\n)\nSELECT\n    Count,\n    (1.0 - (actual_generation_delta / \n            NULLIF(actual_generation_delta + total_loss_div_3600, 0))) * 100.0 AS Discrepancy,\n    (actual_generation_delta / NULLIF(sum_theoretical_gen, 0)) * 3600.0 * 100.0 AS EnergyAvailability,\n    FaultLossGeneration,\n    OverhaulLossGeneration,\n    MaintainLossGeneration,\n    GridLossGeneration,\n    LocalLossGeneration,\n    RemoteLossGeneration,\n    WeatherLossGeneration,\n    LimitLossGeneration\nFROM CalculatedMetrics;", parquetfile, timePoints[i], timePoints[i+1])
						}

						//jsonData := DoSqlProcessJson(sql)
						jsonData, _ := utils.ExecuteCHQueryJSON(taskConfig, sql)
						var response utils.CHDBJsonStruct
						err := json.Unmarshal(jsonData, &response)
						if err != nil {
							log.Fatalf("Error unmarshaling JSON: %v", err)
						}
						thisResult = append(thisResult, response.Data...)
					}
				}
				groupResult := MergeData(thisResult)
				date, _ := time.Parse("2006-01-02 15:04:05", timePoints[0])
				groupResult["Time"] = date
				groupResult["DeviceName"] = key
				//calculate the expression
				for k, v := range p.Expression {
					expression2, err := govaluate.NewEvaluableExpression(v)
					if err != nil {
						fmt.Println("表达式语法错误: %v", err)
					}
					groupResult[k], err = expression2.Evaluate(groupResult)
					if err != nil {
						log.Fatalf("计算错误: %v", err)
					}
				}

				mergeResult = append(mergeResult, groupResult)
			}
		}
		file, err := os.Create(fileDirectory + "/" + "all.csv")
		if err != nil {
			ReportLogger.Errorf("Error creating CSV file:", err)
		}

		err = ReportDataToCSV(mergeResult, p.TranColumns, file)
		if err != nil {
			ReportLogger.Errorf("Error writing to CSV:", err)
		}
		mergeResult = make([]map[string]interface{}, 0)
		fileList = append(fileList, fileDirectory+"/"+"all.csv")
		file.Close()

	} else {
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
					//jsonData := DoSqlProcessJson(sql)
					jsonData, _ := utils.ExecuteCHQueryJSON(taskConfig, sql)
					var response utils.CHDBJsonStruct
					err := json.Unmarshal(jsonData, &response)
					if err != nil {
						log.Fatalf("Error unmarshaling JSON: %v", err)
					}
					//deviceDataList = append(deviceDataList, response.Data...)

					groupResult := MergeData(response.Data)
					date, _ := time.Parse("2006-01-02 15:04:05", timePoints[i])

					groupResult["Time"] = date
					groupResult["DeviceName"] = key

					//calculate the expression
					for k, v := range p.Expression {
						expression2, err := govaluate.NewEvaluableExpression(v)
						if err != nil {
							fmt.Println("表达式语法错误: %v", err)
						}
						groupResult[k], err = expression2.Evaluate(groupResult)
						if err != nil {
							log.Fatalf("计算错误: %v", err)
						}
					}

					filename = key
					mergeResult = append(mergeResult, groupResult)
				}
			}
			if p.Operation.Merge == false {
				file, err := os.Create(fileDirectory + "/" + filename + ".csv")
				if err != nil {
					ReportLogger.Errorf("Error creating CSV file:", err)
					continue
				}

				err = ReportDataToCSV(mergeResult, p.TranColumns, file)
				if err != nil {
					ReportLogger.Errorf("Error writing to CSV:", err)
					continue
				}
				fileList = append(fileList, fileDirectory+"/"+filename+".csv")
				mergeResult = make([]map[string]interface{}, 0)
				file.Close()
			}
		}
		if p.Operation.Merge == true && len(mergeResult) > 0 {
			file, err := os.Create(fileDirectory + "/" + "all.csv")
			if err != nil {
				ReportLogger.Errorf("Error creating CSV file:", err)
			}

			err = ReportDataToCSV(mergeResult, p.TranColumns, file)
			if err != nil {
				ReportLogger.Errorf("Error writing to CSV:", err)
			}
			mergeResult = make([]map[string]interface{}, 0)
			fileList = append(fileList, fileDirectory+"/"+"all.csv")
			file.Close()
		}
	}

	zipFile, err := os.Create(p.FilePath)
	if err != nil {
		ReportLogger.Errorf("Error creating zip file:", err)
		return err
	}

	// Create a zip writer.
	zipWriter := zip.NewWriter(zipFile)

	// Add each file to the zip archive.
	for _, filename := range fileList {
		err := addFileToZip(zipWriter, filename)
		if err != nil {
			ReportLogger.Errorf("Error adding file to zip:", err)
			return err
		}
	}

	ReportLogger.Infof("Successfully created zip archive:", p.FilePath)
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

func HandleCalculateReportTask(ctx context.Context, t *asynq.Task) error {
	lumberjackLogger, taskLogger := logger.GenerateNewLogger("./task_log/calculate_parquet.log", taskConfig.LogMaxSizeMB, taskConfig.LogMaxBackups, taskConfig.LogMaxAgeDays, taskConfig.LogCompress, taskConfig.LogLevel)
	defer lumberjackLogger.Close()

	//接收任务数据.
	var p CalculateReportPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		taskLogger.Error("json.Unmarshal failed for DownloadReportPayload:", err)
		return fmt.Errorf("json.Unmarshal failed: %v: %w", err, asynq.SkipRetry)
	}

	// Now, a single call handles all three report calculations and insertions efficiently.
	if err := utils.CalculateAndInsertCombinedReport(p.FilePath, p.ReportKey, ReportLogger, taskConfig.ClickHouseURL); err != nil {
		ReportLogger.Errorf("Calculate Combined Report task error: %s", err)
	}
	//
	//
	//if err := CalculateSummaryData(p.FilePath, p.ReportKey, ReportLogger, taskConfig.ClickHouseURL); err != nil {
	//	ReportLogger.Errorf("Calculate Report task error: %s", err)
	//}
	//if err := CalculateTurbineAvailabilityMetrics(p.FilePath, p.ReportKey, ReportLogger, taskConfig.ClickHouseURL); err != nil {
	//	ReportLogger.Errorf("Calculate Report task error: %s", err)
	//}
	//if err := CalculateEfficiencyMetrics(p.FilePath, p.ReportKey, ReportLogger, taskConfig.ClickHouseURL); err != nil {
	//	ReportLogger.Errorf("Calculate Report task error: %s", err)
	//}

	return nil
}

func HandleDownloadEventTask(ctx context.Context, t *asynq.Task) error {
	lumberjackLogger, taskLogger := logger.GenerateNewLogger("./task_log/download_event.log", taskConfig.LogMaxSizeMB, taskConfig.LogMaxBackups, taskConfig.LogMaxAgeDays, taskConfig.LogCompress, taskConfig.LogLevel)
	defer lumberjackLogger.Close()

	var p DownloadEventPayload
	//fmt.Println(t.Payload())
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		taskLogger.Error("json.Unmarshal failed for DownloadEventPayload:", err)
		return fmt.Errorf("json.Unmarshal failed: %v: %w", err, asynq.SkipRetry)
	}

	switch p.EventType {
	case "HistoricEvents":
		request := &event_center2.Query{
			System:            p.System,
			Category:          p.Category,
			Username:          p.Username,
			FarmDeviceEntries: event_center2.ToProtoFarmDeviceEntries(p.Devices),
			FirstEvent:        p.FirstEvent,
			EventLevels:       p.EventLevels,
			StartTime:         p.From,
			EndTime:           p.To,
			Confirmed:         p.Confirmed,
			Order:             p.Order,
			Page:              p.Page,
			PageSize:          p.PageSize,
		}
		events, _, err := event_center2.GetEC().GetHistoricalEventsPage(*request)
		if err != nil {
			taskLogger.Error("查询历史事件失败:", err)
			return fmt.Errorf("查询历史事件失败: %v: %w", err, asynq.SkipRetry)
		}
		list, err := utils.ConvertToMapList(events)
		if err != nil {
			taskLogger.Error("转换历史事件失败:", err)
			return fmt.Errorf("转换历史事件失败: %v: %w", err, asynq.SkipRetry)
		}
		err = utils.ConvertMapListToCSV(list, p.Columns, p.FilePath)
		if err != nil {
			taskLogger.Error("导出历史事件失败:", err)
			return fmt.Errorf("导出历史事件失败: %v: %w", err, asynq.SkipRetry)
		}

	case "RealtimeEvents":
		realtimeEvents := make([]event_center2.Event, 0, len(p.Devices)*10)
		// 遍历每个设备组
		for _, farmDeviceEntry := range p.Devices {
			// 构造请求参数
			request := &event_center2.Query{
				System:      p.System,
				Category:    p.Category,
				FarmCode:    farmDeviceEntry.FarmCode,
				Devices:     farmDeviceEntry.DeviceNames,
				Username:    p.Username,
				FirstEvent:  p.FirstEvent,
				Confirmed:   p.Confirmed,
				EventLevels: p.EventLevels,
				Order:       p.Order,
			}

			// 调用event_center获取实时事件
			events, err := event_center2.GetEC().GetRealtimeEvents(*request)
			if err != nil {
				taskLogger.Error("查询实时事件失败，farmCode:["+farmDeviceEntry.FarmCode+"]", err)
				return fmt.Errorf("查询实时事件失败: %v: %w", err, asynq.SkipRetry)
			}
			realtimeEvents = append(realtimeEvents, events...)
		}
		list, err := utils.ConvertToMapList(realtimeEvents)
		if err != nil {
			taskLogger.Error("转换实时事件失败:", err)
			return fmt.Errorf("转换实时事件失败: %v: %w", err, asynq.SkipRetry)
		}
		err = utils.ConvertMapListToCSV(list, p.Columns, p.FilePath)
		if err != nil {
			taskLogger.Error("导出实时事件失败:", err)
			return fmt.Errorf("导出实时事件失败: %v: %w", err, asynq.SkipRetry)
		}
	}

	return nil
}

func HandleCalculateTBAPBAReportTask(ctx context.Context, t *asynq.Task) error {
	//接收任务数据.

	_, file, _, ok := runtime.Caller(0)
	if !ok {
		panic("无法获取当前文件路径")
	}

	// file 可能是相对路径，也可能是绝对路径，取决于编译时的情况。
	// 为了确保得到绝对路径，我们使用 filepath.Abs()。
	absPath, err := filepath.Abs(file)
	if err != nil {
		panic(err)
	}

	command := "/usr/local/bin/python3"
	args := []string{
		absPath + "/pbatba.py",
		"--start-time", time.Now().Format("2006-01-02"),
		"--end-time", time.Now().Format("2006-01-02"),
		"--farm", "WLTEQ",
		"--farm-ch", "乌拉特潇源",
		"--turbines", "001", "002", "003", "004", "005", "006",
		"--data-dir", "/home/data/parquet/F1241/",
	}
	fmt.Println(command)
	fmt.Println(args)

	//cmd := exec.Command(command, args...)
	//
	//// 3. 将子进程的标准输出和标准错误连接到当前程序的标准输出和标准错误
	//// 这样你就可以实时看到 Python 脚本的打印信息
	//cmd.Stdout = os.Stdout
	//cmd.Stderr = os.Stderr
	//
	//// 打印将要执行的命令，方便调试
	//fmt.Println("Executing command:", cmd.String())
	//
	//// 4. 执行命令并等待它完成
	//err := cmd.Run()
	//
	//// 5. 检查错误
	//if err != nil {
	//	// 如果命令执行失败 (例如，返回了非零的退出码)，err 会不为 nil
	//	log.Fatalf(" Command finished with error: %v", err)
	//	return err
	//}

	fmt.Println("\n Python script executed successfully.")
	return nil
}
