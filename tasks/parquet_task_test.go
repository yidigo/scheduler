package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/hibiken/asynq"
	"testing"
	"time"
)

func TestMergeParquetTaskAdd(t *testing.T) {
	type args struct {
		filePath    string
		parquetPath string
	}
	tests := []struct {
		name string
		args args
	}{
		{name: "m1", args: args{filePath: "/home/dbservice/file_F1008_0_testdata-2024-12-31-11.parquet", parquetPath: "/tmp/"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			MergeParquetTaskAdd(tt.args.filePath, tt.args.parquetPath)
		})
	}
}

//
//func TestHandleTestTimeSleepTask(t *testing.T) {
//	type TestTimeSleepPayload struct {
//		UserID    int
//		SleepTime int
//	}
//	tests := []struct {
//		name    string
//		args    TestTimeSleepPayload
//	}{
//		{name: "t1", args: TestTimeSleepPayload{UserID: 111,SleepTime: 10},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//		TestTimeSleepTaskAdd(tt.args.filePath, tt.args.parquetPath)
//		})
//	}
//}
//

func TestDownloadParquetTaskAdd(t *testing.T) {
	p := DownloadParquetPayload{
		Devices: []string{"001", "002"},

		Start:   time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), // One hour ago
		End:     time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC),
		Columns: []string{"TheoreticalGeneration", "preTotalGeneration", "preTotalEleConsumption"},
		TranColumns: map[string]string{
			"standard_name1": "nick_name1",
			"standard_name2": "nick_name2",
		},
		Operation: SecondOperation{
			Resample: "2 HOUR", // Resample every 10 seconds
			Merge:    true,
		},
		FilePath: "/root/output.gzip",
	}

	type args struct {
		dp DownloadParquetPayload
	}
	tests := []struct {
		name string
		args args
	}{
		{name: "t1", args: args{dp: p}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			DownloadParquetTaskAdd(tt.args.dp)
		})
	}
}

// go test -run TestHandleDownloadParquetTask
func TestHandleDownloadParquetTask(t *testing.T) {
	p := DownloadParquetPayload{
		Devices: []string{"001", "002"},

		Start:   time.Date(2025, 4, 10, 0, 0, 0, 0, time.UTC), // One hour ago
		End:     time.Date(2025, 4, 21, 0, 0, 0, 0, time.UTC),
		Columns: []string{"TheoreticalGeneration", "preTotalGeneration", "preTotalEleConsumption"},
		TranColumns: map[string]string{
			"TheoreticalGeneration":  "nick_name1",
			"preTotalGeneration":     "nick_name2",
			"preTotalEleConsumption": "nick_name3",
		},
		Operation: SecondOperation{
			Resample:  "1 HOUR", // Resample every 10 seconds
			Merge:     true,
			PointType: map[string][]string{"TheoreticalGeneration": []string{"avg", "max"}, "preTotalGeneration": []string{"max"}, "preTotalEleConsumption": []string{"min"}},
		},
		FilePath: "/root/output.gzip",
	}

	payload, err := json.Marshal(p)
	if err != nil {
		fmt.Println(err)
	}

	aa := asynq.NewTask("parquet:download", payload)

	type args struct {
		ctx context.Context
		t   *asynq.Task
	}
	tests := []struct {
		name string
		args args
	}{
		{name: "t1", args: args{ctx: nil, t: aa}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := HandleDownloadParquetTask(tt.args.ctx, tt.args.t); err != nil {
				t.Errorf("HandleDownloadParquetTask() error = %v", err)
			}
		})
	}
}

func TestDownloadReportTaskAdd(t *testing.T) {
	req := DownloadReportPayload{
		Groups: []map[string][]string{
			{
				"F1245_001": {"F1245_001"},
			}, {
				"F1245_002": {"F1245_002"},
			},
		},
		ReportType: "SummaryReport",
		Start:      time.Date(2025, 5, 22, 0, 0, 0, 0, time.UTC),
		End:        time.Date(2025, 5, 22, 1, 0, 0, 0, time.UTC),
		Columns:    []string{"Time", "DeviceName", "GriActiveEnergyDelTotal", "GriActiveEnergyRcvSection", "GriActivePowerTotalMax", "GriReactivePowerTotalMin", "TheoreticalGeneration", "PreTotalEleConsumptionFirst"},
		TranColumns: map[string]string{
			"Time":                        "Time",
			"DeviceName":                  "DeviceName",
			"GriActiveEnergyDelTotal":     "GriActiveEnergyDelTotal",
			"GriActiveEnergyRcvSection":   "GriActiveEnergyRcvSection",
			"GriActivePowerTotalMax":      "GriActivePowerTotalMax",
			"GriReactivePowerTotalMin":    "GriReactivePowerTotalMin",
			"TheoreticalGeneration":       "TheoreticalGeneration",
			"PreTotalEleConsumptionFirst": "PreTotalEleConsumptionFirst",
		},
		Operation: ReportOperation{
			"10min",
			false,
		},
		FilePath:      "/tmp/tt1.zip",
		TaskStartTime: time.Now(),
		Language:      "en",
	}

	type args struct {
		filePath    string
		parquetPath string
	}
	tests := []struct {
		name string
		args args
	}{
		{name: "m1", args: args{filePath: "", parquetPath: "/tmp/"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			DownloadReportTaskAdd(req)
		})
	}
}

//func TestContinuousQueries(t *testing.T) {
//	type args struct {
//		ctx        context.Context
//		parquet1   string
//		parquetOut string
//		interval   string
//	}
//	tests := []struct {
//		name string
//		args args
//	}{
//		{"t1",
//			args{parquet1: "/home/data/F1240/second/2025-04-23/F1240_001_20250423_WindFarmData.parquet", parquetOut: "/home/data/F1240/second/2025-04-23/F1240_001_20250423_WindFarmData_10min.parquet", interval: "10 minute"}},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			if err := ContinuousQueries(tt.args.parquet1, tt.args.parquetOut, tt.args.interval); err != nil {
//				t.Errorf("ContinuousQueries() error = %v", err)
//			}
//		})
//	}
//}

func TestDownloadReportPeriodTaskAdd(t *testing.T) {
	req := DownloadReportPayload{
		Groups: []map[string][]string{
			{
				"F1245_001": {"F1245_001", "F1245_002"},
			}, {
				"F1245_002": {"F1245_002", "F1245_002"},
			},
		},
		ReportType: "TurbineAvailabilityMetrics",
		Start:      time.Date(2025, 5, 22, 2, 3, 0, 0, time.UTC),
		End:        time.Date(2025, 5, 23, 1, 0, 0, 0, time.UTC),
		Columns:    []string{"Time", "DeviceName", "GriActiveEnergyDelTotal", "GriActiveEnergyRcvSection", "GriActivePowerTotalMax", "GriReactivePowerTotalMin", "TheoreticalGeneration", "PreTotalEleConsumptionFirst"},
		TranColumns: map[string]string{
			"Time":                        "Time",
			"DeviceName":                  "DeviceName",
			"GriActiveEnergyDelTotal":     "GriActiveEnergyDelTotal",
			"GriActiveEnergyRcvSection":   "GriActiveEnergyRcvSection",
			"GriActivePowerTotalMax":      "GriActivePowerTotalMax",
			"GriReactivePowerTotalMin":    "GriReactivePowerTotalMin",
			"TheoreticalGeneration":       "TheoreticalGeneration",
			"PreTotalEleConsumptionFirst": "PreTotalEleConsumptionFirst",
		},
		Operation: ReportOperation{
			"time",
			false,
		},
		FilePath:      "/tmp/tt1.zip",
		TaskStartTime: time.Now(),
		Language:      "en",
	}

	type args struct {
		filePath    string
		parquetPath string
	}
	tests := []struct {
		name string
		args args
	}{
		{name: "m1", args: args{filePath: "", parquetPath: "/tmp/"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			DownloadReportTaskAdd(req)
		})
	}
}
