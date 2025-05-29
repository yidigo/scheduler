package main

import (
	"github.com/hibiken/asynq"
	"log"
	"scheduler/tasks"
)

func main() {
	srv := asynq.NewServer(
		asynq.RedisClientOpt{
			//Addr: "127.0.0.1:6379",
			//Addr: "10.162.74.33:59010",
			Addr: "127.0.0.1:59010",
			//Password: "123456",
			DB: 8,
		},
		asynq.Config{
			// 每个进程并发执行的worker数量
			Concurrency: 10,
			// Optionally specify multiple queues with different priority.
			Queues: map[string]int{
				"critical":                   10,
				"merge":                      30,
				"default":                    10,
				"second":                     5,
				"SummaryReport":              5,
				"TurbineAvailabilityMetrics": 5,
				"EfficiencyMetrics":          5,
			},
			// See the godoc for other configuration options
		},
	)

	mux := asynq.NewServeMux()
	//mux.HandleFunc(tasks.TypeParquetMergeCpp, tasks.HandleMergeParquetCppTask)
	mux.HandleFunc(tasks.TypeParquetMerge, tasks.HandleMergeParquetTask)
	mux.HandleFunc(tasks.TypeDownloadSecond, tasks.HandleDownloadParquetTask)
	mux.HandleFunc(tasks.TypeDownloadReport, tasks.HandleDownloadReportTask)

	if err := srv.Run(mux); err != nil {
		log.Fatalf("could not run server: %v", err)
	}
}
