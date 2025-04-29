package tasks

import (
	"encoding/json"
	"fmt"
	"github.com/hibiken/asynq"
	"log"
	"time"
)

func MergeParquetTaskAdd(filePath, parquetPath string) {
	client := asynq.NewClient(asynq.RedisClientOpt{
		Addr: REDISRUL,
		//Password: "123456",
		DB: 8,
	})
	defer client.Close()

	payload, err := json.Marshal(map[string]interface{}{
		"UserID":     123,
		"FilePath1":  "/home/data/a.parquet",
		"FilePath2":  "/home/data/b.parquet",
		"TargetPath": "/home/data/c.parquet",
		"DataStr":    time.Now().Format("2006-01-02-15-04-05"),
	})
	if err != nil {
		fmt.Println(err)
	}

	// 任务入队
	info, err := client.Enqueue(asynq.NewTask("parquet:merge", payload), asynq.Queue("merge"), asynq.Retention(24*time.Hour))

	if err != nil {
		log.Fatalf("could not enqueue task: %v", err)
	}
	log.Printf("enqueued task: id=%s queue=%s", info.ID, info.Queue)
}

func DownloadParquetTaskAdd(dp DownloadParquetPayload) {
	client := asynq.NewClient(asynq.RedisClientOpt{
		Addr: REDISRUL,
		DB:   8,
	})
	defer client.Close()

	payload, err := json.Marshal(dp)
	if err != nil {
		fmt.Println(err)
	}

	// 任务入队
	info, err := client.Enqueue(asynq.NewTask("parquet:download", payload), asynq.Queue("second"), asynq.Retention(24*time.Hour))

	//info, err := client.Enqueue(task, time.Now())
	// 延迟执行
	//info, err := client.Enqueue(task, asynq.ProcessIn(3*time.Second))
	// MaxRetry 重度次数 Timeout超时时间
	//info, err = client.Enqueue(task, asynq.MaxRetry(10), asynq.Timeout(3*time.Second))
	if err != nil {
		log.Fatalf("could not enqueue task: %v", err)
	}
	log.Printf("enqueued task: id=%s queue=%s", info.ID, info.Queue)
}

func DownloadReportTaskAdd(dp DownloadReportPayload) {
	client := asynq.NewClient(asynq.RedisClientOpt{
		Addr: REDISRUL,
		DB:   8,
	})
	defer client.Close()

	payload, err := json.Marshal(dp)
	if err != nil {
		fmt.Println(err)
	}

	// 任务入队
	info, err := client.Enqueue(asynq.NewTask("report:download", payload), asynq.Retention(24*time.Hour), asynq.Queue("report"), asynq.MaxRetry(0))

	//info, err := client.Enqueue(task, time.Now())
	// 延迟执行
	//info, err := client.Enqueue(task, asynq.ProcessIn(3*time.Second))
	// MaxRetry 重度次数 Timeout超时时间
	//info, err = client.Enqueue(task, asynq.MaxRetry(10), asynq.Timeout(3*time.Second))
	if err != nil {
		log.Fatalf("could not enqueue task: %v", err)
	}
	log.Printf("enqueued task: id=%s queue=%s", info.ID, info.Queue)
}
