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
		DB: 2,
	})
	defer client.Close()

	// 初使货需要传递的数据
	task, err := NewMergeParquetTask(1, filePath, parquetPath, time.Now().Format("2006-01-02 15:04:05"))
	if err != nil {
		log.Fatalf("could not create task: %v", err)
	}
	// 任务入队
	info, err := client.Enqueue(task)

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

func DownloadParquetTaskAdd(dp DownloadParquetPayload) {
	client := asynq.NewClient(asynq.RedisClientOpt{
		Addr: REDISRUL,
		DB:   2,
	})
	defer client.Close()

	payload, err := json.Marshal(dp)
	if err != nil {
		fmt.Println(err)
	}

	// 任务入队
	info, err := client.Enqueue(asynq.NewTask("parquet:download", payload), asynq.Retention(24*time.Hour))

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
