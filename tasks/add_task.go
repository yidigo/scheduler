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
		Addr: taskConfig.RedisAddr,
		//Password: "123456",
		DB: 8,
	})
	defer client.Close()

	payload, err := json.Marshal(map[string]interface{}{
		"UserID":     123,
		"FilePath1":  "F1245_001_202505150000_WindFarmData.parquet",
		"FilePath2":  "/home/data/parquet/20250515/F1245_001_20250515_WindFarmData.parquet",
		"TargetPath": "/home/data/parquet/20250515/F1245_001_20250515_WindFarmData.parquet",
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
		Addr: taskConfig.RedisAddr,
		DB:   8,
	})
	defer client.Close()

	payload, err := json.Marshal(dp)
	if err != nil {
		fmt.Println(err)
	}

	// 任务入队
	info, err := client.Enqueue(asynq.NewTask("parquet:download", payload), asynq.Queue("SecondDownloadS"), asynq.Retention(24*time.Hour))

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
		Addr: taskConfig.RedisAddr,
		DB:   8,
	})
	defer client.Close()

	payload, err := json.Marshal(dp)
	if err != nil {
		fmt.Println(err)
	}

	// 任务入队
	info, err := client.Enqueue(asynq.NewTask("report:download", payload), asynq.Retention(24*time.Hour), asynq.Queue(dp.ReportType), asynq.MaxRetry(0))

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

func PBATBATaskAdd() {
	scheduler := asynq.NewScheduler(
		asynq.RedisClientOpt{Addr: taskConfig.RedisAddr,
			DB: 8},
		&asynq.SchedulerOpts{},
	)

	// 2. 准备一个要被周期性调度的任务
	task, err := asynq.NewTask("report:download", nil), asynq.Retention(24*time.Hour)
	if err != nil {
		log.Fatalf("无法创建任务: %v", err)
	}

	// 3. 注册周期性任务
	entryID, err1 := scheduler.Register("1 1 * * *", task)
	if err1 != nil {
		log.Fatalf("无法注册周期性任务: %v", err)
	}
	log.Printf("成功注册周期性任务，条目ID: %s", entryID)

	// 你可以注册多个周期性任务
	// task2, _ := tasks.NewEmailReportTask(456)
	// scheduler.Register("0 2 * * *", task2) // 每天凌晨2点执行

	// 4. 运行 Scheduler
	// Run() 会阻塞，直到程序收到停止信号
	if err := scheduler.Run(); err != nil {
		log.Fatalf("Scheduler 运行失败: %v", err)
	}
}
