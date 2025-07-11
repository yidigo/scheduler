package tasks

import (
	"scheduler/config"
)

var (
	//taskLogger *logger.Logger
	taskConfig *config.AppConfig
)

//func SetGlobalLogger(l *logger.Logger) {
//	taskLogger = l
//}
//
//func GetGlobalLogger() *logger.Logger {
//	return taskLogger
//}

func SetGlobalConfig(c *config.AppConfig) {
	taskConfig = c
}

func GetGlobalConfig() *config.AppConfig {
	return taskConfig
}
