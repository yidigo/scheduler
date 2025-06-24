package tasks

import (
	"scheduler/config"
	"scheduler/logger"
)

var (
	taskLogger *logger.Logger
	taskConfig *config.AppConfig
)

func SetGlobalLogger(l *logger.Logger) {
	taskLogger = l
}

func SetGlobalConfig(c *config.AppConfig) {
	taskConfig = c
}
