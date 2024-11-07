package utils

import (
    "github.com/sirupsen/logrus"
    "os"
)

// 全局日志器
var Log = logrus.New()

// 初始化日志器
func InitLogger(logger *logrus.Logger) {
    // 设置日志输出为标准输出
    logger.Out = os.Stdout

    // 设置日志级别
    logger.SetLevel(logrus.InfoLevel)

    // 设置日志格式
    logger.SetFormatter(&logrus.TextFormatter{
        FullTimestamp: true,
    })

    // 更新全局日志器
    Log = logger
}
