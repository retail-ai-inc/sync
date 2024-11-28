package utils

import (
    "github.com/sirupsen/logrus"
    "os"
)

var Log = logrus.New()

func InitLogger(logger *logrus.Logger) {
    logger.Out = os.Stdout

    logger.SetLevel(logrus.InfoLevel)

    logger.SetFormatter(&logrus.TextFormatter{
        FullTimestamp: true,
    })

    Log = logger
}
