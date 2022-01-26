package config

import (
	"os"
	"strconv"
)

func GetKafkaBrokers() string {
	return getenv("KAFKA_BROKERS", "127.0.0.1:9092")
}

func GetRetryCount() int {
	res, err := strconv.Atoi(getenv("RETRY_COUNT", "5"))
	if err != nil {
		return 5
	}
	return res
}

func getenv(key string, fallback string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		return fallback
	}
	return value
}
