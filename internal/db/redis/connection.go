package redis

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

func GetRedisClient(dsn string) (*goredis.Client, error) {
	opt, err := goredis.ParseURL(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse redis DSN: %v", err)
	}
	client := goredis.NewClient(opt)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to ping redis: %v", err)
	}
	return client, nil
}