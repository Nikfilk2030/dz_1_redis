package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"gamehub/internal/config"
	"gamehub/internal/redisconn"

	"github.com/redis/go-redis/v9"
)

func main() {
	client := redisconn.Master()
	defer client.Close()

	consumer := os.Getenv(config.WorkerConsumerEnv)
	if consumer == "" {
		consumer = config.DefaultWorkerConsumer
	}

	ctx := context.Background()
	groupReady := false
	var lastTrim time.Time

	for {
		if !groupReady {
			if err := client.Ping(ctx).Err(); err != nil {
				log.Printf("Redis worker error: %v", err)
				time.Sleep(config.WorkerRetryDelay)
				continue
			}
			if err := ensureGroup(ctx, client); err != nil {
				log.Printf("Redis worker error: %v", err)
				time.Sleep(config.WorkerRetryDelay)
				continue
			}
			if err := drainPending(ctx, client, consumer); err != nil {
				log.Printf("Redis worker error: %v", err)
				time.Sleep(config.WorkerRetryDelay)
				continue
			}
			groupReady = true
		}

		if time.Since(lastTrim) >= config.WorkerTrimInterval {
			// У записей Stream нет своего TTL, поэтому режем по времени в ID
			oldestAllowedTime := time.Now().Add(-config.NotificationsRetention)
			oldestNotificationID := fmt.Sprintf("%d-0", oldestAllowedTime.UnixMilli())
			if err := client.XTrimMinID(ctx, config.NotificationsStream, oldestNotificationID).Err(); err != nil {
				log.Printf("Redis worker error: %v", err)
				groupReady = false
				time.Sleep(config.WorkerRetryDelay)
				continue
			}
			lastTrim = time.Now()
		}

		messages, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    config.NotificationsGroup,
			Consumer: consumer,
			Streams:  []string{config.NotificationsStream, config.StreamNewMessagesID},
			Count:    config.WorkerReadBatchSize,
			Block:    config.WorkerReadBlock,
		}).Result()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err == nil {
			err = handleMessages(ctx, client, messages)
		}
		if err != nil {
			log.Printf("Redis worker error: %v", err)
			groupReady = false
			time.Sleep(config.WorkerRetryDelay)
		}
	}
}

func ensureGroup(ctx context.Context, client *redis.Client) error {
	err := client.XGroupCreateMkStream(ctx, config.NotificationsStream, config.NotificationsGroup, config.StreamGroupNewOnlyID).Err()
	if err != nil && !strings.HasPrefix(err.Error(), config.RedisBusyGroupPrefix) {
		return err
	}
	return nil
}

func drainPending(ctx context.Context, client *redis.Client, consumer string) error {
	for {
		// Забираем сообщения, которые этот worker получил раньше, но не успел подтвердить
		messages, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    config.NotificationsGroup,
			Consumer: consumer,
			Streams:  []string{config.NotificationsStream, config.StreamPendingID},
			Count:    config.WorkerReadBatchSize,
		}).Result()
		if errors.Is(err, redis.Nil) {
			return nil
		}
		if err != nil {
			return err
		}

		count := 0
		for _, batch := range messages {
			count += len(batch.Messages)
		}
		if count == 0 {
			return nil
		}
		if err := handleMessages(ctx, client, messages); err != nil {
			return err
		}
	}
}

func handleMessages(ctx context.Context, client *redis.Client, streams []redis.XStream) error {
	for _, batch := range streams {
		for _, message := range batch.Messages {
			if message.Values != nil {
				fields := map[string]interface{}{"id": message.ID}
				for key, value := range message.Values {
					fields[key] = value
				}
				data, err := json.Marshal(fields)
				if err != nil {
					return err
				}
				fmt.Println(string(data))
			}
			if err := client.XAck(ctx, config.NotificationsStream, config.NotificationsGroup, message.ID).Err(); err != nil {
				return err
			}
		}
	}
	return nil
}
