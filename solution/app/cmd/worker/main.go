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

	"gamehub/internal/redisconn"

	"github.com/redis/go-redis/v9"
)

const (
	stream = "notifications"
	group  = "notifications-group"
)

func main() {
	client := redisconn.Master()
	defer client.Close()

	consumer := os.Getenv("NOTIFICATIONS_CONSUMER")
	if consumer == "" {
		consumer = "worker-1"
	}

	ctx := context.Background()
	groupReady := false
	var lastTrim time.Time

	for {
		if !groupReady {
			if err := client.Ping(ctx).Err(); err != nil {
				log.Printf("Redis worker error: %v", err)
				time.Sleep(2 * time.Second)
				continue
			}
			if err := ensureGroup(ctx, client); err != nil {
				log.Printf("Redis worker error: %v", err)
				time.Sleep(2 * time.Second)
				continue
			}
			if err := drainPending(ctx, client, consumer); err != nil {
				log.Printf("Redis worker error: %v", err)
				time.Sleep(2 * time.Second)
				continue
			}
			groupReady = true
		}

		if time.Since(lastTrim) >= time.Minute {
			// У записей Stream нет своего TTL, поэтому режем по времени в ID.
			cutoff := time.Now().Add(-7 * 24 * time.Hour).UnixMilli()
			if err := client.XTrimMinID(ctx, stream, fmt.Sprintf("%d-0", cutoff)).Err(); err != nil {
				log.Printf("Redis worker error: %v", err)
				groupReady = false
				time.Sleep(2 * time.Second)
				continue
			}
			lastTrim = time.Now()
		}

		messages, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    group,
			Consumer: consumer,
			Streams:  []string{stream, ">"},
			Count:    100,
			Block:    5 * time.Second,
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
			time.Sleep(2 * time.Second)
		}
	}
}

func ensureGroup(ctx context.Context, client *redis.Client) error {
	err := client.XGroupCreateMkStream(ctx, stream, group, "$").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

func drainPending(ctx context.Context, client *redis.Client, consumer string) error {
	for {
		// "0" возвращает сообщения, уже выданные этому consumer, но ещё без XACK.
		messages, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    group,
			Consumer: consumer,
			Streams:  []string{stream, "0"},
			Count:    100,
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
			if err := client.XAck(ctx, stream, group, message.ID).Err(); err != nil {
				return err
			}
		}
	}
	return nil
}
