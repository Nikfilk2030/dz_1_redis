package redisconn

import (
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

func options() *redis.FailoverOptions {
	addresses := os.Getenv("SENTINEL_HOSTS")
	if addresses == "" {
		addresses = "sentinel-1:26379,sentinel-2:26379,sentinel-3:26379"
	}

	hosts := make([]string, 0, 3)
	for _, address := range strings.Split(addresses, ",") {
		address = strings.TrimSpace(address)
		if address != "" {
			hosts = append(hosts, address)
		}
	}

	name := os.Getenv("SENTINEL_MASTER_NAME")
	if name == "" {
		name = "mymaster"
	}

	return &redis.FailoverOptions{
		MasterName:    name,
		SentinelAddrs: hosts,
		DialTimeout:   2 * time.Second,
		ReadTimeout:   10 * time.Second,
		WriteTimeout:  10 * time.Second,
		MaxRetries:    3,
	}
}

func Master() *redis.Client {
	return redis.NewFailoverClient(options())
}

func Replica() *redis.Client {
	opts := options()
	opts.ReplicaOnly = true
	return redis.NewFailoverClient(opts)
}
