package redisconn

import (
	"os"
	"strings"

	"gamehub/internal/config"

	"github.com/redis/go-redis/v9"
)

func options() *redis.FailoverOptions {
	addresses := os.Getenv("SENTINEL_HOSTS")
	if addresses == "" {
		addresses = config.DefaultSentinelHosts
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
		name = config.DefaultSentinelMasterName
	}

	return &redis.FailoverOptions{
		MasterName:    name,
		SentinelAddrs: hosts,
		DialTimeout:   config.RedisDialTimeout,
		ReadTimeout:   config.RedisReadTimeout,
		WriteTimeout:  config.RedisWriteTimeout,
		MaxRetries:    config.RedisMaxRetries,
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
