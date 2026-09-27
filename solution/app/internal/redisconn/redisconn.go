package redisconn

import (
	"os"
	"strings"

	"gamehub/internal/config"

	"github.com/redis/go-redis/v9"
)

func options() *redis.FailoverOptions {
	addresses := os.Getenv(config.SentinelHostsEnv)
	if addresses == "" {
		addresses = config.DefaultSentinelHosts
	}

	rawHosts := strings.Split(addresses, ",")
	hosts := make([]string, 0, len(rawHosts))
	for _, address := range rawHosts {
		address = strings.TrimSpace(address)
		if address != "" {
			hosts = append(hosts, address)
		}
	}

	masterName := os.Getenv(config.SentinelMasterNameEnv)
	if masterName == "" {
		masterName = config.DefaultSentinelMasterName
	}

	return &redis.FailoverOptions{
		MasterName:    masterName,
		SentinelAddrs: hosts,
		DialTimeout:   config.RedisDialTimeout,
		ReadTimeout:   config.RedisReadTimeout,
		WriteTimeout:  config.RedisWriteTimeout,
		MaxRetries:    config.RedisMaxRetries,
	}
}

func Master() *redis.Client {
	failoverOptions := options()
	return redis.NewFailoverClient(failoverOptions)
}

func Replica() *redis.Client {
	failoverOptions := options()
	failoverOptions.ReplicaOnly = true
	return redis.NewFailoverClient(failoverOptions)
}
