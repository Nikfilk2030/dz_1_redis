package config

import "time"

const (
	DefaultSentinelHosts      = "sentinel-1:26379,sentinel-2:26379,sentinel-3:26379"
	DefaultSentinelMasterName = "mymaster"
	RedisDialTimeout          = 2 * time.Second
	RedisReadTimeout          = 10 * time.Second
	RedisWriteTimeout         = 10 * time.Second
	RedisMaxRetries           = 3
	RedisBusyGroupPrefix      = "BUSYGROUP"

	LeaderboardKey         = "tournament:main"
	NotificationsStream    = "notifications"
	NotificationsGroup     = "notifications-group"
	LevelChangedEventType  = "level_changed"
	StreamGroupNewOnlyID   = "$"
	StreamNewMessagesID    = ">"
	StreamPendingID        = "0"
	NotificationsRetention = 7 * 24 * time.Hour
	TimestampLayout        = "2006-01-02T15:04:05.000000-07:00"

	APIListenAddress                 = ":8000"
	RedisStartupTimeout              = time.Minute
	RedisStartupAttemptTimeout       = 3 * time.Second
	RedisStartupRetryDelay           = time.Second
	ProfileCacheTTL                  = time.Minute
	LoginTTL                         = 24 * time.Hour
	MaxRequestBodyBytes        int64 = 1 << 20
	MaxPlayerNameRunes               = 120
	MaxRegionRunes                   = 80
	MaxAchievementNameRunes          = 120
	MaxBatchPlayers                  = 100
	DefaultLeaderboardLimit    int64 = 10
	MaxLeaderboardLimit        int64 = 100

	DefaultWorkerConsumer = "worker-1"
	WorkerRetryDelay      = 2 * time.Second
	WorkerTrimInterval    = time.Minute
	WorkerReadBatchSize   = 100
	WorkerReadBlock       = 5 * time.Second
)
