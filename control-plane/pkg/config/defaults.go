package config

import "time"

const (
	DefaultGRPCPort = 50050
	DefaultHTTPPort = 8080
	DefaultHost     = ""

	DefaultHeartbeatTimeout = 10 * time.Second
	DefaultMaxWorkers       = 100

	DefaultFairSchedulerSlots = 8
	DefaultCoalesceThreshold  = int64(20000)
	DefaultBroadcastThreshold = int64(50000)

	DefaultRateLimit = 1000
	DefaultLogLevel  = "info"
	DefaultLogFormat = "text"
)