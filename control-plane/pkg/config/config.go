package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	GRPCPort int
	HTTPPort int
	Host     string

	HeartbeatTimeout time.Duration
	MaxWorkers       int

	FairSchedulerSlots int
	CoalesceThreshold  int64
	BroadcastThreshold int64

	APIKeys      []string
	RateLimit    int
	AllowOrigins []string

	LogLevel  string
	LogFormat string
}

func FromEnv() *Config {
	cfg := &Config{
		GRPCPort:           DefaultGRPCPort,
		HTTPPort:           DefaultHTTPPort,
		Host:               DefaultHost,
		HeartbeatTimeout:   DefaultHeartbeatTimeout,
		MaxWorkers:         DefaultMaxWorkers,
		FairSchedulerSlots: DefaultFairSchedulerSlots,
		CoalesceThreshold:  DefaultCoalesceThreshold,
		BroadcastThreshold: DefaultBroadcastThreshold,
		RateLimit:          DefaultRateLimit,
		LogLevel:           DefaultLogLevel,
		LogFormat:          DefaultLogFormat,
	}

	if v := os.Getenv("OXIDESTREAM_GRPC_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			cfg.GRPCPort = port
		}
	}
	if v := os.Getenv("OXIDESTREAM_HTTP_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			cfg.HTTPPort = port
		}
	}
	if v := os.Getenv("OXIDESTREAM_HOST"); v != "" {
		cfg.Host = v
	}
	if v := os.Getenv("OXIDESTREAM_HEARTBEAT_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.HeartbeatTimeout = d
		}
	}
	if v := os.Getenv("OXIDESTREAM_MAX_WORKERS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.MaxWorkers = n
		}
	}
	if v := os.Getenv("OXIDESTREAM_FAIR_SCHEDULER_SLOTS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.FairSchedulerSlots = n
		}
	}
	if v := os.Getenv("OXIDESTREAM_COALESCE_THRESHOLD"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			cfg.CoalesceThreshold = n
		}
	}
	if v := os.Getenv("OXIDESTREAM_BROADCAST_THRESHOLD"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			cfg.BroadcastThreshold = n
		}
	}
	if v := os.Getenv("OXIDESTREAM_API_KEYS"); v != "" {
		cfg.APIKeys = strings.Split(v, ",")
	}
	if v := os.Getenv("OXIDESTREAM_RATE_LIMIT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.RateLimit = n
		}
	}
	if v := os.Getenv("OXIDESTREAM_ALLOW_ORIGINS"); v != "" {
		cfg.AllowOrigins = strings.Split(v, ",")
	}
	if v := os.Getenv("OXIDESTREAM_LOG_LEVEL"); v != "" {
		cfg.LogLevel = v
	}
	if v := os.Getenv("OXIDESTREAM_LOG_FORMAT"); v != "" {
		cfg.LogFormat = v
	}

	return cfg
}

func Load() *Config {
	return FromEnv()
}