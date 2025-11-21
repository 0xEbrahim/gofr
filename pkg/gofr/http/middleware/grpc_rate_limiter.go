package middleware

import (
	"context"

	"sync"
	"time"

	"golang.org/x/time/rate"
)

type GRPCRateLimiterConfig struct {
	RequestsPerSecond float64
	Burst             int
	PerIP             bool
}

type grpcRateLimiter struct {
	limiters sync.Map // map[string]*rate.Limiter for per-IP
	global   *rate.Limiter
	config   GRPCRateLimiterConfig
	metrics  rateLimiterMetrics
}

type rateLimiterMetrics interface {
	IncrementCounter(ctx context.Context, name string, labels ...string)
}

func NewGRPCRateLimiter(config GRPCRateLimiterConfig, metrics rateLimiterMetrics) *grpcRateLimiter {
	rl := &grpcRateLimiter{
		config:  config,
		metrics: metrics,
	}

	if !config.PerIP {
		rl.global = rate.NewLimiter(rate.Limit(config.RequestsPerSecond), config.Burst)
	}

	if config.PerIP {
		go rl.cleanupStaleEntries()
	}

	return rl
}

func (rl *grpcRateLimiter) getLimiter(ip string) *rate.Limiter {
	if !rl.config.PerIP {
		return rl.global
	}

	if limiter, exists := rl.limiters.Load(ip); exists {
		return limiter.(*rate.Limiter)
	}

	limiter := rate.NewLimiter(rate.Limit(rl.config.RequestsPerSecond), rl.config.Burst)
	rl.limiters.Store(ip, limiter)
	return limiter
}

func (rl *grpcRateLimiter) cleanupStaleEntries() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		rl.limiters.Range(func(key, value interface{}) bool {
			limiter := value.(*rate.Limiter)
			if limiter.Tokens() == float64(rl.config.Burst) {
				rl.limiters.Delete(key)
			}
			return true
		})
	}
}
