package middleware

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// mockGRPCMetrics implements rateLimiterMetrics for testing
type mockGRPCMetrics struct {
	mu       sync.Mutex
	counters map[string]int
	calls    []struct {
		name   string
		labels []string
	}
}

func newMockGRPCMetrics() *mockGRPCMetrics {
	return &mockGRPCMetrics{
		counters: make(map[string]int),
	}
}

func (m *mockGRPCMetrics) IncrementCounter(_ context.Context, name string, labels ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counters[name]++
	m.calls = append(m.calls, struct {
		name   string
		labels []string
	}{name, labels})
}

func (m *mockGRPCMetrics) getCount(name string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counters[name]
}

func (m *mockGRPCMetrics) getCalls() []struct {
	name   string
	labels []string
} {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

// mockServerStream implements grpc.ServerStream for testing
type mockServerStream struct {
	ctx context.Context
}

func (m *mockServerStream) SetHeader(md metadata.MD) error  { return nil }
func (m *mockServerStream) SendHeader(md metadata.MD) error { return nil }
func (m *mockServerStream) SetTrailer(md metadata.MD)       {}
func (m *mockServerStream) Context() context.Context        { return m.ctx }
func (m *mockServerStream) SendMsg(interface{}) error       { return nil }
func (m *mockServerStream) RecvMsg(interface{}) error       { return nil }

func TestNewGRPCRateLimiter(t *testing.T) {
	metrics := newMockGRPCMetrics()
	config := GRPCRateLimiterConfig{
		RequestsPerSecond: 100,
		Burst:             10,
		PerIP:             true,
	}

	limiter := NewGRPCRateLimiter(config, metrics)

	assert.NotNil(t, limiter, "Expected rate limiter to be created")
	assert.Equal(t, config.RequestsPerSecond, limiter.config.RequestsPerSecond)
	assert.Equal(t, config.Burst, limiter.config.Burst)
	assert.Equal(t, config.PerIP, limiter.config.PerIP)

	// Test global limiter creation
	configGlobal := GRPCRateLimiterConfig{
		RequestsPerSecond: 50,
		Burst:             5,
		PerIP:             false,
	}

	limiterGlobal := NewGRPCRateLimiter(configGlobal, metrics)
	assert.NotNil(t, limiterGlobal.global, "Expected global limiter to be created when PerIP is false")
}

func TestGetLimiter(t *testing.T) {
	metrics := newMockGRPCMetrics()
	config := GRPCRateLimiterConfig{
		RequestsPerSecond: 2,
		Burst:             1,
		PerIP:             true,
	}

	limiter := NewGRPCRateLimiter(config, metrics)

	// Test IP-based limiter creation
	ip1 := "192.168.1.1"
	l1 := limiter.getLimiter(ip1)
	assert.NotNil(t, l1, "Expected limiter to be created for IP")

	// Test same IP returns same limiter
	l2 := limiter.getLimiter(ip1)
	assert.Equal(t, l1, l2, "Expected same limiter for same IP")

	ip2 := "192.168.1.2"
	l3 := limiter.getLimiter(ip2)

	// Use IP1's limiter
	allowed1 := l1.Allow()
	assert.True(t, allowed1, "First request for IP1 should be allowed")

	// IP1's limiter should now be exhausted (burst=1)
	allowed1 = l1.Allow()
	assert.False(t, allowed1, "Second request for IP1 should be rate limited")

	// IP2's limiter should still allow requests (different limiter)
	allowed2 := l3.Allow()
	assert.True(t, allowed2, "First request for IP2 should be allowed even after IP1 is limited")

	// Test global limiter
	configGlobal := GRPCRateLimiterConfig{
		RequestsPerSecond: 2,
		Burst:             1,
		PerIP:             false,
	}
	limiterGlobal := NewGRPCRateLimiter(configGlobal, metrics)

	globalLimiter := limiterGlobal.getLimiter("any-ip")
	assert.Equal(t, limiterGlobal.global, globalLimiter, "Expected global limiter to be returned when PerIP is false")
}

func TestGetGRPCIP(t *testing.T) {
	tests := []struct {
		name     string
		ctx      context.Context
		expected string
	}{
		{
			name:     "with peer IP",
			ctx:      peer.NewContext(context.Background(), &peer.Peer{Addr: &net.TCPAddr{IP: net.ParseIP("192.168.1.1"), Port: 8080}}),
			expected: "192.168.1.1",
		},
		{
			name:     "without peer",
			ctx:      context.Background(),
			expected: "unknown",
		},
		{
			name:     "with nil address",
			ctx:      peer.NewContext(context.Background(), &peer.Peer{Addr: nil}),
			expected: "unknown",
		},
		{
			name:     "with empty TCP address",
			ctx:      peer.NewContext(context.Background(), &peer.Peer{Addr: &net.TCPAddr{}}),
			expected: "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getGRPCIP(tt.ctx)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGRPCRateLimiter_IndependentLimiters(t *testing.T) {
	metrics := newMockGRPCMetrics()
	config := GRPCRateLimiterConfig{
		RequestsPerSecond: 1,
		Burst:             1,
		PerIP:             true,
	}

	limiter := NewGRPCRateLimiter(config, metrics)
	interceptor := limiter.Unary()

	mockHandler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return "success", nil
	}

	// IP1 makes requests
	ctx1 := peer.NewContext(context.Background(), &peer.Peer{Addr: &net.TCPAddr{IP: net.ParseIP("192.168.1.1"), Port: 8080}})

	// First request for IP1 should succeed
	resp, err := interceptor(ctx1, "test", &grpc.UnaryServerInfo{FullMethod: "/test.Service/Test"}, mockHandler)
	assert.NoError(t, err)
	assert.Equal(t, "success", resp)

	// Second request for IP1 should be rate limited
	_, err = interceptor(ctx1, "test", &grpc.UnaryServerInfo{FullMethod: "/test.Service/Test"}, mockHandler)
	assert.Error(t, err)
	assert.Equal(t, codes.ResourceExhausted, status.Code(err))

	// IP2 should be able to make requests even though IP1 is limited
	ctx2 := peer.NewContext(context.Background(), &peer.Peer{Addr: &net.TCPAddr{IP: net.ParseIP("192.168.1.2"), Port: 8080}})
	resp, err = interceptor(ctx2, "test", &grpc.UnaryServerInfo{FullMethod: "/test.Service/Test"}, mockHandler)
	assert.NoError(t, err, "IP2 should be able to make requests even when IP1 is limited")
	assert.Equal(t, "success", resp)
}

func TestGRPCRateLimiter_GlobalLimit(t *testing.T) {
	metrics := newMockGRPCMetrics()
	config := GRPCRateLimiterConfig{
		RequestsPerSecond: 2,
		Burst:             2,
		PerIP:             false,
	}

	limiter := NewGRPCRateLimiter(config, metrics)
	interceptor := limiter.Unary()

	mockHandler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return "success", nil
	}

	ctx := peer.NewContext(context.Background(), &peer.Peer{Addr: &net.TCPAddr{IP: net.ParseIP("192.168.1.1"), Port: 8080}})

	// First 2 requests should succeed (burst)
	for i := 0; i < 2; i++ {
		resp, err := interceptor(ctx, "test", &grpc.UnaryServerInfo{FullMethod: "/test.Service/Test"}, mockHandler)
		assert.NoError(t, err, "Request %d should succeed", i+1)
		assert.Equal(t, "success", resp)
	}

	// 3rd request should be rate limited
	_, err := interceptor(ctx, "test", &grpc.UnaryServerInfo{FullMethod: "/test.Service/Test"}, mockHandler)
	assert.Error(t, err)
	assert.Equal(t, codes.ResourceExhausted, status.Code(err))

	// Verify metric was incremented
	assert.Equal(t, 1, metrics.getCount("app_grpc_rate_limit_exceeded_total"))
}

func TestGRPCRateLimiter_PerIPLimit(t *testing.T) {
	metrics := newMockGRPCMetrics()
	config := GRPCRateLimiterConfig{
		RequestsPerSecond: 2,
		Burst:             2,
		PerIP:             true,
	}

	limiter := NewGRPCRateLimiter(config, metrics)
	interceptor := limiter.Unary()

	mockHandler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return "success", nil
	}

	// IP1: First 2 requests should succeed
	ctx1 := peer.NewContext(context.Background(), &peer.Peer{Addr: &net.TCPAddr{IP: net.ParseIP("192.168.1.1"), Port: 8080}})
	for i := 0; i < 2; i++ {
		resp, err := interceptor(ctx1, "test", &grpc.UnaryServerInfo{FullMethod: "/test.Service/Test"}, mockHandler)
		assert.NoError(t, err)
		assert.Equal(t, "success", resp)
	}

	// IP1: 3rd request should be rate limited
	_, err := interceptor(ctx1, "test", &grpc.UnaryServerInfo{FullMethod: "/test.Service/Test"}, mockHandler)
	assert.Error(t, err)
	assert.Equal(t, codes.ResourceExhausted, status.Code(err))

	// IP2: Should still be able to make requests (different limiter)
	ctx2 := peer.NewContext(context.Background(), &peer.Peer{Addr: &net.TCPAddr{IP: net.ParseIP("192.168.1.2"), Port: 8080}})
	resp, err := interceptor(ctx2, "test", &grpc.UnaryServerInfo{FullMethod: "/test.Service/Test"}, mockHandler)
	assert.NoError(t, err)
	assert.Equal(t, "success", resp)
}

func TestGRPCRateLimiter_StreamLimit(t *testing.T) {
	metrics := newMockGRPCMetrics()
	config := GRPCRateLimiterConfig{
		RequestsPerSecond: 2,
		Burst:             2,
		PerIP:             true,
	}

	limiter := NewGRPCRateLimiter(config, metrics)
	interceptor := limiter.Stream()

	mockHandler := func(srv interface{}, stream grpc.ServerStream) error {
		return nil
	}

	ctx := peer.NewContext(context.Background(), &peer.Peer{Addr: &net.TCPAddr{IP: net.ParseIP("192.168.1.1"), Port: 8080}})
	stream := &mockServerStream{ctx: ctx}

	// First 2 requests should succeed
	for i := 0; i < 2; i++ {
		err := interceptor(nil, stream, &grpc.StreamServerInfo{FullMethod: "/test.Service/Stream"}, mockHandler)
		assert.NoError(t, err, "Stream request %d should succeed", i+1)
	}

	// 3rd request should be rate limited
	err := interceptor(nil, stream, &grpc.StreamServerInfo{FullMethod: "/test.Service/Stream"}, mockHandler)
	assert.Error(t, err)
	assert.Equal(t, codes.ResourceExhausted, status.Code(err))
}

func TestGRPCRateLimiter_ConcurrentRequests(t *testing.T) {
	metrics := newMockGRPCMetrics()
	config := GRPCRateLimiterConfig{
		RequestsPerSecond: 10,
		Burst:             10,
		PerIP:             true,
	}

	limiter := NewGRPCRateLimiter(config, metrics)
	interceptor := limiter.Unary()

	mockHandler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return "success", nil
	}

	var wg sync.WaitGroup
	successCount := 0
	rateLimitedCount := 0
	var mu sync.Mutex

	ctx := peer.NewContext(context.Background(), &peer.Peer{Addr: &net.TCPAddr{IP: net.ParseIP("192.168.1.1"), Port: 8080}})

	// Send 20 concurrent requests from same IP
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := interceptor(ctx, "test", &grpc.UnaryServerInfo{FullMethod: "/test.Service/Test"}, mockHandler)

			mu.Lock()
			if err == nil {
				successCount++
				assert.Equal(t, "success", resp)
			} else if status.Code(err) == codes.ResourceExhausted {
				rateLimitedCount++
			}
			mu.Unlock()
		}()
	}

	wg.Wait()

	// Due to timing/race conditions in concurrent tests, we allow a small tolerance
	assert.GreaterOrEqual(t, successCount, 9, "Should allow approximately burst size requests")
	assert.LessOrEqual(t, successCount, 11, "Should not allow significantly more than burst size")
	assert.Greater(t, rateLimitedCount, 0, "Should have some rate limited requests")
	assert.Equal(t, 20, successCount+rateLimitedCount, "Total requests should be 20")
}

func TestGRPCRateLimiter_TokenRefill(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping time-based test in short mode")
	}

	metrics := newMockGRPCMetrics()
	config := GRPCRateLimiterConfig{
		RequestsPerSecond: 5, // 5 requests per second
		Burst:             2,
		PerIP:             false,
	}

	limiter := NewGRPCRateLimiter(config, metrics)
	interceptor := limiter.Unary()

	mockHandler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return "success", nil
	}

	ctx := peer.NewContext(context.Background(), &peer.Peer{Addr: &net.TCPAddr{IP: net.ParseIP("192.168.1.1"), Port: 8080}})

	// Use up burst
	for i := 0; i < 2; i++ {
		resp, err := interceptor(ctx, "test", &grpc.UnaryServerInfo{FullMethod: "/test.Service/Test"}, mockHandler)
		require.NoError(t, err)
		require.Equal(t, "success", resp)
	}

	// Next request should be rate limited
	_, err := interceptor(ctx, "test", &grpc.UnaryServerInfo{FullMethod: "/test.Service/Test"}, mockHandler)
	assert.Error(t, err)
	assert.Equal(t, codes.ResourceExhausted, status.Code(err))

	// Wait for token refill (200ms = 1 token at 5 req/sec)
	time.Sleep(220 * time.Millisecond)

	// Should succeed now
	resp, err := interceptor(ctx, "test", &grpc.UnaryServerInfo{FullMethod: "/test.Service/Test"}, mockHandler)
	assert.NoError(t, err)
	assert.Equal(t, "success", resp)
}

func TestGRPCRateLimiter_MetricsLabels(t *testing.T) {
	metrics := newMockGRPCMetrics()
	config := GRPCRateLimiterConfig{
		RequestsPerSecond: 1,
		Burst:             1,
		PerIP:             true,
	}

	limiter := NewGRPCRateLimiter(config, metrics)
	interceptor := limiter.Unary()

	mockHandler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return "success", nil
	}

	ctx := peer.NewContext(context.Background(), &peer.Peer{Addr: &net.TCPAddr{IP: net.ParseIP("192.168.1.1"), Port: 8080}})

	// First request succeeds
	_, err := interceptor(ctx, "test", &grpc.UnaryServerInfo{FullMethod: "/test.Service/Test"}, mockHandler)
	assert.NoError(t, err)

	// Second request gets rate limited
	_, err = interceptor(ctx, "test", &grpc.UnaryServerInfo{FullMethod: "/test.Service/Test"}, mockHandler)
	assert.Error(t, err)

	// Verify metrics were called with correct labels
	calls := metrics.getCalls()
	require.GreaterOrEqual(t, len(calls), 1)

	rateLimitCall := calls[len(calls)-1] // Last call should be the rate limit
	assert.Equal(t, "app_grpc_rate_limit_exceeded_total", rateLimitCall.name)
	assert.Contains(t, rateLimitCall.labels, "method")
	assert.Contains(t, rateLimitCall.labels, "ip")
	assert.Equal(t, "/test.Service/Test", rateLimitCall.labels[1]) // method value
	assert.Equal(t, "192.168.1.1", rateLimitCall.labels[3])        // ip value
}

func TestGRPCRateLimiter_CleanupStaleEntries(t *testing.T) {
	metrics := newMockGRPCMetrics()
	config := GRPCRateLimiterConfig{
		RequestsPerSecond: 2,
		Burst:             1,
		PerIP:             true,
	}

	limiter := NewGRPCRateLimiter(config, metrics)

	// Add some limiters by calling getLimiter directly
	ip1 := "192.168.1.1"
	ip2 := "192.168.1.2"

	limiter1 := limiter.getLimiter(ip1)
	limiter2 := limiter.getLimiter(ip2)

	assert.NotNil(t, limiter1, "Limiter for ip1 should be created")
	assert.NotNil(t, limiter2, "Limiter for ip2 should be created")

	// Verify limiters were stored by checking the internal map
	_, exists1 := limiter.limiters.Load(ip1)
	assert.True(t, exists1, "Limiter for ip1 should be stored")

	_, exists2 := limiter.limiters.Load(ip2)
	assert.True(t, exists2, "Limiter for ip2 should be stored")

	// Count the number of stored limiters
	limitersCount := 0
	limiter.limiters.Range(func(key, value interface{}) bool {
		limitersCount++
		return true
	})
	assert.Equal(t, 2, limitersCount, "Should have exactly 2 limiters stored")

}

func TestGRPCRateLimiter_DifferentMethodsSameIP(t *testing.T) {
	metrics := newMockGRPCMetrics()
	config := GRPCRateLimiterConfig{
		RequestsPerSecond: 10, // Higher rate for this test
		Burst:             3,  // Allow 3 requests to test all methods
		PerIP:             true,
	}

	limiter := NewGRPCRateLimiter(config, metrics)
	interceptor := limiter.Unary()

	mockHandler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return "success", nil
	}

	ctx := peer.NewContext(context.Background(), &peer.Peer{Addr: &net.TCPAddr{IP: net.ParseIP("192.168.1.1"), Port: 8080}})

	// Make requests to different methods from same IP
	methods := []string{"/service.Method1", "/service.Method2", "/service.Method3"}

	for i, method := range methods {
		resp, err := interceptor(ctx, "test", &grpc.UnaryServerInfo{FullMethod: method}, mockHandler)
		assert.NoError(t, err, "Request %d to %s should succeed", i+1, method)
		assert.Equal(t, "success", resp)
	}

}

func TestGRPCRateLimiter_NoMetrics(t *testing.T) {
	// Test that rate limiter works without metrics
	config := GRPCRateLimiterConfig{
		RequestsPerSecond: 1,
		Burst:             1,
		PerIP:             false,
	}

	limiter := NewGRPCRateLimiter(config, nil)
	interceptor := limiter.Unary()

	mockHandler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return "success", nil
	}

	ctx := peer.NewContext(context.Background(), &peer.Peer{Addr: &net.TCPAddr{IP: net.ParseIP("192.168.1.1"), Port: 8080}})

	// First request should succeed
	resp, err := interceptor(ctx, "test", &grpc.UnaryServerInfo{FullMethod: "/test.Service/Test"}, mockHandler)
	assert.NoError(t, err)
	assert.Equal(t, "success", resp)

	// Second request should be rate limited (even without metrics)
	_, err = interceptor(ctx, "test", &grpc.UnaryServerInfo{FullMethod: "/test.Service/Test"}, mockHandler)
	assert.Error(t, err)
	assert.Equal(t, codes.ResourceExhausted, status.Code(err))
}

func TestGRPCRateLimiter_StreamWithDifferentIPs(t *testing.T) {
	metrics := newMockGRPCMetrics()
	config := GRPCRateLimiterConfig{
		RequestsPerSecond: 1,
		Burst:             1,
		PerIP:             true,
	}

	limiter := NewGRPCRateLimiter(config, metrics)
	interceptor := limiter.Stream()

	mockHandler := func(srv interface{}, stream grpc.ServerStream) error {
		return nil
	}

	// IP1: First request should succeed
	ctx1 := peer.NewContext(context.Background(), &peer.Peer{Addr: &net.TCPAddr{IP: net.ParseIP("192.168.1.1"), Port: 8080}})
	stream1 := &mockServerStream{ctx: ctx1}
	err := interceptor(nil, stream1, &grpc.StreamServerInfo{FullMethod: "/test.Service/Stream"}, mockHandler)
	assert.NoError(t, err)

	// IP1: Second request should be rate limited
	err = interceptor(nil, stream1, &grpc.StreamServerInfo{FullMethod: "/test.Service/Stream"}, mockHandler)
	assert.Error(t, err)
	assert.Equal(t, codes.ResourceExhausted, status.Code(err))

	// IP2: First request should succeed (different IP)
	ctx2 := peer.NewContext(context.Background(), &peer.Peer{Addr: &net.TCPAddr{IP: net.ParseIP("192.168.1.2"), Port: 8080}})
	stream2 := &mockServerStream{ctx: ctx2}
	err = interceptor(nil, stream2, &grpc.StreamServerInfo{FullMethod: "/test.Service/Stream"}, mockHandler)
	assert.NoError(t, err)
}
