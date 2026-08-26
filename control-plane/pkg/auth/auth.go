package auth

import (
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

var (
	validAPIKeys   map[string]bool
	authEnabled    bool
	corsOrigins    = map[string]bool{
		"http://localhost:3000":  true,
		"http://localhost:5173":  true,
		"http://localhost:8080":  true,
		"https://localhost:3000": true,
		"https://localhost:5173": true,
		"https://localhost:8080": true,
	}
	rateLimitMu    sync.Mutex
	rateLimitBuckets = make(map[string]*rateBucket)
	defaultRate    = 100.0
)

type rateBucket struct {
	tokens   float64
	lastTime time.Time
}

func init() {
	keysEnv := os.Getenv("OXIDESTREAM_API_KEYS")
	validAPIKeys = make(map[string]bool)
	if keysEnv == "" {
		authEnabled = false
		return
	}
	authEnabled = true
	for _, k := range strings.Split(keysEnv, ",") {
		k = strings.TrimSpace(k)
		if k != "" {
			validAPIKeys[k] = true
		}
	}
	if len(validAPIKeys) == 0 {
		authEnabled = false
	}
}

func CORSMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if corsOrigins[origin] {
			w.Header().Set("Access-Control-Allow-Origin", origin)
		}
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key")
		w.Header().Set("Access-Control-Max-Age", "86400")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func APIKeyMiddleware(next http.Handler) http.Handler {
	if !authEnabled {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiKey := r.Header.Get("X-API-Key")
		if apiKey == "" {
			http.Error(w, "Missing X-API-Key header", http.StatusUnauthorized)
			return
		}
		if !validAPIKeys[apiKey] {
			http.Error(w, "Invalid API key", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func RateLimitMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := r.RemoteAddr
		if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
			ip = strings.Split(fwd, ",")[0]
		}
		if !allowRequest(ip) {
			http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func allowRequest(ip string) bool {
	rateLimitMu.Lock()
	defer rateLimitMu.Unlock()

	now := time.Now()
	bucket, exists := rateLimitBuckets[ip]
	if !exists {
		rateLimitBuckets[ip] = &rateBucket{tokens: defaultRate - 1, lastTime: now}
		return true
	}

	elapsed := now.Sub(bucket.lastTime).Seconds()
	bucket.tokens += elapsed * defaultRate
	if bucket.tokens > defaultRate {
		bucket.tokens = defaultRate
	}
	bucket.lastTime = now

	if bucket.tokens < 1 {
		return false
	}
	bucket.tokens--
	return true
}

func RequestLoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		wrapped := &statusRecorder{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(wrapped, r)
		log.Printf("%s %s %d %s", r.Method, r.URL.Path, wrapped.statusCode, time.Since(start))
	})
}

type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (rec *statusRecorder) WriteHeader(code int) {
	rec.statusCode = code
	rec.ResponseWriter.WriteHeader(code)
}
