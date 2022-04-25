package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Attempts int = iota
	Retry
)

type Backend struct {
	URL          *url.URL
	Alive        bool
	mux          sync.RWMutex
	ReverseProxy *httputil.ReverseProxy
}

func (b *Backend) SetAlive(alive bool) {
	b.mux.Lock()
	b.Alive = alive
	b.mux.Unlock()
}

func (b *Backend) IsAlive() (alive bool) {
	b.mux.RLock()
	alive = b.Alive
	b.mux.RUnlock()
	return
}

type ServerPool struct {
	backends []*Backend
	current  uint64
}

func (s *ServerPool) NextIndex() int {
	return int(atomic.AddUint64(&s.current, uint64(1)) % uint64(len(s.backends)))
}

func (s *ServerPool) GetNextPeer() *Backend {
	next := s.NextIndex()
	l := len(s.backends) + next

	for i := next; i < l; i++ {
		index := i % len(s.backends)

		if s.backends[index].IsAlive() {
			if i != next {
				atomic.StoreUint64(&s.current, uint64(index))
			}

			return s.backends[index]
		}
	}

	return nil
}

func (s *ServerPool) HealthCheck() {
	for _, backend := range s.backends {
		status := "up"
		isAlive := isBackendAlive(backend.URL)

		backend.SetAlive(isAlive)

		if !isAlive {
			status = "down"
		}

		log.Printf("%s [%s]\n", backend.URL, status)
	}
}

func (s *ServerPool) MarkBackendStatus(backendUrl *url.URL, alive bool) {
	for _, backend := range s.backends {
		if backend.URL.String() == backendUrl.String() {
			backend.SetAlive(alive)
			break
		}
	}
}

func isBackendAlive(u *url.URL) bool {
	timeout := 2 * time.Second

	conn, err := net.DialTimeout("tcp", u.Host, timeout)

	if err != nil {
		log.Printf("Site unreachable, error: ", err)
		return false
	}

	_ = conn.Close()
	return true
}

func GetAttemptsFromContext(r *http.Request) int {
	if attempts, ok := r.Context().Value(Attempts).(int); ok {
		return attempts
	}

	return 1
}

func GetRetryFromContext(r *http.Request) int {
	if retry, ok := r.Context().Value(Retry).(int); ok {
		return retry
	}

	return 0
}

func loadBalance(w http.ResponseWriter, r *http.Request) {
	attempts := GetAttemptsFromContext(r)

	if attempts > 3 {
		log.Printf("%s(%s) Max attempts reached, terminating\n", r.RemoteAddr, r.URL.Path)

		http.Error(w, "Service not available", http.StatusServiceUnavailable)

		return
	}

	peer := serverPool.GetNextPeer()

	if peer != nil {
		peer.ReverseProxy.ServeHTTP(w, r)
		return
	}

	http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
}

func healthCheck() {
	t := time.NewTicker(time.Second * 20)

	for {
		select {
		case <-t.C:
			log.Printf("Starting health check\n")
			serverPool.HealthCheck()
			log.Printf("Health check completed\n")
		}
	}
}

var serverPool ServerPool

func main() {
	var port int

	serverUrl, _ := url.Parse("http://localhost:8181")
	proxy := httputil.NewSingleHostReverseProxy(serverUrl)

	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("[%s] %s\n", serverUrl.Host, err.Error)

		retries := GetRetryFromContext(r)

		if retries < 3 {
			select {
			case <-time.After(time.Millisecond * 10):
				ctx := context.WithValue(r.Context(), Retry, retries+1)
				proxy.ServeHTTP(w, r.WithContext(ctx))
			}
			return
		}

		serverPool.MarkBackendStatus(serverUrl, false)

		attempts := GetAttemptsFromContext(r)

		log.Printf("%s(%s) Attempting retry %d\n", r.RemoteAddr, r.URL.Path, attempts)

		ctx := context.WithValue(r.Context(), Attempts, attempts+1)

		loadBalance(w, r.WithContext(ctx))
	}

	server := http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: http.HandlerFunc(loadBalance),
	}

	go healthCheck()

	log.Printf("Load Balancer started at :%d\n", port)
	if err := server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
