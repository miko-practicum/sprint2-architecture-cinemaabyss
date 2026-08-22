package main

import (
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type proxyConfig struct {
	port                 string
	monolithURL          string
	moviesServiceURL     string
	eventsServiceURL     string
	gradualMigration     bool
	moviesMigrationPcent int
}

func main() {
	cfg := loadConfig()
	rand.Seed(time.Now().UnixNano())

	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/api/movies", cfg.moviesHandler)
	http.HandleFunc("/api/movies/health", proxyTo(cfg.moviesServiceURL))
	http.HandleFunc("/api/users", proxyTo(cfg.monolithURL))
	http.HandleFunc("/api/payments", proxyTo(cfg.monolithURL))
	http.HandleFunc("/api/subscriptions", proxyTo(cfg.monolithURL))
	http.HandleFunc("/api/events/", proxyTo(cfg.eventsServiceURL))

	log.Printf("Proxy listening on %s", cfg.port)
	log.Fatal(http.ListenAndServe(":"+cfg.port, nil))
}

func loadConfig() proxyConfig {
	return proxyConfig{
		port:                 env("PORT", "8000"),
		monolithURL:          env("MONOLITH_URL", "http://localhost:8080"),
		moviesServiceURL:     env("MOVIES_SERVICE_URL", "http://localhost:8081"),
		eventsServiceURL:     env("EVENTS_SERVICE_URL", "http://localhost:8082"),
		gradualMigration:     env("GRADUAL_MIGRATION", "true") == "true",
		moviesMigrationPcent: mustAtoi(env("MOVIES_MIGRATION_PERCENT", "50")),
	}
}

func (c proxyConfig) moviesHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	target := c.monolithURL
	if c.gradualMigration && shouldRouteToMoviesService(c.moviesMigrationPcent) {
		target = c.moviesServiceURL
	}
	log.Printf("proxy /api/movies -> %s", target)
	proxyRequest(target, w, r)
}

func shouldRouteToMoviesService(percent int) bool {
	if percent <= 0 {
		return false
	}
	if percent >= 100 {
		return true
	}
	return rand.Intn(100) < percent
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = io.WriteString(w, "Strangler Fig Proxy is healthy")
}

func proxyTo(target string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		proxyRequest(target, w, r)
	}
}

func proxyRequest(target string, w http.ResponseWriter, r *http.Request) {
	targetURL, err := url.Parse(target)
	if err != nil {
		http.Error(w, "invalid target url", http.StatusInternalServerError)
		return
	}
	proxy := httputil.NewSingleHostReverseProxy(targetURL)
	proxy.ErrorHandler = func(rw http.ResponseWriter, req *http.Request, err error) {
		http.Error(rw, fmt.Sprintf("proxy error: %v", err), http.StatusBadGateway)
	}
	proxy.ServeHTTP(w, r)
}

func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func mustAtoi(v string) int {
	n, err := strconv.Atoi(v)
	if err != nil {
		return 50
	}
	return n
}
