package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type eventPayload struct {
	Type      string                 `json:"type"`
	CreatedAt  string                 `json:"created_at"`
	Payload    map[string]interface{} `json:"payload"`
}

type apiResponse struct {
	Status  string `json:"status"`
	Topic   string `json:"topic"`
	Message string `json:"message"`
}

var writer *kafka.Writer
var brokers []string

func main() {
	port := env("PORT", "8082")
	brokers = splitAndTrim(env("KAFKA_BROKERS", "localhost:9092"))
	if len(brokers) == 0 {
		brokers = []string{"localhost:9092"}
	}

	writer = &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		Async:        false,
	}
	defer writer.Close()

	go consume("movie-events")
	go consume("user-events")
	go consume("payment-events")

	http.HandleFunc("/api/events/health", healthHandler)
	http.HandleFunc("/api/events/movie", makeHandler("movie-events", "movie"))
	http.HandleFunc("/api/events/user", makeHandler("user-events", "user"))
	http.HandleFunc("/api/events/payment", makeHandler("payment-events", "payment"))

	log.Printf("Events service listening on %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func makeHandler(topic, eventType string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		var payload map[string]interface{}
		if len(body) > 0 {
			if err := json.Unmarshal(body, &payload); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		} else {
			payload = map[string]interface{}{}
		}

		msg := eventPayload{
			Type:     eventType,
			CreatedAt: time.Now().UTC().Format(time.RFC3339),
			Payload:   payload,
		}

		data, err := json.Marshal(msg)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		err = writer.WriteMessages(r.Context(), kafka.Message{
			Topic: topic,
			Key:   []byte(eventType),
			Value: data,
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(apiResponse{
			Status:  "success",
			Topic:   topic,
			Message: fmt.Sprintf("%s event created", strings.ToUpper(eventType[:1])+eventType[1:]),
		})
	}
}

func consume(topic string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  "cinemaabyss-events-service",
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("kafka consumer error for topic %s: %v", topic, err)
			time.Sleep(time.Second)
			continue
		}
		log.Printf("consumed topic=%s key=%s value=%s", topic, string(msg.Key), string(msg.Value))
	}
}

func env(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func splitAndTrim(value string) []string {
	parts := strings.Split(value, ",")
	var result []string
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}
