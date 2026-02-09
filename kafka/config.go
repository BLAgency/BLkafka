package kafka

import "time"

// Config содержит настройки подключения к Kafka
type Config struct {
	Brokers      []string      // Список брокеров Kafka
	Topic        string        // Топик по умолчанию
	GroupID      string        // ID группы для consumer
	BatchSize    int           // Размер батча для producer
	BatchTimeout time.Duration // Таймаут батча для producer
	WriteTimeout time.Duration // Таймаут записи для producer
	ReadTimeout  time.Duration // Таймаут чтения для consumer
	RequiredAcks int           // Количество подтверждений (0, 1, -1)
	MaxAttempts  int           // Максимальное количество попыток
}

// DefaultConfig возвращает конфигурацию по умолчанию
func DefaultConfig() *Config {
	return &Config{
		Brokers:      []string{"localhost:9092"},
		Topic:        "default-topic",
		GroupID:      "default-group",
		BatchSize:    100,
		BatchTimeout: 1 * time.Second,
		WriteTimeout: 10 * time.Second,
		ReadTimeout:  10 * time.Second,
		RequiredAcks: 1,
		MaxAttempts:  3,
	}
}
