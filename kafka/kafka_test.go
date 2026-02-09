package kafka

import (
	"context"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()
	if len(config.Brokers) == 0 {
		t.Error("Default config should have brokers")
	}
	if config.Topic == "" {
		t.Error("Default config should have topic")
	}
}

func TestInitKafka(t *testing.T) {
	config := DefaultConfig()
	brokers := []string{"localhost:9092"} // Предполагаем, что Kafka запущен

	client := InitKafkaWithKey("default", brokers, config)
	if client == nil {
		t.Error("InitKafkaWithKey should return a client")
	}

	// Закрываем
	CloseKafka()
}

func TestProduceAndConsume(t *testing.T) {
	config := DefaultConfig()
	brokers := []string{"localhost:9092"}
	config.Topic = "test-topic"

	client := InitKafkaWithKey("test", brokers, config)
	defer CloseKafkaByKey("test")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Produce
	err := client.Produce(ctx, "test-key", "test-value")
	if err != nil {
		t.Logf("Produce failed: %v", err)
		return // Пропускаем тест, если не можем отправить
	}

	// Consume
	message, err := client.Consume(ctx)
	if err != nil {
		t.Logf("Consume failed: %v", err)
		return
	}

	if string(message.Key) != "test-key" || string(message.Value) != "test-value" {
		t.Errorf("Expected key='test-key', value='test-value', got key='%s', value='%s'", string(message.Key), string(message.Value))
	}
}
