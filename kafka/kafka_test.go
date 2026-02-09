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
	config.Brokers = []string{"localhost:9092"} // Предполагаем, что Kafka запущен

	client := InitKafka(config)
	if client == nil {
		t.Error("InitKafka should return a client")
	}

	// Тест Ping (может не работать без запущенного Kafka)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := client.Ping(ctx)
	if err != nil {
		t.Logf("Ping failed (expected if Kafka not running): %v", err)
	}

	// Закрываем
	Close()
}

func TestProduceAndConsume(t *testing.T) {
	config := DefaultConfig()
	config.Brokers = []string{"localhost:9092"}
	config.Topic = "test-topic"

	client := InitKafka(config)
	defer Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Создаем топик для теста
	err := client.CreateTopic(ctx, "test-topic", 1, 1)
	if err != nil {
		t.Logf("Create topic failed (may already exist): %v", err)
	}

	// Produce
	err = client.Produce(ctx, "test-key", "test-value")
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

	// Удаляем топик
	err = client.DeleteTopic(ctx, "test-topic")
	if err != nil {
		t.Logf("Delete topic failed: %v", err)
	}
}
