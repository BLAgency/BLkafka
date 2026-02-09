package kafka

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/segmentio/kafka-go"
)

var (
	instances map[string]*KafkaClient
	mu        sync.RWMutex
	once      sync.Once
)

// KafkaClient представляет клиент для работы с Kafka
type KafkaClient struct {
	config   *Config
	producer *kafka.Writer
	consumer *kafka.Reader
}

// InitKafka инициализирует подключение к Kafka с переданной конфигурацией (для обратной совместимости, использует "default" как ключ)
func InitKafka(config *Config) *KafkaClient {
	return InitKafkaWithKey("default", config)
}

// InitKafkaWithKey инициализирует подключение к Kafka с ключом и конфигурацией
func InitKafkaWithKey(key string, config *Config) *KafkaClient {
	once.Do(func() {
		instances = make(map[string]*KafkaClient)
	})

	mu.Lock()
	defer mu.Unlock()

	if _, exists := instances[key]; exists {
		log.Printf("Подключение к Kafka '%s' уже инициализировано", key)
		return instances[key]
	}

	// Создаем producer
	producer := &kafka.Writer{
		Addr:         kafka.TCP(config.Brokers...),
		Topic:        config.Topic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    config.BatchSize,
		BatchTimeout: config.BatchTimeout,
		WriteTimeout: config.WriteTimeout,
		RequiredAcks: kafka.RequiredAcks(config.RequiredAcks),
		MaxAttempts:  config.MaxAttempts,
	}

	// Создаем consumer
	consumer := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     config.Brokers,
		Topic:       config.Topic,
		GroupID:     config.GroupID,
		StartOffset: kafka.LastOffset,
	})

	client := &KafkaClient{
		config:   config,
		producer: producer,
		consumer: consumer,
	}

	instances[key] = client
	fmt.Printf("Подключение к Kafka '%s' установлено\n", key)
	return client
}

// GetKafka возвращает экземпляр клиента Kafka по ключу (для обратной совместимости использует "default")
func GetKafka() *KafkaClient {
	return GetKafkaByKey("default")
}

// GetKafkaByKey возвращает экземпляр клиента Kafka по ключу
func GetKafkaByKey(key string) *KafkaClient {
	mu.RLock()
	defer mu.RUnlock()

	if client, exists := instances[key]; exists {
		return client
	}
	log.Fatalf("Kafka клиент '%s' не инициализирован. Вызовите InitKafkaWithKey() сначала.", key)
	return nil
}

// Close закрывает все подключения к Kafka
func Close() {
	mu.Lock()
	defer mu.Unlock()

	for key, client := range instances {
		if client != nil {
			client.producer.Close()
			client.consumer.Close()
			fmt.Printf("Подключение к Kafka '%s' закрыто\n", key)
		}
	}
	instances = nil
}

// CloseByKey закрывает подключение к Kafka по ключу
func CloseByKey(key string) {
	mu.Lock()
	defer mu.Unlock()

	if client, exists := instances[key]; exists {
		client.producer.Close()
		client.consumer.Close()
		fmt.Printf("Подключение к Kafka '%s' закрыто\n", key)
		delete(instances, key)
	}
}

// Ping проверяет подключение к Kafka
func (kc *KafkaClient) Ping(ctx context.Context) error {
	// Проверяем producer
	err := kc.producer.WriteMessages(ctx, kafka.Message{
		Key:   []byte("ping"),
		Value: []byte("ping"),
	})
	if err != nil {
		return fmt.Errorf("failed to ping producer: %w", err)
	}

	// Проверяем consumer (просто пытаемся прочитать, но не блокируемся)
	go func() {
		defer kc.consumer.Close()
		kc.consumer.ReadMessage(ctx)
	}()

	return nil
}
