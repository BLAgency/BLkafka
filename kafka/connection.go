package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"sync"

	"github.com/segmentio/kafka-go"
)

var (
	instances map[string]*KafkaClient
	mu        sync.RWMutex
	once      sync.Once
)

type KafkaClient struct {
	producer *kafka.Writer
	consumer *kafka.Reader
	config   *Config
}

func InitKafka(connString string) *KafkaClient {
	config := DefaultConfig()
	config.UseSSL = &[]bool{true}[0] // Включаем SSL
	return InitKafkaWithKey("default", []string{connString}, config)
}

func InitKafkaWithKey(key string, brokers []string, config *Config) *KafkaClient {
	once.Do(func() {
		instances = make(map[string]*KafkaClient)
	})

	mu.Lock()
	defer mu.Unlock()

	if _, exists := instances[key]; exists {
		fmt.Printf("Kafka клиент '%s' уже инициализирован\n", key)
		return instances[key]
	}

	client := &KafkaClient{
		config: config,
	}

	// Настройка TLS если включен SSL
	var tlsConfig *tls.Config
	if config.UseSSL != nil && *config.UseSSL {
		var err error
		tlsConfig, err = createTLSConfig(config)
		if err != nil {
			panic(fmt.Sprintf("Ошибка создания TLS конфигурации для Kafka клиента '%s': %v", key, err))
		}
	}

	// Настройка producer
	writerConfig := kafka.WriterConfig{
		Brokers:      brokers,
		Topic:        config.Topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: config.RequiredAcks,
		Async:        false,
		WriteTimeout: config.WriteTimeout,
		MaxAttempts:  config.MaxAttempts,
	}

	if tlsConfig != nil {
		writerConfig.Dialer = &kafka.Dialer{
			TLS: tlsConfig,
		}
	}

	client.producer = kafka.NewWriter(writerConfig)

	// Настройка consumer
	readerConfig := kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       config.Topic,
		GroupID:     config.GroupID,
		StartOffset: kafka.LastOffset,
		MaxAttempts: config.MaxAttempts,
	}

	if tlsConfig != nil {
		readerConfig.Dialer = &kafka.Dialer{
			TLS: tlsConfig,
		}
	}

	client.consumer = kafka.NewReader(readerConfig)

	instances[key] = client
	sslEnabled := config.UseSSL != nil && *config.UseSSL
	fmt.Printf("Kafka клиент '%s' инициализирован (SSL: %t)\n", key, sslEnabled)
	return client
}

func GetKafka() *KafkaClient {
	return GetKafkaByKey("default")
}

func GetKafkaByKey(key string) *KafkaClient {
	mu.RLock()
	defer mu.RUnlock()

	if client, exists := instances[key]; exists {
		return client
	}
	panic(fmt.Sprintf("Kafka клиент '%s' не инициализирован. Вызовите InitKafkaWithKey() сначала.", key))
}

func CloseKafka() {
	mu.Lock()
	defer mu.Unlock()

	for key, client := range instances {
		if client.producer != nil {
			client.producer.Close()
		}
		if client.consumer != nil {
			client.consumer.Close()
		}
		fmt.Printf("Kafka клиент '%s' закрыт\n", key)
	}
	instances = nil
}

func CloseKafkaByKey(key string) {
	mu.Lock()
	defer mu.Unlock()

	if client, exists := instances[key]; exists {
		if client.producer != nil {
			client.producer.Close()
		}
		if client.consumer != nil {
			client.consumer.Close()
		}
		fmt.Printf("Kafka клиент '%s' закрыт\n", key)
		delete(instances, key)
	}
}

// createTLSConfig создает TLS конфигурацию для Kafka
func createTLSConfig(config *Config) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true, // Для самоподписанных сертификатов
	}

	// Если передана кастомная конфигурация, используем её
	if config.TLSConfig != nil {
		return config.TLSConfig, nil
	}

	// Загружаем сертификаты из файлов
	if config.TLSCertFile != "" && config.TLSKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(config.TLSCertFile, config.TLSKeyFile)
		if err != nil {
			return nil, fmt.Errorf("ошибка загрузки клиентского сертификата: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	// Настраиваем CA если указан
	if config.TLSCAFile != "" {
		caCert, err := ioutil.ReadFile(config.TLSCAFile)
		if err != nil {
			return nil, fmt.Errorf("ошибка чтения CA сертификата: %w", err)
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		tlsConfig.RootCAs = caCertPool
	}

	return tlsConfig, nil
}
