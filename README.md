# BLkafka

BLkafka - это Go-модуль для работы с Apache Kafka в проекте Blacklist PVZ. Модуль предоставляет простой и удобный интерфейс для отправки и получения сообщений из Kafka.

## Особенности

- Поддержка producer и consumer паттернов
- Конфигурируемые параметры подключения
- Поддержка нескольких брокеров
- Автоматическое создание и управление соединениями
- Тестирование с помощью встроенных тестов

## Установка

```bash
go get github.com/BLAgency/BLkafka
```

## Быстрый старт

### Инициализация

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/BLAgency/BLkafka/kafka"
)

func main() {
    // Создаем конфигурацию
    config := &kafka.Config{
        Brokers:      []string{"kafka:9092"},
        Topic:        "my-topic",
        GroupID:      "my-group",
        BatchSize:    100,
        BatchTimeout: 1 * time.Second,
    }

    // Инициализируем клиент
    client := kafka.InitKafka(config)
    defer kafka.Close()

    ctx := context.Background()

    // Отправляем сообщение
    err := client.Produce(ctx, "key1", "Hello, Kafka!")
    if err != nil {
        log.Fatal(err)
    }

    // Читаем сообщение
    message, err := client.Consume(ctx)
    if err != nil {
        log.Fatal(err)
    }

    log.Printf("Received: key=%s, value=%s", string(message.Key), string(message.Value))
}
```

## Конфигурация

### Config

```go
type Config struct {
    Brokers       []string      // Список адресов брокеров Kafka
    Topic         string        // Топик по умолчанию
    GroupID       string        // ID группы для consumer
    BatchSize     int           // Размер батча для producer
    BatchTimeout  time.Duration // Таймаут батча
    WriteTimeout  time.Duration // Таймаут записи
    ReadTimeout   time.Duration // Таймаут чтения
    RequiredAcks  int           // Количество подтверждений (0, 1, -1)
    MaxAttempts   int           // Максимальное количество попыток
}
```

### DefaultConfig

Для быстрого старта используйте `DefaultConfig()`:

```go
config := kafka.DefaultConfig()
config.Brokers = []string{"kafka:9092"}
config.Topic = "my-topic"
```

## API

### Инициализация

- `InitKafka(config *Config) *KafkaClient` - инициализирует клиент с конфигом
- `InitKafkaWithKey(key string, config *Config) *KafkaClient` - инициализирует клиент с ключом
- `GetKafka() *KafkaClient` - получает клиент по умолчанию
- `GetKafkaByKey(key string) *KafkaClient` - получает клиент по ключу
- `Close()` - закрывает все соединения
- `CloseByKey(key string)` - закрывает соединение по ключу

### Producer

- `Produce(ctx context.Context, key, value string) error` - отправляет сообщение в топик по умолчанию
- `ProduceWithTopic(ctx context.Context, topic, key, value string) error` - отправляет сообщение в указанный топик

### Consumer

- `Consume(ctx context.Context) (kafka.Message, error)` - читает одно сообщение
- `ConsumeWithHandler(ctx context.Context, handler func(kafka.Message) error) error` - читает сообщения с обработчиком

### Управление топиками

- `CreateTopic(ctx context.Context, topic string, partitions, replicationFactor int) error` - создает топик
- `DeleteTopic(ctx context.Context, topic string) error` - удаляет топик
- `ListTopics(ctx context.Context) ([]string, error)` - возвращает список топиков

### Проверка соединения

- `Ping(ctx context.Context) error` - проверяет подключение к Kafka

## Примеры использования

### Producer

```go
client := kafka.InitKafka(config)
defer kafka.Close()

ctx := context.Background()

// Отправка простого сообщения
err := client.Produce(ctx, "user-123", `{"action": "login", "timestamp": "2023-01-01T00:00:00Z"}`)
if err != nil {
    log.Printf("Failed to produce message: %v", err)
}
```

### Consumer

```go
client := kafka.InitKafka(config)
defer kafka.Close()

ctx := context.Background()

// Чтение одного сообщения
message, err := client.Consume(ctx)
if err != nil {
    log.Printf("Failed to consume message: %v", err)
} else {
    log.Printf("Received: %s = %s", string(message.Key), string(message.Value))
}
```

### Consumer с обработчиком

```go
handler := func(message kafka.Message) error {
    log.Printf("Processing: %s = %s", string(message.Key), string(message.Value))
    // Обработка сообщения...
    return nil
}

err := client.ConsumeWithHandler(ctx, handler)
if err != nil {
    log.Printf("Consumer error: %v", err)
}
```

### Управление топиками

```go
// Создание топика
err := client.CreateTopic(ctx, "new-topic", 3, 1)
if err != nil {
    log.Printf("Failed to create topic: %v", err)
}

// Получение списка топиков
topics, err := client.ListTopics(ctx)
if err != nil {
    log.Printf("Failed to list topics: %v", err)
} else {
    log.Printf("Topics: %v", topics)
}
```

## Тестирование

Запустите тесты:

```bash
go test ./kafka
```

Для интеграционных тестов убедитесь, что Kafka запущен на `localhost:9092`.

## Зависимости

- `github.com/segmentio/kafka-go` - Kafka клиент для Go

## Лицензия

Этот модуль является частью проекта Blacklist PVZ и следует его лицензии.
