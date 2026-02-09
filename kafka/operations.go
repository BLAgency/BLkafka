package kafka

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

// Produce отправляет сообщение в топик по умолчанию
func (kc *KafkaClient) Produce(ctx context.Context, key, value string) error {
	return kc.ProduceWithTopic(ctx, kc.config.Topic, key, value)
}

// ProduceWithTopic отправляет сообщение в указанный топик
func (kc *KafkaClient) ProduceWithTopic(ctx context.Context, topic, key, value string) error {
	message := kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: []byte(value),
	}

	err := kc.producer.WriteMessages(ctx, message)
	if err != nil {
		return fmt.Errorf("failed to produce message: %w", err)
	}

	return nil
}

// Consume читает одно сообщение из топика
func (kc *KafkaClient) Consume(ctx context.Context) (kafka.Message, error) {
	message, err := kc.consumer.ReadMessage(ctx)
	if err != nil {
		return kafka.Message{}, fmt.Errorf("failed to consume message: %w", err)
	}

	return message, nil
}

// ConsumeWithHandler читает сообщения и обрабатывает их с помощью переданного обработчика
func (kc *KafkaClient) ConsumeWithHandler(ctx context.Context, handler func(kafka.Message) error) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			message, err := kc.consumer.ReadMessage(ctx)
			if err != nil {
				return fmt.Errorf("failed to read message: %w", err)
			}

			err = handler(message)
			if err != nil {
				return fmt.Errorf("handler error: %w", err)
			}

			// Подтверждаем обработку сообщения
			kc.consumer.CommitMessages(ctx, message)
		}
	}
}

// CreateTopic создает новый топик
func (kc *KafkaClient) CreateTopic(ctx context.Context, topic string, partitions, replicationFactor int) error {
	conn, err := kafka.Dial("tcp", kc.config.Brokers[0])
	if err != nil {
		return fmt.Errorf("failed to connect to kafka: %w", err)
	}
	defer conn.Close()

	err = conn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: replicationFactor,
	})
	if err != nil {
		return fmt.Errorf("failed to create topic: %w", err)
	}

	return nil
}

// DeleteTopic удаляет топик
func (kc *KafkaClient) DeleteTopic(ctx context.Context, topic string) error {
	conn, err := kafka.Dial("tcp", kc.config.Brokers[0])
	if err != nil {
		return fmt.Errorf("failed to connect to kafka: %w", err)
	}
	defer conn.Close()

	err = conn.DeleteTopics(topic)
	if err != nil {
		return fmt.Errorf("failed to delete topic: %w", err)
	}

	return nil
}

// ListTopics возвращает список всех топиков
func (kc *KafkaClient) ListTopics(ctx context.Context) ([]string, error) {
	conn, err := kafka.Dial("tcp", kc.config.Brokers[0])
	if err != nil {
		return nil, fmt.Errorf("failed to connect to kafka: %w", err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		return nil, fmt.Errorf("failed to read partitions: %w", err)
	}

	topicMap := make(map[string]bool)
	for _, partition := range partitions {
		topicMap[partition.Topic] = true
	}

	var topics []string
	for topic := range topicMap {
		topics = append(topics, topic)
	}

	return topics, nil
}

// GetPartitionInfo возвращает информацию о партициях топика
func (kc *KafkaClient) GetPartitionInfo(ctx context.Context, topic string) ([]kafka.Partition, error) {
	conn, err := kafka.Dial("tcp", kc.config.Brokers[0])
	if err != nil {
		return nil, fmt.Errorf("failed to connect to kafka: %w", err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		return nil, fmt.Errorf("failed to read partitions for topic %s: %w", topic, err)
	}

	return partitions, nil
}

// SeekToBeginning перемещает offset consumer'а в начало топика
func (kc *KafkaClient) SeekToBeginning(ctx context.Context, topic string) error {
	return kc.consumer.SetOffset(kafka.FirstOffset)
}

// SeekToEnd перемещает offset consumer'а в конец топика
func (kc *KafkaClient) SeekToEnd(ctx context.Context, topic string) error {
	return kc.consumer.SetOffset(kafka.LastOffset)
}

// SeekToOffset перемещает offset consumer'а на указанную позицию
func (kc *KafkaClient) SeekToOffset(ctx context.Context, topic string, offset int64) error {
	return kc.consumer.SetOffset(offset)
}
