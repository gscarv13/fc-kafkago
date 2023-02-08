package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	deliveryChan := make(chan kafka.Event)
	producer := NewKafkaProducer()
	Publish("mensagem", "teste", producer, nil, deliveryChan)

	// =================================================================
	// ==================SYNCHRONOUS IMPLEMENTATION=====================
	// This will make the program stop and wait for the message be delivered
	// to the channel. Once that is done the program will resume its execution
	// This is a synchronous operation :]
	e := <-deliveryChan

	msg := e.(*kafka.Message)

	if msg.TopicPartition.Error != nil {
		fmt.Println("Erro ao enviar", msg.TopicPartition.Error.Error())
	} else {
		// This will print the a message on the following format:
		// topic_name[partition_number]@offset_number
		// eg: run_rule_group[3]@2
		fmt.Println("Mensagem enviada", msg.TopicPartition)
	}
	// ===================================================================

	producer.Flush(1000)
}

func NewKafkaProducer() *kafka.Producer {

	// This create a pointer to an instance of kafka.ConfigMap
	// which needs to be a pointer since the producer
	// implementation expects it to be a pointer
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "fc2-gokafka-kafka-1:9092",
	}

	p, err := kafka.NewProducer(configMap)

	if err != nil {
		log.Printf(err.Error())
	}

	return p
}

func Publish(msg string, topic string, producer *kafka.Producer, key []byte, deliveryChan chan kafka.Event) error {
	msgStruct := &kafka.Message{
		Value:          []byte(msg),
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
	}

	err := producer.Produce(msgStruct, deliveryChan)

	if err != nil {
		return err
	}

	return nil
}
