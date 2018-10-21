package mykafka

import (
	"github.com/Shopify/sarama"
)

func CreateNewConsumer(addrs []string) (sarama.Consumer, error) {
	conf := sarama.NewConfig()
	return sarama.NewConsumer(addrs, conf)
}

func GetPartitionList(consumer sarama.Consumer, topic string) ([]uint32, error) {
	partList, err := consumer.Partitions(topic)

}
