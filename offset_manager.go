package kafka_httpcat

import (
	"fmt"
	"log"
	"time"

	"github.com/Shopify/sarama"
	"github.com/mathpl/go-tsdmetrics"
	"github.com/rcrowley/go-metrics"
)

type OffsetManager struct {
	client             sarama.Client
	broker             *sarama.Broker
	topic              string
	consumerGroup      string
	consumerID         string
	currentOffsetMap   map[int32]int64
	committedOffsetMap map[int32]int64
	commitThreshold    int64

	metricsRegistry tsdmetrics.TaggedRegistry
}

func GetDefaultSaramaConfig() *sarama.Config {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Net.DialTimeout = 5 * time.Second
	saramaConfig.Net.ReadTimeout = 30 * time.Second
	saramaConfig.Net.WriteTimeout = 30 * time.Second
	saramaConfig.Metadata.Retry.Max = 3
	saramaConfig.Consumer.Fetch.Min = 1
	saramaConfig.Consumer.Fetch.Default = 32768
	saramaConfig.Consumer.Retry.Backoff = 2 * time.Second
	saramaConfig.Consumer.MaxWaitTime = 250 * time.Millisecond
	saramaConfig.Consumer.Return.Errors = true

	return saramaConfig
}

func NewOffsetManager(metricsRegistry tsdmetrics.TaggedRegistry, brokerList []string, partitionList []int32, topic string, consumerGroup string, consumerID string, initialOffset int64, commitThreshold int64) *OffsetManager {
	om := &OffsetManager{topic: topic, consumerGroup: consumerGroup, consumerID: consumerID, commitThreshold: commitThreshold, metricsRegistry: metricsRegistry}

	var err error
	if om.client, err = sarama.NewClient(brokerList, GetDefaultSaramaConfig()); err != nil {
		log.Fatalf("Unable to connect to broker with client: %s", err)
	}

	if om.broker, err = om.client.Coordinator(consumerGroup); err != nil {
		log.Fatalf("Unable to connect to fetch broker from coordinator: %s", err)
	}

	offsetRequest := sarama.OffsetFetchRequest{ConsumerGroup: consumerGroup, Version: 1}
	for _, partition := range partitionList {
		offsetRequest.AddPartition(topic, partition)
	}

	om.currentOffsetMap = make(map[int32]int64)
	om.committedOffsetMap = make(map[int32]int64)

	if resp, err := om.broker.FetchOffset(&offsetRequest); err != nil {
		log.Fatalf("Unable to fetch stored offset: %s", err)
	} else {
		for partition, offsetResponseBlock := range resp.Blocks[topic] {
			switch offsetResponseBlock.Err {
			case 0:
				om.currentOffsetMap[partition] = offsetResponseBlock.Offset
				om.committedOffsetMap[partition] = offsetResponseBlock.Offset
			case 1:
				//Not on server anymore, pick default
				om.currentOffsetMap[partition] = initialOffset
				om.committedOffsetMap[partition] = initialOffset
			default:
				log.Fatalf("Unexpected error fetching offsets: %d", offsetResponseBlock.Err)
			}
		}
	}

	return om
}

func (om *OffsetManager) Add(partition int32, offset int64) error {
	tags := tsdmetrics.Tags{"partition": fmt.Sprintf("%d", partition)}

	om.currentOffsetMap[partition] = offset
	if om.currentOffsetMap[partition]-om.committedOffsetMap[partition] > om.commitThreshold {
		offsetReq := &sarama.OffsetCommitRequest{ConsumerGroup: om.consumerGroup, ConsumerID: om.consumerID, Version: 1}
		offsetReq.AddBlock(om.topic, partition, offset, time.Now().Unix(), "")
		if _, err := om.broker.CommitOffset(offsetReq); err != nil {
			return fmt.Errorf("Unable to commit offset: %s", err)
		} else {
			log.Printf("Commited offset for partition: %d offset %d", partition, offset)
			om.committedOffsetMap[partition] = offset

			i := om.metricsRegistry.GetOrRegister("consumer.committed", tags, metrics.NewGauge())
			if m, ok := i.(metrics.Gauge); ok {
				m.Update(offset)
			} else {
				log.Print("Unexpected metric type")
			}
		}
	}

	i := om.metricsRegistry.GetOrRegister("consumer.sent", tags, metrics.NewGauge())
	if m, ok := i.(metrics.Gauge); ok {
		m.Update(offset)
	} else {
		log.Printf("Unexpected metric type")
	}

	return nil
}

func (om *OffsetManager) CommitAll() {
	offsetReq := &sarama.OffsetCommitRequest{ConsumerGroup: om.consumerGroup, ConsumerID: om.consumerID, Version: 1}
	for partition, offset := range om.currentOffsetMap {
		offsetReq.AddBlock(om.topic, partition, offset, time.Now().Unix(), "")
		log.Printf("Committing offset for partition: %d offset %d...", partition, offset)
	}

	if _, err := om.broker.CommitOffset(offsetReq); err != nil {
		//FIXME: disconnect on long wait
		log.Printf("Unable to commit offsets: %s", err)
	} else {
		for partition, offset := range om.currentOffsetMap {
			om.committedOffsetMap[partition] = offset
		}
		log.Print("Committed")
	}
}

func (om *OffsetManager) GetCurrentOffset(partition int32) int64 {
	if offset, ok := om.currentOffsetMap[partition]; ok {
		return offset
	} else {
		return -1
	}
}
