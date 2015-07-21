package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/Shopify/sarama"
	"github.com/mathpl/kafka_httpcat"
)

var flagConf = flag.String("c", "", "Location of configuration file.")

type Config struct {
	// Target urls
	Hosts []string

	//Context path
	ContextPath string

	//Headers to add to the request
	Headers map[string][]string

	//HTTP method to use
	Method string

	//Expected http response deoces
	ExpectedResponses []int

	//Broker
	BrokerList []string

	//Topic
	Topic string

	ConsumerGroup string
	ConsumerID    string

	Partitions  []int32
	StartOffset string
	BufferSize  int

	//PayloadSize sent between each Kafka commit
	OffsetCommitSize int

	//OpentsdbReport
	MetricsReport string
}

func readConf(filename string) (conf *Config) {
	f, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Unable to open config file: %s", err)
	}
	defer f.Close()

	host, err := os.Hostname()
	if err != nil {
		log.Fatalf("Unable to get hostname: %s", err)
	}

	cid := fmt.Sprintf("%s-%d", host, os.Getpid())

	conf = &Config{StartOffset: "newest", BufferSize: 16, ConsumerID: cid, OffsetCommitSize: 1e6}

	md, err := toml.DecodeReader(f, conf)
	if err != nil {
		log.Fatal("Unable to parse config file: %s", err)
	}
	if u := md.Undecoded(); len(u) > 0 {
		log.Fatal("Extra keys in config file: %v", u)
	}

	return
}

func getPartitions(conf *Config, c sarama.Consumer) ([]int32, error) {
	if len(conf.Partitions) == 0 {
		return c.Partitions(conf.Topic)
	}

	return conf.Partitions, nil
}

func main() {
	flag.Parse()
	conf := readConf(*flagConf)

	var initialOffset int64

	switch conf.StartOffset {
	case "oldest":
		initialOffset = sarama.OffsetOldest
	case "newest":
		initialOffset = sarama.OffsetNewest
	default:
		log.Fatal("offset should be `oldest` or `newest`")
	}

	log.Printf("Connecting to: %s", conf.BrokerList)

	saramaConfig := sarama.NewConfig()
	saramaConfig.Net.DialTimeout = 30 * time.Second
	saramaConfig.Net.ReadTimeout = 30 * time.Second
	saramaConfig.Net.WriteTimeout = 30 * time.Second
	saramaConfig.Metadata.Retry.Max = 3
	saramaConfig.Consumer.Fetch.Min = 1
	saramaConfig.Consumer.Fetch.Default = 32768
	saramaConfig.Consumer.Retry.Backoff = 2 * time.Second
	saramaConfig.Consumer.MaxWaitTime = 250 * time.Millisecond
	saramaConfig.Consumer.Return.Errors = true

	c, err := sarama.NewConsumer(conf.BrokerList, saramaConfig)
	if err != nil {
		log.Fatalf("Failed to start consumer: %s", err)
	}

	partitionList, err := getPartitions(conf, c)
	if err != nil {
		log.Fatalf("Failed to get the list of partitions: %s", err)
	}

	var (
		messages = make(chan *sarama.ConsumerMessage, conf.BufferSize)
		closing  = make(chan struct{})
		wg       sync.WaitGroup
		client   sarama.Client
		broker   *sarama.Broker
	)

	if client, err = sarama.NewClient(conf.BrokerList, saramaConfig); err != nil {
		log.Fatalf("Unable to connect to broker with client: %s", err)
	}

	if broker, err = client.Coordinator(conf.ConsumerGroup); err != nil {
		log.Fatalf("Unable to connect to fetch broker from coordinator: %s", err)
	}

	offsetRequest := sarama.OffsetFetchRequest{ConsumerGroup: conf.ConsumerGroup, Version: 1}
	for _, partition := range partitionList {
		offsetRequest.AddPartition(conf.Topic, partition)
	}

	offsetMap := make(map[int32]int64)

	if resp, err := broker.FetchOffset(&offsetRequest); err != nil {
		log.Fatalf("Unable to fetch stored offset: %s", err)
	} else {
		for partition, offsetResponseBlock := range resp.Blocks[conf.Topic] {
			switch offsetResponseBlock.Err {
			case 0:
				offsetMap[partition] = offsetResponseBlock.Offset
			case 1:
				//Not on server anymore, pick default
				offsetMap[partition] = initialOffset
			default:
				log.Fatalf("Unexpected error fetching offsets: %d", offsetResponseBlock.Err)
			}
		}
	}

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Kill, os.Interrupt)
		<-signals
		log.Println("Initiating shutdown of consumer...")
		close(closing)
	}()

	for _, partition := range partitionList {
		log.Printf("Starting consumer on topic %s partition %s offset %d", conf.Topic, partition, offsetMap[partition])
		pc, err := c.ConsumePartition(conf.Topic, partition, offsetMap[partition])
		if err != nil {
			log.Fatalf("Failed to start consumer for partition %d: %s", partition, err)
		}

		go func(pc sarama.PartitionConsumer) {
			<-closing
			pc.AsyncClose()
		}(pc)

		wg.Add(1)
		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()
			for message := range pc.Messages() {
				messages <- message
			}
		}(pc)
	}

	go func() {
		partitionBatchPayload := make(map[int32]int)
		for msg := range messages {
			offset := &sarama.OffsetCommitRequest{ConsumerGroup: conf.ConsumerGroup, ConsumerID: conf.ConsumerID, Version: 1}
			//fmt.Printf("Partition:\t%d\n", msg.Partition)
			//fmt.Printf("Offset:\t%d\n", msg.Offset)
			//fmt.Printf("Key:\t%s\n", string(msg.Key))
			sender := kafka_httpcat.NewHTTPSender(conf.Hosts, conf.ContextPath, conf.Method, conf.Headers, conf.ExpectedResponses)
			for {
				if err := sender.RRSend(msg.Value); err != nil {
					log.Printf("Error send data: %s", err)
				} else {
					break
				}
			}

			partitionBatchPayload[msg.Partition] += len(msg.Value)
			for batchPartition, batchPayload := range partitionBatchPayload {
				if batchPayload > conf.OffsetCommitSize {
					offset.AddBlock(conf.Topic, msg.Partition, msg.Offset, time.Now().Unix(), "")
					if _, err := broker.CommitOffset(offset); err != nil {
						log.Printf("Unable to commit offset: %s", err)
					} else {
						partitionBatchPayload[batchPartition] = 0
					}
				}
			}
		}
	}()

	wg.Wait()
	log.Println("Done consuming topic", conf.Topic)
	close(messages)

	if err := c.Close(); err != nil {
		log.Println("Failed to close consumer: ", err)
	}

}
