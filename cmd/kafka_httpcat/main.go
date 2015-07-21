package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"

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
	OffsetCommitThreshold int64

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

	conf = &Config{StartOffset: "newest", BufferSize: 16, ConsumerID: cid, OffsetCommitThreshold: 1e3}

	md, err := toml.DecodeReader(f, conf)
	if err != nil {
		log.Fatal("Unable to parse config file: %s", err)
	}
	if u := md.Undecoded(); len(u) > 0 {
		log.Fatal("Extra keys in config file: %v", u)
	}

	log.Printf("ConsumerID: %s", conf.ConsumerID)

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

	saramaConfig := kafka_httpcat.GetDefaultSaramaConfig()

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
	)

	om := kafka_httpcat.NewOffsetManager(conf.BrokerList, partitionList, conf.Topic, conf.ConsumerGroup, conf.ConsumerID, initialOffset, conf.OffsetCommitThreshold)

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Kill, os.Interrupt)
		<-signals
		log.Println("Initiating shutdown of consumer...")
		close(closing)
	}()

	for _, partition := range partitionList {
		offset := om.GetCurrentOffset(partition)
		log.Printf("Starting consumer on topic %s partition %d offset %d", conf.Topic, partition, offset)
		pc, err := c.ConsumePartition(conf.Topic, partition, offset)
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
		for msg := range messages {
			//fmt.Printf("Key:\t%s\n", string(msg.Key))
			sender := kafka_httpcat.NewHTTPSender(conf.Hosts, conf.ContextPath, conf.Method, conf.Headers, conf.ExpectedResponses)
			for {
				if err := sender.RRSend(msg.Value); err != nil {
					log.Printf("Error send data: %s", err)
				} else {
					break
				}
			}

			om.Add(msg.Partition, msg.Offset)
		}
	}()

	wg.Wait()
	log.Println("Done consuming topic", conf.Topic)
	close(messages)

	if err := c.Close(); err != nil {
		log.Println("Failed to close consumer: ", err)
	}

	om.CommitAll()
}
