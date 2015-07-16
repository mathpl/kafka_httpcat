package main

import (
	"bytes"
	"flag"
	"io/ioutil"
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

	Partitions []int32
	Offset     string
	BufferSize int
}

func readConf(filename string) (conf *Config) {
	f, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Unable to open config file: %s", err)
	}
	defer f.Close()

	conf = &Config{Offset: "newest", BufferSize: 64}

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
	switch conf.Offset {
	case "oldest":
		initialOffset = sarama.OffsetOldest
	case "newest":
		initialOffset = sarama.OffsetNewest
	default:
		log.Fatal("offset should be `oldest` or `newest`")
	}

	c, err := sarama.NewConsumer(conf.BrokerList, nil)
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

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Kill, os.Interrupt)
		<-signals
		log.Println("Initiating shutdown of consumer...")
		close(closing)
	}()

	for _, partition := range partitionList {
		pc, err := c.ConsumePartition(conf.Topic, partition, initialOffset)
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
			//fmt.Printf("Partition:\t%d\n", msg.Partition)
			//fmt.Printf("Offset:\t%d\n", msg.Offset)
			//fmt.Printf("Key:\t%s\n", string(msg.Key))
			//fmt.Printf("Value:\t%s\n", string(msg.Value))
			//fmt.Println()
			sender := kafka_httpcat.NewHTTPSender(conf.Hosts, conf.ContextPath, conf.Method, conf.Headers, conf.ExpectedResponses)
			for {
				msg_reader := bytes.NewReader(msg.Value)
				if err := sender.RRSend(ioutil.NopCloser(msg_reader)); err != nil {
					log.Printf("Error send data: %s", err)
				} else {
					break
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
