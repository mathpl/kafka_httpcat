package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/codegangsta/cli"
	"github.com/mathpl/go-tsdmetrics"
	"github.com/mathpl/kafka_httpcat"
	"github.com/rcrowley/go-metrics"
)

var version = "0.2.0"

func generateConsumerLag(r tsdmetrics.TaggedRegistry) {
	valsSent := make(map[string]int64, 0)
	valsCommitted := make(map[string]int64, 0)
	valsHWM := make(map[string]int64, 0)

	fn := func(n string, tm tsdmetrics.StandardTaggedMetric) {
		switch n {
		case "kafka_httpcat.consumer.sent":
			if m, ok := tm.Metric.(metrics.Gauge); !ok {
				log.Printf("Unexpect metric type.")
			} else {
				valsSent[tm.Tags["partition"]] = m.Value()
			}
		case "kafka_httpcat.consumer.committed":
			if m, ok := tm.Metric.(metrics.Gauge); !ok {
				log.Printf("Unexpect metric type.")
			} else {
				valsCommitted[tm.Tags["partition"]] = m.Value()
			}
		case "kafka_httpcat.consumer.high_water_mark":
			if m, ok := tm.Metric.(metrics.Gauge); !ok {
				log.Printf("Unexpect metric type.")
			} else {
				valsHWM[tm.Tags["partition"]] = m.Value()
			}
		}
	}

	r.Each(fn)

	for partition, hwmOffset := range valsHWM {
		if sentOffset, ok := valsSent[partition]; ok {
			i := r.GetOrRegister("consumer.sent.offset_lag", tsdmetrics.Tags{"partition": partition}, metrics.NewGauge())
			if m, ok := i.(metrics.Gauge); ok {
				offsetLag := hwmOffset - sentOffset
				m.Update(offsetLag)
			} else {
				log.Print("Unexpected metric type")
			}
		}

		if committedOffset, ok := valsCommitted[partition]; ok {
			i := r.GetOrRegister("consumer.committed.offset_lag", tsdmetrics.Tags{"partition": partition}, metrics.NewGauge())
			if m, ok := i.(metrics.Gauge); ok {
				offsetLag := hwmOffset - committedOffset
				m.Update(offsetLag)
			} else {
				log.Print("Unexpected metric type")
			}
		}

	}
}

func commaDelimitedToStringList(s string) []string {
	list := strings.Split(s, ",")
	cleanList := make([]string, 0)
	for _, v := range list {
		c := strings.TrimSpace(v)
		if c != "" {
			cleanList = append(cleanList, c)
		}
	}
	return cleanList
}

func stringListToHeaderMap(l []string) (map[string][]string, error) {
	headers := make(map[string][]string, len(l))
	for _, h := range l {
		idx := strings.Index(h, ":")
		if idx == -1 {
			return nil, fmt.Errorf("Unable to parse header %s", h)
		}
		headers[h[0:idx]] = append(headers[h[0:idx]], strings.TrimSpace(h[idx+1:]))
	}

	return headers, nil
}

func commaDelimitedToIntList(s string) ([]int, error) {
	list := strings.Split(s, ",")
	intList := make([]int, len(list))
	for i, v := range list {
		var err error
		intList[i], err = strconv.Atoi(strings.TrimSpace(v))
		if err != nil {
			return nil, err
		}
	}
	return intList, nil
}

func main() {
	app := cli.NewApp()
	app.Name = "kafka_httpcat"
	app.Usage = "Forward kafka data to http endpoint"
	app.Version = version

	host, err := os.Hostname()
	if err != nil {
		log.Fatalf("Unable to get hostname: %s", err)
	}

	app.Flags = []cli.Flag{
		cli.IntFlag{
			Name:   "verbosity",
			Value:  2,
			Usage:  "verbosity (0-5)",
			EnvVar: "VERBOSITY",
		},
		cli.StringFlag{
			Name:   "target-host-list, t",
			Usage:  "Comma delimited target hosts",
			EnvVar: "TARGET_HOST_LIST",
		},
		cli.StringFlag{
			Name:   "target-path, p",
			Usage:  "HTTP path",
			EnvVar: "TARGET_PATH",
		},
		cli.StringFlag{
			Name:   "headers, H",
			Usage:  "Comma delimited headers",
			EnvVar: "HTTP_HEADERS",
		},
		cli.StringFlag{
			Name:   "method, m",
			Value:  "POST",
			Usage:  "HTTP method",
			EnvVar: "HTTP_METHOD",
		},
		cli.StringFlag{
			Name:   "expected-statuses, e",
			Value:  "200",
			Usage:  "Comma delimited list of expected HTTP status",
			EnvVar: "HTTP_EXPECTED_STATUSES",
		},
		cli.StringFlag{
			Name:   "kafka-broker-list, b",
			Usage:  "Comma delimited kafka broker list.",
			EnvVar: "KAFKA_BROKER_LIST",
		},
		cli.StringFlag{
			Name:   "kafka-topic, T",
			Usage:  "Kafka topic.",
			EnvVar: "KAFKA_TOPIC",
		},
		cli.StringFlag{
			Name:   "kafka-consumer-id, i",
			Value:  fmt.Sprintf("%s-%d", host, os.Getpid()),
			Usage:  "Kafka consumer id.",
			EnvVar: "KAFKA_CONSUMER_ID",
		},
		cli.StringFlag{
			Name:   "kafka-consumer-group, g",
			Usage:  "Kafka consumer group.",
			EnvVar: "KAFKA_CONSUMER_GROUP",
		},
		cli.StringFlag{
			Name:   "kafka-start-offset, o",
			Value:  "newest",
			Usage:  "Kafka offset to start with (newest or oldest)",
			EnvVar: "KAFKA_START_OFFSET",
		},
		cli.IntFlag{
			Name:   "kafka-buffer-size, B",
			Value:  1024,
			Usage:  "Kafka buffer size.",
			EnvVar: "KAFKA_BUFFER_SIZE",
		},
		cli.IntFlag{
			Name:   "kafka-commit-batch, c",
			Value:  1000,
			Usage:  "Commit consumed messages every X messages.",
			EnvVar: "KAFKA_COMMIT_BATCH",
		},
		cli.StringFlag{
			Name:   "metrics-report-url, r",
			Usage:  "Where to send OpenTSDB metrics.",
			EnvVar: "METRICS_REPORT_URL",
		},
	}

	app.Action = func(c *cli.Context) {
		var initialOffset int64

		switch c.String("kafka-start-offset") {
		case "oldest":
			initialOffset = sarama.OffsetOldest
		case "newest":
			initialOffset = sarama.OffsetNewest
		default:
			log.Fatal("offset should be `oldest` or `newest`")
		}

		expectedStatuses, err := commaDelimitedToIntList(c.String("expected-statuses"))
		if err != nil {
			log.Fatalf("Expected http status must be an integer: %s", err)
		}

		httpHeaderList := commaDelimitedToStringList(c.String("headers"))
		httpHeaders, err := stringListToHeaderMap(httpHeaderList)
		if err != nil {
			log.Fatalf("Unable to parse headers: %s", err)
		}

		targetHosts := commaDelimitedToStringList(c.String("target-host-list"))

		metricsRegistry := tsdmetrics.NewPrefixedTaggedRegistry("kafka_httpcat", tsdmetrics.Tags{"topic": c.String("kafka-topic"), "consumergroup": c.String("kafka-consumer-group")})
		metricsTsdb := tsdmetrics.TaggedOpenTSDBConfig{Addr: c.String("metrics-report-url"), Registry: metricsRegistry, FlushInterval: 15 * time.Second, DurationUnit: time.Millisecond, Format: tsdmetrics.Json}

		log.Printf("Connecting to: %s", c.String("kafka-broker-list"))

		saramaConfig := kafka_httpcat.GetDefaultSaramaConfig()

		brokerList := commaDelimitedToStringList(c.String("kafka-broker-list"))

		consumer, err := sarama.NewConsumer(brokerList, saramaConfig)
		if err != nil {
			log.Fatalf("Failed to start consumer: %s", err)
		}

		partitionList, err := consumer.Partitions(c.String("kafka-topic"))
		if err != nil {
			log.Fatalf("Failed to get the list of partitions: %s", err)
		}

		var (
			messages = make(chan *sarama.ConsumerMessage, c.Int("kafka-buffer-size"))
			closing  = make(chan struct{})
			wg       sync.WaitGroup
		)

		om := kafka_httpcat.NewOffsetManager(metricsRegistry, brokerList, partitionList, c.String("kafka-topic"), c.String("kafka-consumer-id"), c.String("kafka-consumer-group"), initialOffset, int64(c.Int("kafka-commit-batch")))

		go func() {
			signals := make(chan os.Signal, 1)
			signal.Notify(signals, os.Kill, os.Interrupt)
			<-signals
			log.Println("Initiating shutdown of consumer...")
			close(closing)
		}()

		for _, partition := range partitionList {
			offset := om.GetCurrentOffset(partition)
			log.Printf("Starting consumer on topic %s partition %d offset %d", c.String("kafka-topic"), partition, offset)
			pc, err := consumer.ConsumePartition(c.String("kafka-topic"), partition, offset)
			if err != nil {
				log.Fatalf("Failed to start consumer for partition %d: %s", partition, err)
			}

			m := metrics.NewGauge()
			m.Update(pc.HighWaterMarkOffset())
			metricsRegistry.GetOrRegister("consumer.high_water_mark", tsdmetrics.Tags{"partition": fmt.Sprintf("%d", partition)}, m)

			go func(pc sarama.PartitionConsumer) {
				<-closing
				pc.AsyncClose()
			}(pc)

			wg.Add(1)
			go func(pc sarama.PartitionConsumer) {
				defer wg.Done()
				for message := range pc.Messages() {
					m.Update(pc.HighWaterMarkOffset())
					messages <- message
				}
			}(pc)
		}

		go tsdmetrics.TaggedOpenTSDBWithConfigAndPreprocessing(metricsTsdb, []func(tsdmetrics.TaggedRegistry){generateConsumerLag})

		go func() {
			for msg := range messages {
				sender := kafka_httpcat.NewHTTPSender(targetHosts, c.String("target-path"), c.String("method"), httpHeaders, expectedStatuses)
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
		log.Println("Done consuming topic", c.String("kafka-topic"))
		close(messages)

		if err := consumer.Close(); err != nil {
			log.Println("Failed to close consumer: ", err)
		}

		om.CommitAll()
	}

	app.Run(os.Args)
}
