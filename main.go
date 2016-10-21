package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/net/context"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/codegangsta/cli"
	"github.com/mathpl/go-tsdmetrics"
	"github.com/rcrowley/go-metrics"
)

var version = "0.4"

func generateConsumerLag(r tsdmetrics.TaggedRegistry) {
	valsSent := make(map[string]int64, 0)
	valsCommitted := make(map[string]int64, 0)
	valsHWM := make(map[string]int64, 0)

	fn := func(n string, tm tsdmetrics.TaggedMetric) {
		switch n {
		case "kafka_httpcat.consumer.sent":
			if m, ok := tm.GetMetric().(metrics.Gauge); !ok {
				log.Printf("Unexpect metric type.")
			} else {
				valsSent[tm.GetTags()["partition"]] = m.Value()
			}
		case "kafka_httpcat.consumer.committed":
			if m, ok := tm.GetMetric().(metrics.Gauge); !ok {
				log.Printf("Unexpect metric type.")
			} else {
				valsCommitted[tm.GetTags()["partition"]] = m.Value()
			}
		case "kafka_httpcat.consumer.high_water_mark":
			if m, ok := tm.GetMetric().(metrics.Gauge); !ok {
				log.Printf("Unexpect metric type.")
			} else {
				valsHWM[tm.GetTags()["partition"]] = m.Value()
			}
		}
	}

	r.Each(fn)

	for partition, hwmOffset := range valsHWM {
		if sentOffset, ok := valsSent[partition]; ok {
			i := r.GetOrRegister("kafka_httpcat.consumer.sent.offset_lag", tsdmetrics.Tags{"partition": partition}, metrics.NewGauge())
			if m, ok := i.(metrics.Gauge); ok {
				offsetLag := hwmOffset - sentOffset
				m.Update(offsetLag)
			} else {
				log.Print("Unexpected metric type")
			}
		}

		if committedOffset, ok := valsCommitted[partition]; ok {
			i := r.GetOrRegister("kafka_httpcat.consumer.committed.offset_lag", tsdmetrics.Tags{"partition": partition}, metrics.NewGauge())
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
		log.Printf("Unable to get hostname: %s", err)
		os.Exit(1)
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
		cli.StringFlag{
			Name:   "metrics-tags",
			Usage:  "Comma delimited list of default tags",
			EnvVar: "METRICS_TAGS",
		},
	}

	app.Action = func(c *cli.Context) error {
		expectedStatuses, err := commaDelimitedToIntList(c.String("expected-statuses"))
		if err != nil {
			return fmt.Errorf("Expected http status must be an integer: %s", err)
		}

		httpHeaderList := commaDelimitedToStringList(c.String("headers"))
		httpHeaders, err := stringListToHeaderMap(httpHeaderList)
		if err != nil {
			return fmt.Errorf("Unable to parse headers: %s", err)
		}

		targetHosts := commaDelimitedToStringList(c.String("target-host-list"))
		defaultTags, err := tsdmetrics.TagsFromString(c.String("metrics-tags"))
		if err != nil {
			return err
		}
		defaultTags = defaultTags.AddTags(tsdmetrics.Tags{"topic": c.String("kafka-topic"), "consumergroup": c.String("kafka-consumer-group")})

		rootRegistry := tsdmetrics.NewSegmentedTaggedRegistry("", defaultTags, nil)
		tsdmetrics.RegisterTaggedRuntimeMemStats(rootRegistry)
		metricsRegistry := tsdmetrics.NewSegmentedTaggedRegistry("kafka_httpcat", nil, rootRegistry)
		metricsTsdb := tsdmetrics.TaggedOpenTSDB{Addr: c.String("metrics-report-url"), Registry: rootRegistry, FlushInterval: 15 * time.Second, DurationUnit: time.Millisecond, Format: tsdmetrics.Json}

		log.Printf("Connecting to: %s", c.String("kafka-broker-list"))

		// Init config
		config := cluster.NewConfig()
		config.Consumer.Return.Errors = true
		config.Group.Return.Notifications = true

		switch c.String("kafka-start-offset") {
		case "oldest":
			config.Consumer.Offsets.Initial = sarama.OffsetOldest
		case "newest":
			config.Consumer.Offsets.Initial = sarama.OffsetNewest
		default:
			return fmt.Errorf("offset should be `oldest` or `newest`")
		}

		brokerList := strings.Split(c.String("kafka-broker-list"), ",")
		topicList := strings.Split(c.String("kafka-topic"), ",")

		// Init consumer, consume errors & messages
		consumer, err := cluster.NewConsumer(brokerList, c.String("kafka-consumer-group"), topicList, config)
		if err != nil {
			log.Fatal(err)
		}

		wait := make(chan os.Signal, 1)
		signal.Notify(wait, syscall.SIGINT, syscall.SIGTERM)

		go func() {
			for err := range consumer.Errors() {
				fmt.Printf("Error: %s\n", err.Error())

				// Abort all
				wait <- syscall.SIGTERM
			}
		}()

		go func() {
			for note := range consumer.Notifications() {
				fmt.Printf("Rebalanced: %+v\n", note)
			}
		}()

		batchSize := int64(c.Int("kafka-commit-batch"))

		hwmUpdate := func(r tsdmetrics.TaggedRegistry) {
			hwms := consumer.HighWaterMarks()
			for _, hwm := range hwms {
				for partition, offset := range hwm {
					tags := tsdmetrics.Tags{"partition": fmt.Sprintf("%d", partition)}
					m := metricsRegistry.GetOrRegister("consumer.high_water_mark", tags, metrics.NewGauge())
					m.(metrics.Gauge).Update(offset)
				}
			}
		}

		go func() {
			sender := NewHTTPSender(targetHosts, c.String("target-path"), c.String("method"), httpHeaders, expectedStatuses)
			for msg := range consumer.Messages() {
				if err := sender.RRSend(msg.Value); err != nil {
					log.Printf("Error send data: %s\n", err)
				}

				tags := tsdmetrics.Tags{"partition": fmt.Sprintf("%d", msg.Partition)}
				s := metricsRegistry.GetOrRegister("consumer.sent", tags, metrics.NewGauge())
				s.(metrics.Gauge).Update(msg.Offset)

				if msg.Offset%batchSize == 0 {
					consumer.MarkOffset(msg, "")
					c := metricsRegistry.GetOrRegister("consumer.committed", tags, metrics.NewGauge())
					c.(metrics.Gauge).Update(msg.Offset)
					log.Printf("Commited offset partition:%d offset:%d\n", msg.Partition, msg.Offset)
				}
			}
		}()

		collectFn := tsdmetrics.RuntimeCaptureFn
		collectFn = append(collectFn, hwmUpdate)
		collectFn = append(collectFn, generateConsumerLag)

		go metricsTsdb.RunWithPreprocessing(context.Background(), collectFn)

		<-wait
		consumer.CommitOffsets()

		if err := consumer.Close(); err != nil {
			fmt.Printf("Failed to close consumer: %s\n", err)
		}

		return nil
	}

	err = app.Run(os.Args)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
