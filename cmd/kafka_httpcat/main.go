package main

import (
	"bufio"
	"flag"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"

	"github.com/BurntSushi/toml"
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
}

func readConf(filename string) (conf *Config) {
	f, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Unable to open config file: %s", err)
	}
	defer f.Close()

	conf = &Config{}

	md, err := toml.DecodeReader(f, conf)
	if err != nil {
		log.Fatal("Unable to parse config file: %s", err)
	}
	if u := md.Undecoded(); len(u) > 0 {
		log.Fatal("Extra keys in config file: %v", u)
	}

	return
}

func main() {
	flag.Parse()
	conf := readConf(*flagConf)

	sender := kafka_httpcat.NewHTTPSender(conf.Hosts, conf.ContextPath, conf.Method, conf.Headers)

	r := bufio.NewReader(os.Stdin)
	for {
		if payload_size_msg, err := r.ReadBytes(10); err == nil {
			if payload_size, err := strconv.ParseInt(string(payload_size_msg[0:len(payload_size_msg)-1]), 10, 32); err == nil {
				//	gzreader, err := gzip.NewReader(io.LimitReader(r, payload_size))
				//	if err != nil {
				//		log.Fatal("Unable to uncompress payload")
				//	}
				//	gzreader.Multistream(false)

				if err := sender.RRSend(ioutil.NopCloser(io.LimitReader(r, payload_size))); err != nil {
					log.Printf("%s", err)
				}
			} else {
				log.Printf("Unable to parse payload size: %s", err)
			}
		} else if err == io.EOF {
			log.Printf("STDIN closed, stopping.")
			break
		} else {
			log.Printf("Unable to find delimiter: %s", err)
		}
	}
}
