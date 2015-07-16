package kafka_httpcat

import (
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"time"
)

type HTTPSender struct {
	hosts             []string
	contextPath       string
	method            string
	parsedContextPath *url.URL
	headers           map[string][]string
	currentHost       int

	client *http.Client
}

func NewHTTPSender(hosts []string, contextPath string, method string, headers map[string][]string) *HTTPSender {
	//Random host
	if len(hosts) == 0 {
		log.Fatal("Need at least one host defined.")
	}
	cur := rand.Int() % len(hosts)
	if u, err := url.Parse(contextPath); err != nil {
		log.Fatalf("Unable to parse context path: %s", err)
		return nil
	} else {
		return &HTTPSender{hosts: hosts, contextPath: contextPath, headers: headers, parsedContextPath: u, currentHost: cur, client: &http.Client{}}
	}
}

func (h *HTTPSender) buildBaseRequest(contextPath string, method string, headers map[string][]string, bodyReader io.ReadCloser) *http.Request {
	var req http.Request
	req.Method = h.method
	req.ProtoMajor = 1
	req.ProtoMinor = 1
	req.Close = false
	req.Header = h.headers
	//req.TransferEncoding = []string{"chunked"}
	req.URL = h.parsedContextPath
	req.URL.Scheme = "http"
	req.URL.Host = h.hosts[h.currentHost]
	req.Body = bodyReader
	return &req
}

func (h *HTTPSender) send(bodyReader io.ReadCloser) error {
	req := h.buildBaseRequest(h.contextPath, h.method, h.headers, bodyReader)
	if resp, err := h.client.Do(req); err != nil {
		log.Printf("inot sent!: %s", err)
		return err
	} else {
		log.Printf("resp: %s", resp)
	}

	return nil
}

func (h *HTTPSender) RRSend(bodyReader io.ReadCloser) error {
	retries := 0
	for {
		if err := h.send(bodyReader); err != nil {
			//Round robin
			h.currentHost = (h.currentHost + 1) % len(h.hosts)

			retries++
			if retries > 10 {
				time.Sleep(time.Second)
			}
		} else {
			return nil
		}
	}
}
