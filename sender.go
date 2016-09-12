package kafka_httpcat

import (
	"bytes"
	"fmt"
	"io/ioutil"
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
	expectedRespCodes map[int]bool

	client *http.Client
}

func NewHTTPSender(hosts []string, contextPath string, method string, headers map[string][]string, expectedRespCodes []int) *HTTPSender {
	//Random host
	if len(hosts) == 0 {
		log.Fatal("Need at least one host defined.")
	}
	cur := rand.Int() % len(hosts)
	if u, err := url.Parse(contextPath); err != nil {
		log.Fatalf("Unable to parse context path: %s", err)
		return nil
	} else {
		u.Scheme = "http"
		respMap := make(map[int]bool)
		for _, expectedRespCode := range expectedRespCodes {
			respMap[expectedRespCode] = true
		}

		return &HTTPSender{hosts: hosts, contextPath: contextPath, method: method, headers: headers,
			parsedContextPath: u, expectedRespCodes: respMap, currentHost: cur, client: &http.Client{Timeout: time.Duration(15 * time.Second)}}
	}
}

func (h *HTTPSender) buildBaseRequest(contextPath string, method string, headers map[string][]string, bodyReader *bytes.Reader) *http.Request {
	var req http.Request
	req.Method = h.method
	req.ProtoMajor = 1
	req.ProtoMinor = 1
	req.Close = false
	req.Header = h.headers
	req.URL = h.parsedContextPath
	req.URL.Host = h.hosts[h.currentHost]
	req.Body = ioutil.NopCloser(bodyReader)
	req.ContentLength = int64(bodyReader.Len())
	return &req
}

func (h *HTTPSender) send(bodyReader *bytes.Reader) error {
	req := h.buildBaseRequest(h.contextPath, h.method, h.headers, bodyReader)
	//io.Copy(os.Stdout, req.Body)

	if resp, err := h.client.Do(req); err != nil {
		log.Printf("Not sent!: %s", err)
		return err
	} else {
		defer resp.Body.Close()
		if _, ok := h.expectedRespCodes[resp.StatusCode]; !ok {
			msg, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				return fmt.Errorf("Unexpected http code: %d", resp.StatusCode)
			} else {
				return fmt.Errorf("Unexpected http code: %d. %s", string(msg))
			}
		}
	}

	return nil
}

func (h *HTTPSender) RRSend(body []byte) error {
	retries := 0

	for {
		bodyReader := bytes.NewReader(body)
		if err := h.send(bodyReader); err != nil {
			log.Printf("Backing off sending: %s", err)
			return nil
			//Round robin
			h.currentHost = (h.currentHost + 1) % len(h.hosts)

			retries++

			if retries < 10 {
				time.Sleep(100 * time.Millisecond)
			} else {
				log.Printf("Backing off sending: %s", err)
				log.Printf("%s\n", string(body))
				time.Sleep(time.Second)
			}
		} else {
			return nil
		}
	}
}
