package tsdmetrics

import (
	"net"
	"time"
)

func ExampleTaggedOpenTSDB() {
	addr, _ := net.ResolveTCPAddr("net", ":2003")
	go TaggedOpenTSDB(DefaultTaggedRegistry, 1*time.Second, "some.prefix", addr)
}

func ExampleTaggedOpenTSDBWithConfig() {
	addr, _ := net.ResolveTCPAddr("net", ":2003")
	go TaggedOpenTSDBWithConfig(TaggedOpenTSDBConfig{
		Addr:          addr,
		Registry:      DefaultTaggedRegistry,
		FlushInterval: 1 * time.Second,
		DurationUnit:  time.Millisecond,
	})
}
