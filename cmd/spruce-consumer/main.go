package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/lajosnagyuk/spruce/pkg/spruce"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	server := flag.String("server", "http://localhost:8080", "Spruce URL")
	topic := flag.String("topic", "demo", "topic")
	group := flag.String("group", "", "consumer group")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	c := spruce.New(*server)
	c.Token = os.Getenv("SPRUCE_TOKEN")
	e := c.Subscribe(ctx, spruce.SubscribeOptions{Topic: *topic, Group: *group}, func(_ context.Context, d spruce.Delivery) error {
		fmt.Printf("%s attempt=%d %s\n", d.MessageID, d.Attempt, string(d.Payload))
		return nil
	})
	if e != nil && ctx.Err() == nil {
		log.Fatal(e)
	}
}
