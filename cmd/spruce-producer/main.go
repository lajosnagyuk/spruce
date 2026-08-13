package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/lajosnagyuk/spruce/pkg/spruce"
	"log"
	"os"
	"time"
)

func main() {
	server := flag.String("server", "http://localhost:8080", "Spruce URL")
	topic := flag.String("topic", "demo", "topic")
	message := flag.String("message", "hello from spruce", "message")
	count := flag.Int("count", 1, "messages")
	flag.Parse()
	c := spruce.New(*server)
	c.Token = envToken()
	for i := 0; i < *count; i++ {
		r, e := c.Publish(context.Background(), *topic, []byte(*message), spruce.PublishOptions{ContentType: "text/plain", TTL: time.Minute})
		if e != nil {
			log.Fatal(e)
		}
		fmt.Println(r.ID)
	}
}

func envToken() string { return os.Getenv("SPRUCE_TOKEN") }
