package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"

	"github.com/lajosnagyuk/spruce/pkg/spruce"
)

func TestLifecycleHelper(t *testing.T) {
	if os.Getenv("SPRUCE_LIFECYCLE_TEST_HELPER") != "1" {
		return
	}
	for i, arg := range os.Args {
		if arg == "--" {
			os.Args = append([]string{"spruce-lifecycle"}, os.Args[i+1:]...)
			break
		}
	}
	flag.CommandLine = flag.NewFlagSet("spruce-lifecycle", flag.ExitOnError)
	main()
	os.Exit(0)
}

func TestLifecycleOracleDetectsDeliveryFailures(t *testing.T) {
	for _, mode := range []string{"healthy", "missing", "duplicate", "reordered"} {
		t.Run(mode, func(t *testing.T) {
			var mu sync.Mutex
			groups := make(map[string]chan []byte)
			var held []byte
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/subscriptions/stream" {
					ch := make(chan []byte, 16)
					group := r.URL.Query().Get("group")
					mu.Lock()
					groups[group] = ch
					mu.Unlock()
					w.WriteHeader(200)
					w.(http.Flusher).Flush()
					for {
						select {
						case <-r.Context().Done():
							return
						case payload := <-ch:
							var e event
							if err := json.Unmarshal(payload, &e); err != nil {
								panic(err)
							}
							meta, _ := json.Marshal(spruce.Delivery{DeliveryID: fmt.Sprint(e.Sequence), MessageID: fmt.Sprint(e.Sequence), Topic: r.URL.Query().Get("topic"), Key: fmt.Sprint(e.Key), Cursor: fmt.Sprint(e.Sequence)})
							var header [8]byte
							binary.BigEndian.PutUint32(header[:4], uint32(len(meta)))
							binary.BigEndian.PutUint32(header[4:], uint32(len(payload)))
							_, _ = w.Write(header[:])
							_, _ = w.Write(meta)
							_, _ = w.Write(payload)
							w.(http.Flusher).Flush()
						}
					}
				}
				if strings.HasPrefix(r.URL.Path, "/v1/deliveries/") {
					w.WriteHeader(204)
					return
				}
				payload, err := io.ReadAll(r.Body)
				if err != nil {
					w.WriteHeader(400)
					return
				}
				var e event
				if json.Unmarshal(payload, &e) != nil {
					w.WriteHeader(400)
					return
				}
				mu.Lock()
				for group, ch := range groups {
					if group == "group-1" {
						if mode == "missing" && e.Sequence == 1 {
							continue
						}
						if mode == "reordered" && e.Sequence == 0 {
							held = payload
							continue
						}
						if mode == "duplicate" && e.Sequence == 1 {
							ch <- payload
						}
					}
					ch <- payload
					if group == "group-1" && mode == "reordered" && e.Sequence == 1 {
						ch <- held
					}
				}
				mu.Unlock()
				w.WriteHeader(202)
				_, _ = fmt.Fprintf(w, `{"id":"%d","replicated":true}`, e.Sequence)
			}))
			defer server.Close()
			cmd := exec.Command(os.Args[0], "-test.run=^TestLifecycleHelper$", "--", "-server", server.URL, "-producers", "1", "-topics", "1", "-groups", "2", "-members", "1", "-keys", "1", "-messages", "3", "-timeout", "1500ms", "-settle", "10ms")
			cmd.Env = append(os.Environ(), "SPRUCE_LIFECYCLE_TEST_HELPER=1", "SPRUCE_TOKEN=")
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			err := cmd.Run()
			var got report
			if decodeErr := json.Unmarshal(stdout.Bytes(), &got); decodeErr != nil {
				t.Fatalf("no report: %s %s err=%v", stdout.String(), stderr.String(), err)
			}
			if got.Accepted != 3 || got.Invalid != 0 {
				t.Fatalf("unexpected report: %+v", got)
			}
			switch mode {
			case "healthy":
				if err != nil || got.Missing+got.Duplicates+got.OrderRegressions != 0 {
					t.Fatalf("healthy failed: %+v %v", got, err)
				}
			case "missing":
				if err == nil || got.Missing != 1 {
					t.Fatalf("missing event passed unnoticed: %+v", got)
				}
			case "duplicate":
				if err == nil || got.Duplicates != 1 {
					t.Fatalf("duplicate passed unnoticed: %+v", got)
				}
			case "reordered":
				if err == nil || got.OrderRegressions != 1 {
					t.Fatalf("reordering passed unnoticed: %+v", got)
				}
			}
		})
	}
}
