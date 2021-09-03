package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"

	"github.com/streadway/amqp"
	"google.golang.org/protobuf/proto"

	"github.com/stevecallear/pamqp"
	"github.com/stevecallear/pamqp/internal/proto/testpb"
)

type handler struct{}

func (h *handler) Message() proto.Message {
	return new(testpb.Message)
}

func (h *handler) Handle(ctx context.Context, m proto.Message, md pamqp.Metadata) error {
	tm := m.(*testpb.Message)
	log.Printf("received: %s value=%s", md.ID, tm.Value)
	return nil
}

func main() {
	conn, err := amqp.Dial(envOrDefault("AMQP_ADDR", "amqp://guest:guest@localhost:5672"))
	if err != nil {
		log.Fatal(err)
	}

	r, err := pamqp.NewRegistry(conn, pamqp.WithConsumerNaming("consumer", "pamqp", "registry"))
	if err != nil {
		log.Fatal(err)
	}

	p, err := pamqp.NewPublisher(conn, pamqp.WithRegistry(r))
	if err != nil {
		log.Fatal(err)
	}

	c := pamqp.NewConsumer(conn, pamqp.WithRegistry(r))

	ctx := newContext()
	wg := new(sync.WaitGroup)

	wg.Add(1)
	go func() {
		defer wg.Done()

		if err := c.Consume(ctx, new(handler)); err != nil {
			log.Fatal(err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		t := time.NewTicker(1 * time.Second)
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := p.Publish(ctx, newMessage()); err != nil {
					log.Fatal(err)
				}
			}
		}
	}()

	wg.Wait()
	log.Println("done")
}

func newContext() context.Context {
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		<-sigc
		cancel()
	}()

	return ctx
}

func newMessage() *testpb.Message {
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	return &testpb.Message{Value: ts}
}

func envOrDefault(key, def string) string {
	if s := os.Getenv(key); s != "" {
		return s
	}

	return def
}
