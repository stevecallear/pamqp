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

const (
	exchangeName = "pamqp.static"
	queueName    = "pamqp.static.test.message"
	routingKey   = "test.#"
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

	err = createInfrastructure(conn, exchangeName, queueName, routingKey)
	if err != nil {
		log.Fatal(err)
	}

	opts := func(o *pamqp.Options) {
		o.ExchangeNameFn = func(proto.Message) (string, error) {
			return exchangeName, nil
		}
		o.QueueNameFn = func(proto.Message) (string, error) {
			return queueName, nil
		}
		o.RoutingKeyFn = func(proto.Message) (string, error) {
			return routingKey, nil
		}
	}

	p, err := pamqp.NewPublisher(conn, opts)
	if err != nil {
		log.Fatal(err)
	}

	c := pamqp.NewConsumer(conn, opts)

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

func createInfrastructure(conn *amqp.Connection, xn, qn, rk string) error {
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	err = ch.ExchangeDeclare(xn, amqp.ExchangeTopic, false, false, false, false, nil)
	if err != nil {
		return err
	}

	q, err := ch.QueueDeclare(qn, false, false, false, false, nil)
	if err != nil {
		return err
	}

	return ch.QueueBind(q.Name, rk, xn, false, nil)
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
