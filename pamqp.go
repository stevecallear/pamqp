package pamqp

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/streadway/amqp"
	"google.golang.org/protobuf/proto"
)

type (
	// Channel represents an amqp channel interface
	Channel interface {
		ExchangeDeclare(name string, kind string, durable bool, autoDelete bool, internal bool, noWait bool, args amqp.Table) error
		QueueDeclare(name string, durable bool, autoDelete bool, exclusive bool, noWait bool, args amqp.Table) (amqp.Queue, error)
		QueueBind(name string, key string, exchange string, noWait bool, args amqp.Table) error
		Publish(exchange string, key string, mandatory bool, immediate bool, msg amqp.Publishing) error
		Consume(queue string, consumer string, autoAck bool, exclusive bool, noLocal bool, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
		Close() error
	}

	// HandlerFunc represents a handler func
	HandlerFunc func(context.Context, proto.Message, Metadata) error

	// MiddlewareFunc represents a middleware func
	MiddlewareFunc func(HandlerFunc) HandlerFunc

	// Metadata represents message metadata
	Metadata struct {
		ID            string
		Type          string
		ContentType   string
		CorrelationID string
		Timestamp     time.Time
		Headers       map[string]interface{}
	}

	// Options represents a set of options
	Options struct {
		ChannelFn      func(*amqp.Connection) (Channel, error)
		ExchangeNameFn func(proto.Message) (string, error)
		QueueNameFn    func(proto.Message) (string, error)
		RoutingKeyFn   func(proto.Message) (string, error)
		MiddlewareFn   MiddlewareFunc
		ErrorFn        func(Metadata, error)
	}
)

var (
	// UTCNow returns the current utc time
	UTCNow = func() time.Time {
		return time.Now().UTC()
	}

	// NewID returns a unique id
	NewID = func() string {
		return uuid.NewString()
	}

	defaultOptions = Options{
		ChannelFn: func(c *amqp.Connection) (Channel, error) {
			return c.Channel()
		},
		ExchangeNameFn: func(m proto.Message) (string, error) {
			return strings.ToLower(MessageName(m)), nil
		},
		QueueNameFn: func(m proto.Message) (string, error) {
			return strings.ToLower(MessageName(m)), nil
		},
		RoutingKeyFn: func(proto.Message) (string, error) {
			return "", nil
		},
		ErrorFn: func(_ Metadata, err error) {
			log.Println(err)
		},
	}
)

// MessageName returns the message name
func MessageName(m proto.Message) string {
	return string(m.ProtoReflect().Descriptor().FullName())
}

// ChainMiddleware returns a middleware func that wraps the specified funcs
func ChainMiddleware(m ...MiddlewareFunc) MiddlewareFunc {
	return func(n HandlerFunc) HandlerFunc {
		for i := len(m) - 1; i >= 0; i-- {
			n = m[i](n)
		}
		return n
	}
}

// WithRegistry configures the publisher/consumer to use the specified registry
func WithRegistry(r *Registry) func(*Options) {
	return func(o *Options) {
		o.ExchangeNameFn = r.Exchange
		o.QueueNameFn = r.Queue
	}
}

// WithMiddleware configures the publisher/consumer to use the specified middleware
func WithMiddleware(mw ...MiddlewareFunc) func(*Options) {
	return func(o *Options) {
		o.MiddlewareFn = ChainMiddleware(mw...)
	}
}
