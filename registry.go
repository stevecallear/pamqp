package pamqp

import (
	"fmt"
	"sync"

	"github.com/streadway/amqp"
	"google.golang.org/protobuf/proto"
)

type (
	// Registry represents an exchange/queue registry
	Registry struct {
		exchangeNameFn func(proto.Message) string
		queueNameFn    func(proto.Message) string
		channel        Channel
		store          map[string]string
		mu             sync.RWMutex
	}

	// RegistryOptions represents a set of registry options
	RegistryOptions struct {
		ChannelFn      func(*amqp.Connection) (Channel, error)
		ExchangeNameFn func(proto.Message) string
		QueueNameFn    func(proto.Message) string
	}
)

var defaultRegistryOptions = RegistryOptions{
	ChannelFn: func(c *amqp.Connection) (Channel, error) {
		return c.Channel()
	},
	ExchangeNameFn: func(m proto.Message) string {
		return entityName(m)
	},
	QueueNameFn: func(m proto.Message) string {
		return entityName(m)
	},
}

// NewRegistry returns a new registry
func NewRegistry(conn *amqp.Connection, optFns ...func(*RegistryOptions)) (*Registry, error) {
	o := defaultRegistryOptions
	for _, fn := range optFns {
		fn(&o)
	}

	ch, err := o.ChannelFn(conn)
	if err != nil {
		return nil, err
	}

	return &Registry{
		exchangeNameFn: o.ExchangeNameFn,
		queueNameFn:    o.QueueNameFn,
		channel:        ch,
		store:          map[string]string{},
	}, nil
}

// Exchange ensures that the specified exchange exists and returns the name
func (r *Registry) Exchange(m proto.Message) (string, error) {
	return r.getOrSet("exchange:"+MessageName(m), func() (string, error) {
		en := r.exchangeNameFn(m)

		err := r.channel.ExchangeDeclare(en, amqp.ExchangeFanout, false, false, false, false, nil)
		if err != nil {
			return "", err
		}

		return en, nil
	})
}

// Exchange ensures that the specified queue exists and returns the name
func (r *Registry) Queue(m proto.Message) (string, error) {
	en, err := r.Exchange(m)
	if err != nil {
		return "", err
	}

	return r.getOrSet("queue:"+MessageName(m), func() (string, error) {
		qn := r.queueNameFn(m)

		q, err := r.channel.QueueDeclare(qn, false, false, false, false, nil)
		if err != nil {
			return "", err
		}

		err = r.channel.QueueBind(q.Name, "", en, false, nil)
		if err != nil {
			return "", err
		}

		return q.Name, err
	})
}

// Close closes the underlying channel
func (r *Registry) Close() error {
	return r.channel.Close()
}

func (r *Registry) getOrSet(key string, fn func() (string, error)) (string, error) {
	v, ok := func() (string, bool) {
		r.mu.RLock()
		defer r.mu.RUnlock()

		v, ok := r.store[key]
		return v, ok
	}()
	if ok {
		return v, nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if v, ok = r.store[key]; ok {
		return v, nil
	}

	var err error
	v, err = fn()
	if err != nil {
		return "", err
	}

	r.store[key] = v
	return v, nil
}

// WithPrefixNaming applies a prefix naming convention to exchange and queue names
// Exchanges take the form stage-package-message
// Queues take the form stage-package-message-service
func WithPrefixNaming(stage, service string) func(*RegistryOptions) {
	return func(o *RegistryOptions) {
		o.ExchangeNameFn = func(m proto.Message) string {
			return fmt.Sprintf("%s-%s", stage, entityName(m))
		}
		o.QueueNameFn = func(m proto.Message) string {
			return fmt.Sprintf("%s-%s-%s", stage, service, entityName(m))
		}
	}
}
