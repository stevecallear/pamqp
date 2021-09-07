package pamqp

import (
	"strings"
	"sync"

	"github.com/streadway/amqp"
	"google.golang.org/protobuf/proto"
)

type (
	// Registry represents an exchange/queue registry
	Registry struct {
		exchangeNameFn           func(proto.Message) string
		queueNameFn              func(proto.Message) string
		deadLetterExchangeNameFn func(proto.Message) string
		deadLetterQueueNameFn    func(proto.Message) string
		deadLetterKeyFn          func(proto.Message) string
		channel                  Channel
		cache                    map[string][]string
		mu                       sync.RWMutex
	}

	// RegistryOptions represents a set of registry options
	RegistryOptions struct {
		ChannelFn                func(*amqp.Connection) (Channel, error)
		ExchangeNameFn           func(proto.Message) string
		QueueNameFn              func(proto.Message) string
		DeadLetterExchangeNameFn func(proto.Message) string
		DeadLetterQueueNameFn    func(proto.Message) string
		DeadLetterKeyFn          func(proto.Message) string
	}

	deadLetterConfig struct {
		exchange string
		queue    string
		key      string
	}
)

var defaultRegistryOptions = RegistryOptions{
	ChannelFn: func(c *amqp.Connection) (Channel, error) {
		return c.Channel()
	},
	ExchangeNameFn: func(m proto.Message) string {
		return strings.ToLower(MessageName(m))
	},
	QueueNameFn: func(m proto.Message) string {
		return strings.ToLower(MessageName(m))
	},
	DeadLetterExchangeNameFn: func(proto.Message) string {
		return "default_dlx"
	},
	DeadLetterQueueNameFn: func(m proto.Message) string {
		return strings.ToLower(MessageName(m)) + "_dlq"
	},
	DeadLetterKeyFn: func(m proto.Message) string {
		return strings.ToLower(MessageName(m))
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
		exchangeNameFn:           o.ExchangeNameFn,
		queueNameFn:              o.QueueNameFn,
		deadLetterExchangeNameFn: o.DeadLetterExchangeNameFn,
		deadLetterQueueNameFn:    o.DeadLetterQueueNameFn,
		deadLetterKeyFn:          o.DeadLetterKeyFn,
		channel:                  ch,
		cache:                    map[string][]string{},
	}, nil
}

// Exchange ensures that the specified exchange exists and returns the name
func (r *Registry) Exchange(m proto.Message) (string, error) {
	v, err := r.getOrSet("x:"+MessageName(m), func() ([]string, error) {
		en := r.exchangeNameFn(m)

		err := r.channel.ExchangeDeclare(en, amqp.ExchangeFanout, true, false, false, false, nil)
		if err != nil {
			return nil, err
		}

		return []string{en}, nil
	})
	if err != nil {
		return "", err
	}

	return v[0], nil
}

// Queue ensures that the specified queue exists and returns the name
func (r *Registry) Queue(m proto.Message) (string, error) {
	dlc, err := r.ensureDeadLetterConfig(m)
	if err != nil {
		return "", err
	}

	xn, err := r.Exchange(m)
	if err != nil {
		return "", err
	}

	v, err := r.getOrSet("q:"+MessageName(m), func() ([]string, error) {
		qn := r.queueNameFn(m)

		q, err := r.channel.QueueDeclare(qn, true, false, false, false, amqp.Table{
			"x-dead-letter-exchange":    dlc.exchange,
			"x-dead-letter-routing-key": dlc.key,
		})
		if err != nil {
			return nil, err
		}

		err = r.channel.QueueBind(q.Name, "", xn, false, nil)
		if err != nil {
			return nil, err
		}

		return []string{q.Name}, err
	})
	if err != nil {
		return "", err
	}

	return v[0], nil
}

// Close closes the underlying channel
func (r *Registry) Close() error {
	return r.channel.Close()
}

func (r *Registry) ensureDeadLetterConfig(m proto.Message) (deadLetterConfig, error) {
	var dlc deadLetterConfig

	dlx, err := r.getOrSet("dlx:"+MessageName(m), func() ([]string, error) {
		n := r.deadLetterExchangeNameFn(m)
		err := r.channel.ExchangeDeclare(n, amqp.ExchangeDirect, true, false, false, false, nil)
		if err != nil {
			return nil, err
		}

		return []string{n}, nil
	})
	if err != nil {
		return dlc, err
	}

	dlq, err := r.getOrSet("dlq:"+MessageName(m), func() ([]string, error) {
		n := r.deadLetterQueueNameFn(m)
		q, err := r.channel.QueueDeclare(n, true, false, false, false, nil)
		if err != nil {
			return nil, err
		}

		k := r.deadLetterKeyFn(m)
		err = r.channel.QueueBind(q.Name, k, dlx[0], false, nil)
		if err != nil {
			return nil, err
		}

		return []string{q.Name, k}, nil
	})
	if err != nil {
		return dlc, err
	}

	dlc.exchange = dlx[0]
	dlc.queue = dlq[0]
	dlc.key = dlq[1]

	return dlc, nil
}

func (r *Registry) getOrSet(key string, fn func() ([]string, error)) ([]string, error) {
	v, ok := func() ([]string, bool) {
		r.mu.RLock()
		defer r.mu.RUnlock()

		v, ok := r.cache[key]
		return v, ok
	}()
	if ok {
		return v, nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if v, ok = r.cache[key]; ok {
		return v, nil
	}

	var err error
	v, err = fn()
	if err != nil {
		return nil, err
	}

	r.cache[key] = v
	return v, nil
}

// WithConsumerNaming applies a consumer prefix naming convention to queue names
// This ensures that each separate consumer uses a dedicated queue bound to the publisher exchange.
func WithConsumerNaming(consumer string, prefixes ...string) func(*RegistryOptions) {
	pre := strings.ToLower(strings.Join(prefixes, "."))
	con := strings.ToLower(strings.Join(append(prefixes, consumer), "."))

	fmtN := func(p, s string) string {
		if p == "" {
			return s
		}
		return p + "." + s
	}

	return func(o *RegistryOptions) {
		o.ExchangeNameFn = func(m proto.Message) string {
			return fmtN(pre, strings.ToLower(MessageName(m)))
		}
		o.QueueNameFn = func(m proto.Message) string {
			return fmtN(con, strings.ToLower(MessageName(m)))
		}
		o.DeadLetterExchangeNameFn = func(proto.Message) string {
			return fmtN(pre, "default_dlx")
		}
		o.DeadLetterQueueNameFn = func(m proto.Message) string {
			return fmtN(con, strings.ToLower(MessageName(m))+"_dlq")
		}
		o.DeadLetterKeyFn = func(m proto.Message) string {
			return fmtN(con, strings.ToLower(MessageName(m)))
		}
	}
}
