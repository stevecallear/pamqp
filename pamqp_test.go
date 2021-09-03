package pamqp_test

//go:generate mockgen -source=./pamqp.go -destination=./internal/mocks/pamqp.go -package=mocks
//go:generate mockgen -destination=./internal/mocks/amqp.go -package=mocks github.com/streadway/amqp Acknowledger

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/streadway/amqp"
	"google.golang.org/protobuf/proto"

	"github.com/stevecallear/pamqp"
	"github.com/stevecallear/pamqp/proto/testpb"
)

func TestMessageName(t *testing.T) {
	t.Run("should return the message name", func(t *testing.T) {
		in := new(testpb.Message)

		if act, exp := pamqp.MessageName(in), "test.Message"; act != exp {
			t.Errorf("got %s, expected %s", act, exp)
		}
	})
}

func TestWithRegistry(t *testing.T) {
	t.Run("should configure the publisher to use the registry", func(t *testing.T) {
		r, err := pamqp.NewRegistry(nil, func(o *pamqp.RegistryOptions) {
			o.ChannelFn = func(*amqp.Connection) (pamqp.Channel, error) {
				return nil, nil
			}
		})
		assertErrorExists(t, err, false)

		var o pamqp.Options
		pamqp.WithRegistry(r)(&o)

		act := reflect.ValueOf(o.ExchangeNameFn).Pointer()
		exp := reflect.ValueOf(r.Exchange).Pointer()

		if act != exp {
			t.Errorf("got %v, expected %v", act, exp)
		}

		act = reflect.ValueOf(o.QueueNameFn).Pointer()
		exp = reflect.ValueOf(r.Queue).Pointer()

		if act != exp {
			t.Errorf("got %v, expected %v", act, exp)
		}
	})
}

func TestWithMiddleware(t *testing.T) {
	t.Run("should configure the publisher to use the middleware", func(t *testing.T) {
		b := new(strings.Builder)

		var o pamqp.Options
		pamqp.WithMiddleware(newMiddleware("1", b), newMiddleware("2", b))(&o)

		o.MiddlewareFn(func(context.Context, proto.Message, pamqp.Metadata) error {
			b.WriteString("h")
			return nil
		})(context.Background(), new(testpb.Message), pamqp.Metadata{})

		if act, exp := b.String(), "12h21"; act != exp {
			t.Errorf("got %s, expected %s", act, exp)
		}
	})
}

func newMiddleware(id string, b *strings.Builder) pamqp.MiddlewareFunc {
	return func(n pamqp.HandlerFunc) pamqp.HandlerFunc {
		return func(ctx context.Context, m proto.Message, md pamqp.Metadata) error {
			b.WriteString(id)
			defer b.WriteString(id)

			return n(ctx, m, md)
		}
	}
}

func assertErrorExists(t *testing.T, act error, exp bool) {
	if act != nil && !exp {
		t.Errorf("got %v, expected nil", act)
	}
	if act == nil && exp {
		t.Error("got nil, expected an error")
	}
}

func marshal(m proto.Message) []byte {
	b, err := proto.Marshal(m)
	if err != nil {
		panic(err)
	}
	return b
}

func setUTCNow(t time.Time) func() {
	pfn := pamqp.UTCNow
	pamqp.UTCNow = func() time.Time {
		return t
	}

	return func() {
		pamqp.UTCNow = pfn
	}
}

func setNewID(id string) func() {
	pfn := pamqp.NewID
	pamqp.NewID = func() string {
		return id
	}

	return func() {
		pamqp.NewID = pfn
	}
}
