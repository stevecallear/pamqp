package pamqp

import (
	"context"

	"github.com/streadway/amqp"
	"google.golang.org/protobuf/proto"
)

type (
	// Handler represents a message handler
	Handler interface {
		Message() proto.Message
		Handle(context.Context, proto.Message, Metadata) error
	}

	// Consumer represents a consumer
	Consumer struct {
		conn         *amqp.Connection
		channelFn    func(*amqp.Connection) (Channel, error)
		queueNameFn  func(proto.Message) (string, error)
		middlewareFn MiddlewareFunc
		errorFn      func(Metadata, error)
	}
)

// NewConsumer returns a new consumer
func NewConsumer(c *amqp.Connection, optFns ...func(*Options)) *Consumer {
	o := defaultOptions
	for _, fn := range optFns {
		fn(&o)
	}

	return &Consumer{
		conn:         c,
		channelFn:    o.ChannelFn,
		queueNameFn:  o.QueueNameFn,
		middlewareFn: o.MiddlewareFn,
		errorFn:      o.ErrorFn,
	}
}

// Consume consumes messages for the specified handler
// The call is blocking until the supplied context is cancelled
func (c *Consumer) Consume(ctx context.Context, h Handler) error {
	ch, err := c.channelFn(c.conn)
	if err != nil {
		return err
	}

	q, err := c.queueNameFn(h.Message())
	if err != nil {
		return err
	}

	dc, err := ch.Consume(q, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	var hfn HandlerFunc = h.Handle
	if c.middlewareFn != nil {
		hfn = c.middlewareFn(hfn)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case d := <-dc:
			handleDelivery(ctx, d, h.Message(), hfn, c.errorFn)
		}
	}
}

func handleDelivery(ctx context.Context, d amqp.Delivery, m proto.Message, hfn HandlerFunc, efn func(Metadata, error)) {
	md := Metadata{
		ID:            d.MessageId,
		Type:          d.Type,
		ContentType:   d.ContentType,
		CorrelationID: d.CorrelationId,
		Timestamp:     d.Timestamp,
		Headers:       d.Headers,
	}

	err := proto.Unmarshal(d.Body, m)
	if err != nil {
		efn(md, err)

		if err = d.Nack(false, false); err != nil {
			efn(md, err)
		}

		return
	}

	err = hfn(ctx, m, md)
	if err != nil {
		efn(md, err)

		if err = d.Nack(false, false); err != nil {
			efn(md, err)
		}

		return
	}

	if err = d.Ack(false); err != nil {
		efn(md, err)
	}
}
