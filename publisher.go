package pamqp

import (
	"context"

	"github.com/streadway/amqp"
	"google.golang.org/protobuf/proto"
)

// Publisher represents a message publisher
type Publisher struct {
	channel        Channel
	exchangeNameFn func(proto.Message) (string, error)
	middlewareFn   MiddlewareFunc
}

// NewPublisher returns a new publisher
func NewPublisher(c *amqp.Connection, optFns ...func(*Options)) (*Publisher, error) {
	o := defaultOptions
	for _, fn := range optFns {
		fn(&o)
	}

	ch, err := o.ChannelFn(c)
	if err != nil {
		return nil, err
	}

	return &Publisher{
		channel:        ch,
		exchangeNameFn: o.ExchangeNameFn,
		middlewareFn:   o.MiddlewareFn,
	}, nil
}

// Publish publishes the specified message
func (p *Publisher) Publish(ctx context.Context, m proto.Message, mdFns ...func(*Metadata)) error {
	md := Metadata{
		ID:          NewID(),
		Type:        MessageName(m),
		ContentType: "application/protobuf",
		Timestamp:   UTCNow(),
		Headers:     map[string]interface{}{},
	}

	for _, fn := range mdFns {
		fn(&md)
	}

	hfn := p.handlePublish
	if p.middlewareFn != nil {
		hfn = p.middlewareFn(hfn)
	}

	return hfn(ctx, m, md)
}

// Close closes the underlying channel
func (p *Publisher) Close() error {
	return p.channel.Close()
}

func (p *Publisher) handlePublish(ctx context.Context, m proto.Message, md Metadata) error {
	e, err := p.exchangeNameFn(m)
	if err != nil {
		return err
	}

	b, err := proto.Marshal(m)
	if err != nil {
		return err
	}

	return p.channel.Publish(e, "", false, false, amqp.Publishing{
		Headers:       md.Headers,
		Body:          b,
		ContentType:   md.ContentType,
		MessageId:     md.ID,
		CorrelationId: md.CorrelationID,
		Type:          md.Type,
		Timestamp:     md.Timestamp,
	})
}
