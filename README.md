# pamqp
[![Build Status](https://github.com/stevecallear/pamqp/actions/workflows/build.yml/badge.svg)](https://github.com/stevecallear/pamqp/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/stevecallear/pamqp/branch/master/graph/badge.svg)](https://codecov.io/gh/stevecallear/pamqp)
[![Go Report Card](https://goreportcard.com/badge/github.com/stevecallear/pamqp)](https://goreportcard.com/report/github.com/stevecallear/pamqp)

`pamqp` offers an opinionated AMQP pub/sub messaging framework using the excellent [amqp](https://github.com/streadway/amqp) package and Google Protobuf. It is fundamentally a simplified version of [pram](https://github.com/stevecallear/pram) for AMQP.

## Publisher
`Publisher` publishes messages to the appropriate exchange. The target exchange is resolved using `Options.ExchangeNameFn` and assumes the existence of an exchange with the name format `package-message` by default. A `Registry` instance can be supplied to resolve/create infrastructure by convention.

Messages are published using the `Publish` function, which takes an optional function to update the outgoing metadata.
```
p.Publish(ctx, new(testpb.Message), func(md *pamqp.Metadata) {
    md.CorrelationID = pamqp.NewID()
})
```

Each publisher opens a single AMQP channel that should be closed using the `Close` function.

## Consumer
`Consumer` receives messages published to the appropriate queue. The target queue is resolved using `Options.QueueNameFn` and assumes the existence of a queue with the name format `package-message` by default. A `Registry` instance can be supplied to resolve/create infrastructure by convention.

Messages are handled by calling `Consume`. This operation is blocking until the supplied context is cancelled. A dedicated channel is opened for each call and is closed once the context has been cancelled. This allows a single consumer to handle multiple message types using goroutines.

Each call to `Consume` expects a dedicated `Handler` implementation for the particular message type.
```
type handler struct{}

func (h *handler) Message() proto.Message {
	return new(testpb.Message)
}

func (h *handler) Handle(ctx context.Context, m proto.Message, md pamqp.Metadata) error {
	tm := m.(*testpb.Message)
	// handle message
	return nil
}
```

Calls to `Consume` will only return an error if infrastructure resolution fails. Handler errors will not result in an error being returned. Instead, `Options.ErrorFn` can be configured to log handler errors. By default handler errors are logged using the standard logger.

## Registry
`Registry` is responsible for the craetion of AMQP infrastructure by convention. By default the registry will create a fanout exchange and associated queue for each message type. This effectively results in each consumer acting as a competing consumer.

To support more typical routing patterns, the registry should be configured to generate a separate queue for each consuming service. This convention can be applied with `pamqp.WithConsumerNaming`.

### Example
Service 'a' publishes a message to the `package.message` exchange. All instances of service 'a' will publish to the same exchange.
```
r, _ := pamqp.NewRegistry(conn, pamqp.WithConsumerNaming("a"))
p, _ := pamqp.NewPublisher(conn, pamqp.WithRegistry(r))

p.Publish(ctx, new(package.Message))
```

Service 'b' consumes published messages by creating a service-specific queue named `b.package.message` and binding it to the exchange. All instances of service 'b' will act as competing consumers.
```
r, _ := pamqp.NewRegistry(conn, pamqp.WithConsumerNaming("b"))
c := pamqp.NewConsumer(conn, pamqp.WithRegistry(r))

c.Consume(ctx, new(handler))
```

Service 'c' consumes messages from the same exchange by creating a second service-specific queue named `c.package.message`. This is bound to the same exchange, resulting in fanout behaviour, but with all instances of service 'c' acting as competing consumers.
```
r, _ := pamqp.NewRegistry(conn, pamqp.WithConsumerNaming("c"))
c := pamqp.NewConsumer(conn, pamqp.WithRegistry(r))

c.Consume(ctx, new(handler))
```

## Middleware
Both `Publisher` and `Consumer` allow middleware to be specified using `Options.MiddlewareFn`.
```
func publishLogging(n pamqp.HandlerFunc) pamqp.HandlerFunc {
	return func(ctx context.Context, m proto.Message, md pamqp.Metadata) error {
		if err := n(ctx, m, md); err != nil {
			return err
		}

		log.Printf("message published: %s", md.ID)
		return nil
	}
}
```
```
p, _ := pamqp.NewPublisher(conn, pamqp.WithRegistry(r), pamqp.WithMiddleware(publishLogging))
```

While the `MiddlewareFunc` signature is identical for both publishers and consumers, it is rare that a single middleware function would be valid for both scenarios. Care should also be take to not swallow errors in consumer middleware functions as this will result in the message being acked.