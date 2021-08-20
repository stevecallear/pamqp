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

## Subscriber
`Subscriber` receives messages published to the appropriate queue. The target queue is resolved using `Options.QueueNameFn` and assumes the existence of a queue with the name format `package-message` by default. A `Registry` instance can be supplied to resolve/create infrastructure by convention.

A message subscription can be created by calling `Subscribe`. This operation is blocking until the supplied context is cancelled. A dedicated channel is opened for each call and is closed once the context has been cancelled. This allows a single subscriber to handle multiple message types using goroutines.

Each subscription expects a dedicated `Handler` implementation for the particular message type.
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

Calls to `Subscribe` will only return an error if infrastructure resolution fails. Handler errors will not result in an error being returned. Instead, `Options.ErrorFn` can be configured to log handler errors. By default handler errors are logged using the standard logger.

## Registry
`Registry` is responsible for the craetion of AMQP infrastructure by convention. By default the registry will create a fanout exchange and associated queue for each message type. This effectively results in each subscriber acting as a competing consumer.

To support more typical routing patterns, the registry should be configured to generate a separte queue for each subscribing service. This convention can be applied with `pamqp.WithPrefixNaming`.

### Example
Service 'a' publishes a message to the `dev-package-message` exchange. All instances of service 'a' will publish to the same exchange.
```
r, _ := pamqp.NewRegistry(conn, pamqp.WithPrefixNaming("dev", "a"))
p, _ := pamqp.NewPublisher(conn, pamqp.WithRegistry(r))

p.Publish(ctx, new(package.Message))
```

Service 'b' subscribes to published messages by creating a service-specific queue named `dev-package-message-b` and binding it to the exchange. All instances of service 'b' will act as competing consumers.
```
r, _ := pamqp.NewRegistry(conn, pamqp.WithPrefixNaming("dev", "b"))
s := pamqp.NewSubscriber(conn, pamqp.WithRegistry(r))

s.Subscribe(ctx, new(handler))
```

Service 'c' subscribes to the same exchange by creating a second service-specific queue named `dev-package-message-c`. This is bound to the same exchange, resulting in fanout behaviour, but with all instances of service 'c' acting as competing consumers.
```
r, _ := pamqp.NewRegistry(conn, pamqp.WithPrefixNaming("dev", "c"))
s := pamqp.NewSubscriber(conn, pamqp.WithRegistry(r))

s.Subscribe(ctx, new(handler))
```

## Middleware
Both `Publisher` and `Subscriber` allow middleware to be specified using `Options.MiddlewareFn`.
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

While the `MiddlewareFunc` signature is identical for both publishers and subscribers, it is rare that a single middleware function would be valid for both scenarios. Care should also be take to not swallow errors in subscriber middleware functions as this will result in the message being acked.