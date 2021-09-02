package pamqp_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/streadway/amqp"
	"google.golang.org/protobuf/proto"

	"github.com/stevecallear/pamqp"
	"github.com/stevecallear/pamqp/internal/mocks"
	"github.com/stevecallear/pamqp/internal/proto/testpb"
)

func TestConsumer_Consume(t *testing.T) {
	deliveries := make(chan amqp.Delivery)
	defer func() {
		close(deliveries)
	}()

	tests := []struct {
		name       string
		setup      func(*mocks.MockChannelMockRecorder, *mocks.MockAcknowledgerMockRecorder)
		optFn      func(*pamqp.Options)
		handler    *handler
		deliveryFn func(*amqp.Delivery)
		errCount   int32
	}{
		{
			name:  "should return an error if the channel cannot be created",
			setup: func(*mocks.MockChannelMockRecorder, *mocks.MockAcknowledgerMockRecorder) {},
			optFn: func(o *pamqp.Options) {
				o.ChannelFn = func(*amqp.Connection) (pamqp.Channel, error) {
					return nil, errors.New("error")
				}
			},
			errCount: 1, // channel
		},
		{
			name:  "should return an error if the queue name cannot be resolved",
			setup: func(*mocks.MockChannelMockRecorder, *mocks.MockAcknowledgerMockRecorder) {},
			optFn: func(o *pamqp.Options) {
				o.QueueNameFn = func(proto.Message) (string, error) {
					return "", errors.New("error")
				}
			},
			errCount: 1, // queue
		},
		{
			name: "should return consume errors",
			setup: func(c *mocks.MockChannelMockRecorder, _ *mocks.MockAcknowledgerMockRecorder) {
				c.Consume(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(nil, errors.New("error"))
			},
			optFn:    func(*pamqp.Options) {},
			errCount: 1, // consume
		},
		{
			name: "should send nack errors on invalid request body",
			setup: func(r *mocks.MockChannelMockRecorder, a *mocks.MockAcknowledgerMockRecorder) {
				r.Consume(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(deliveries, nil).Times(1)

				a.Nack(gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("error"))
			},
			optFn: func(*pamqp.Options) {},
			handler: &handler{
				handleFn: func(ctx context.Context, m proto.Message, md pamqp.Metadata) error {
					return nil
				},
			},
			deliveryFn: func(d *amqp.Delivery) {
				d.Body = []byte("invalid")
			},
			errCount: 2, // invalid body, nack
		},
		{
			name: "should nack on invalid request body",
			setup: func(r *mocks.MockChannelMockRecorder, a *mocks.MockAcknowledgerMockRecorder) {
				r.Consume(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(deliveries, nil).Times(1)

				a.Nack(gomock.Any(), false, false).Return(nil)
			},
			optFn: func(*pamqp.Options) {},
			handler: &handler{
				handleFn: func(ctx context.Context, m proto.Message, md pamqp.Metadata) error {
					return nil
				},
			},
			deliveryFn: func(d *amqp.Delivery) {
				d.Body = []byte("invalid")
			},
			errCount: 1, // nack
		},
		{
			name: "should send nack errors on handler error",
			setup: func(r *mocks.MockChannelMockRecorder, a *mocks.MockAcknowledgerMockRecorder) {
				r.Consume(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(deliveries, nil).Times(1)

				a.Nack(gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("error"))
			},
			optFn: func(*pamqp.Options) {},
			handler: &handler{
				handleFn: func(ctx context.Context, m proto.Message, md pamqp.Metadata) error {
					return errors.New("error")
				},
			},
			deliveryFn: func(d *amqp.Delivery) {
				d.Body = marshal(new(testpb.Message))
			},
			errCount: 2, // handler, nack
		},
		{
			name: "should nack on handler error",
			setup: func(r *mocks.MockChannelMockRecorder, a *mocks.MockAcknowledgerMockRecorder) {
				r.Consume(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(deliveries, nil).Times(1)

				a.Nack(gomock.Any(), false, false).Return(nil)
			},
			optFn: func(*pamqp.Options) {},
			handler: &handler{
				handleFn: func(ctx context.Context, m proto.Message, md pamqp.Metadata) error {
					return errors.New("error")
				},
			},
			deliveryFn: func(d *amqp.Delivery) {
				d.Body = marshal(new(testpb.Message))
			},
			errCount: 1, // handler
		},
		{
			name: "should send ack errors",
			setup: func(r *mocks.MockChannelMockRecorder, a *mocks.MockAcknowledgerMockRecorder) {
				r.Consume(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(deliveries, nil).Times(1)

				a.Ack(gomock.Any(), gomock.Any()).Return(errors.New("error"))
			},
			optFn: func(*pamqp.Options) {},
			handler: &handler{
				handleFn: func(ctx context.Context, m proto.Message, md pamqp.Metadata) error {
					return nil
				},
			},
			deliveryFn: func(d *amqp.Delivery) {
				d.Body = marshal(new(testpb.Message))
			},
			errCount: 1, // ack
		},
		{
			name: "should ack handled messages",
			setup: func(r *mocks.MockChannelMockRecorder, a *mocks.MockAcknowledgerMockRecorder) {
				r.Consume("test-message", gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(deliveries, nil).Times(1)

				a.Ack(gomock.Any(), false).Return(nil)
			},
			optFn: func(*pamqp.Options) {},
			handler: &handler{
				handleFn: func(ctx context.Context, m proto.Message, md pamqp.Metadata) error {
					pm := m.(*testpb.Message)
					if pm.Value != "expected" {
						return errors.New("incorrect message")
					}
					return nil
				},
			},
			deliveryFn: func(d *amqp.Delivery) {
				d.Body = marshal(&testpb.Message{Value: "expected"})
			},
		},
		{
			name: "should use the supplied middleware",
			setup: func(r *mocks.MockChannelMockRecorder, a *mocks.MockAcknowledgerMockRecorder) {
				r.Consume(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(deliveries, nil).Times(1)

				a.Nack(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
			},
			optFn: func(o *pamqp.Options) {
				o.MiddlewareFn = func(n pamqp.HandlerFunc) pamqp.HandlerFunc {
					return func(ctx context.Context, m proto.Message, md pamqp.Metadata) error {
						return errors.New("error")
					}
				}
			},
			handler: &handler{
				handleFn: func(ctx context.Context, m proto.Message, md pamqp.Metadata) error {
					return nil
				},
			},
			deliveryFn: func(d *amqp.Delivery) {
				d.Body = marshal(new(testpb.Message))
			},
			errCount: 1, // middleware
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			ch := mocks.NewMockChannel(ctrl)
			ack := mocks.NewMockAcknowledger(ctrl)
			tt.setup(ch.EXPECT(), ack.EXPECT())

			var errCount int32
			sut := pamqp.NewConsumer(nil, func(o *pamqp.Options) {
				o.ChannelFn = func(*amqp.Connection) (pamqp.Channel, error) {
					return ch, nil
				}
				o.ErrorFn = func(_ pamqp.Metadata, err error) {
					if err != nil {
						atomic.AddInt32(&errCount, 1)
					}
				}
			}, tt.optFn)

			ctx, cancel := context.WithCancel(context.Background())

			wg := new(sync.WaitGroup)
			wg.Add(1)

			go func() {
				defer wg.Done()
				if err := sut.Consume(ctx, tt.handler); err != nil {
					atomic.AddInt32(&errCount, 1)
				}
			}()

			time.Sleep(10 * time.Millisecond) // wait for consume errors

			if atomic.LoadInt32(&errCount) < 1 {
				d := amqp.Delivery{Acknowledger: ack}
				tt.deliveryFn(&d)

				deliveries <- d
			}

			cancel()
			wg.Wait()

			if errCount != tt.errCount {
				t.Errorf("got %d errors, expected %d", errCount, tt.errCount)
			}
		})
	}
}

type handler struct {
	handleFn pamqp.HandlerFunc
}

func (h *handler) Message() proto.Message {
	return new(testpb.Message)
}

func (h *handler) Handle(ctx context.Context, m proto.Message, md pamqp.Metadata) error {
	return h.handleFn(ctx, m, md)
}
