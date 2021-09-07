package pamqp_test

import (
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/streadway/amqp"
	"google.golang.org/protobuf/proto"

	"github.com/stevecallear/pamqp"
	"github.com/stevecallear/pamqp/internal/mocks"
	"github.com/stevecallear/pamqp/internal/proto/testpb"
)

func TestNewRegistry(t *testing.T) {
	tests := []struct {
		name string
		chFn func(*amqp.Connection) (pamqp.Channel, error)
		err  bool
	}{
		{
			name: "should return an error if the channel cannot be created",
			chFn: func(*amqp.Connection) (pamqp.Channel, error) {
				return nil, errors.New("error")
			},
			err: true,
		},
		{
			name: "should not return an error if the channel can be created",
			chFn: func(*amqp.Connection) (pamqp.Channel, error) {
				return nil, nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := pamqp.NewRegistry(nil, func(o *pamqp.RegistryOptions) {
				o.ChannelFn = tt.chFn
			})

			assertErrorExists(t, err, tt.err)
		})
	}
}

func TestRegistry_Exchange(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*mocks.MockChannelMockRecorder)
		optFn func(*pamqp.RegistryOptions)
		exp   string
		err   bool
	}{
		{
			name: "should return exchange declare errors",
			setup: func(m *mocks.MockChannelMockRecorder) {
				m.ExchangeDeclare(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(errors.New("error")).Times(1)
			},
			optFn: func(*pamqp.RegistryOptions) {},
			err:   true,
		},
		{
			name: "should return the default exchange name",
			setup: func(m *mocks.MockChannelMockRecorder) {
				m.ExchangeDeclare("test.message", amqp.ExchangeFanout, true, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(nil).Times(1)
			},
			optFn: func(*pamqp.RegistryOptions) {},
			exp:   "test.message",
		},
		{
			name: "should use the exchange name option func",
			setup: func(m *mocks.MockChannelMockRecorder) {
				m.ExchangeDeclare("expected", amqp.ExchangeFanout, true, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(nil).Times(1)
			},
			optFn: func(o *pamqp.RegistryOptions) {
				o.ExchangeNameFn = func(proto.Message) string {
					return "expected"
				}
			},
			exp: "expected",
		},
	}

	for _, tt := range tests {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ch := mocks.NewMockChannel(ctrl)
		tt.setup(ch.EXPECT())

		sut, err := pamqp.NewRegistry(nil, tt.optFn, func(o *pamqp.RegistryOptions) {
			o.ChannelFn = func(*amqp.Connection) (pamqp.Channel, error) {
				return ch, nil
			}
		})

		assertErrorExists(t, err, false)
		if err != nil {
			return
		}

		act, err := sut.Exchange(new(testpb.Message))
		assertErrorExists(t, err, tt.err)

		if act != tt.exp {
			t.Errorf("got %s, expected %s", act, tt.exp)
		}
	}
}

func TestRegistry_Queue(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*pamqp.Registry, *mocks.MockChannelMockRecorder)
		optFn func(*pamqp.RegistryOptions)
		exp   string
		err   bool
	}{
		{
			name: "should return dead letter exchange create errors",
			setup: func(_ *pamqp.Registry, m *mocks.MockChannelMockRecorder) {
				m.ExchangeDeclare(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(errors.New("error")).Times(1)
			},
			optFn: func(*pamqp.RegistryOptions) {},
			err:   true,
		},
		{
			name: "should return dead letter queue create errors",
			setup: func(_ *pamqp.Registry, m *mocks.MockChannelMockRecorder) {
				gomock.InOrder(
					m.ExchangeDeclare(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
						Return(nil).Times(1),

					m.QueueDeclare(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
						Return(amqp.Queue{}, errors.New("error")).Times(1),
				)
			},
			optFn: func(*pamqp.RegistryOptions) {},
			err:   true,
		},
		{
			name: "should return dead letter queue bind errors",
			setup: func(_ *pamqp.Registry, m *mocks.MockChannelMockRecorder) {
				gomock.InOrder(
					m.ExchangeDeclare(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
						Return(nil).Times(1),

					m.QueueDeclare(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
						Return(amqp.Queue{Name: "test.message_dlq"}, nil).Times(1),

					m.QueueBind(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
						Return(errors.New("error")).Times(1),
				)
			},
			optFn: func(*pamqp.RegistryOptions) {},
			err:   true,
		},
		{
			name: "should return exchange declare errors",
			setup: func(_ *pamqp.Registry, m *mocks.MockChannelMockRecorder) {
				gomock.InOrder(
					m.ExchangeDeclare(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
						Return(nil).Times(1),

					m.QueueDeclare(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
						Return(amqp.Queue{Name: "test.message_dlq"}, nil).Times(1),

					m.QueueBind(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
						Return(nil).Times(1),

					m.ExchangeDeclare(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
						Return(errors.New("error")).Times(1),
				)
			},
			optFn: func(*pamqp.RegistryOptions) {},
			err:   true,
		},
		{
			name: "should return queue declare errors",
			setup: func(_ *pamqp.Registry, m *mocks.MockChannelMockRecorder) {
				gomock.InOrder(
					m.ExchangeDeclare(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
						Return(nil).Times(1),

					m.QueueDeclare(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
						Return(amqp.Queue{Name: "test.message_dlq"}, nil).Times(1),

					m.QueueBind(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
						Return(nil).Times(1),

					m.ExchangeDeclare(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
						Return(nil).Times(1),

					m.QueueDeclare(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
						Return(amqp.Queue{}, errors.New("error")).Times(1),
				)
			},
			optFn: func(*pamqp.RegistryOptions) {},
			err:   true,
		},
		{
			name: "should return queue bind errors",
			setup: func(_ *pamqp.Registry, m *mocks.MockChannelMockRecorder) {
				gomock.InOrder(
					m.ExchangeDeclare(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
						Return(nil).Times(1),

					m.QueueDeclare(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
						Return(amqp.Queue{Name: "test.message_dlq"}, nil).Times(1),

					m.QueueBind(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
						Return(nil).Times(1),

					m.ExchangeDeclare(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
						Return(nil).Times(1),

					m.QueueDeclare(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
						Return(amqp.Queue{}, nil).Times(1),

					m.QueueBind(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
						Return(errors.New("error")).Times(1),
				)
			},
			optFn: func(*pamqp.RegistryOptions) {},
			err:   true,
		},
		{
			name: "should return the default queue name",
			setup: func(_ *pamqp.Registry, m *mocks.MockChannelMockRecorder) {
				gomock.InOrder(
					m.ExchangeDeclare("default_dlx", amqp.ExchangeDirect, true, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
						Return(nil).Times(1),

					m.QueueDeclare("test.message_dlq", true, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
						Return(amqp.Queue{Name: "test.message_dlq"}, nil).Times(1),

					m.QueueBind("test.message_dlq", "test.message", gomock.Any(), gomock.Any(), gomock.Any()).
						Return(nil).Times(1),

					m.ExchangeDeclare("test.message", amqp.ExchangeFanout, true, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
						Return(nil).Times(1),

					m.QueueDeclare("test.message", true, gomock.Any(), gomock.Any(), gomock.Any(), amqp.Table{
						"x-dead-letter-exchange":    "default_dlx",
						"x-dead-letter-routing-key": "test.message",
					}).Return(amqp.Queue{Name: "test.message"}, nil).Times(1),

					m.QueueBind("test.message", "", gomock.Any(), gomock.Any(), gomock.Any()).
						Return(nil).Times(1),
				)
			},
			optFn: func(*pamqp.RegistryOptions) {},
			exp:   "test.message",
		},
		{
			name: "should use the dead letter name funcs",
			setup: func(_ *pamqp.Registry, m *mocks.MockChannelMockRecorder) {
				gomock.InOrder(
					m.ExchangeDeclare("expected_dlx", amqp.ExchangeDirect, true, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
						Return(nil).Times(1),

					m.QueueDeclare("expected_dlq", true, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
						Return(amqp.Queue{Name: "expected_dlq"}, nil).Times(1),

					m.QueueBind("expected_dlq", "expected_dlk", gomock.Any(), gomock.Any(), gomock.Any()).
						Return(nil).Times(1),

					m.ExchangeDeclare(gomock.Any(), amqp.ExchangeFanout, true, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
						Return(nil).Times(1),

					m.QueueDeclare(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
						Return(amqp.Queue{Name: "test.message"}, nil).Times(1),

					m.QueueBind(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
						Return(nil).Times(1),
				)
			},
			optFn: func(o *pamqp.RegistryOptions) {
				o.DeadLetterExchangeNameFn = func(proto.Message) string {
					return "expected_dlx"
				}
				o.DeadLetterQueueNameFn = func(proto.Message) string {
					return "expected_dlq"
				}
				o.DeadLetterKeyFn = func(proto.Message) string {
					return "expected_dlk"
				}
			},
			exp: "test.message",
		},
		{
			name: "should use the queue name option func",
			setup: func(_ *pamqp.Registry, m *mocks.MockChannelMockRecorder) {
				gomock.InOrder(
					m.ExchangeDeclare(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
						Return(nil).Times(1),

					m.QueueDeclare(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
						Return(amqp.Queue{Name: "test.message_dlq"}, nil).Times(1),

					m.QueueBind(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
						Return(nil).Times(1),

					m.ExchangeDeclare(gomock.Any(), amqp.ExchangeFanout, true, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
						Return(nil).Times(1),

					m.QueueDeclare("expected", true, gomock.Any(), gomock.Any(), gomock.Any(), amqp.Table{
						"x-dead-letter-exchange":    "default_dlx",
						"x-dead-letter-routing-key": "test.message",
					}).Return(amqp.Queue{Name: "expected"}, nil).Times(1),

					m.QueueBind("expected", "", gomock.Any(), gomock.Any(), gomock.Any()).
						Return(nil).Times(1),
				)
			},
			optFn: func(o *pamqp.RegistryOptions) {
				o.QueueNameFn = func(proto.Message) string {
					return "expected"
				}
			},
			exp: "expected",
		},
		{
			name: "should utilise the cache",
			setup: func(r *pamqp.Registry, m *mocks.MockChannelMockRecorder) {
				gomock.InOrder(
					m.ExchangeDeclare(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
						Return(nil).Times(1),

					m.QueueDeclare(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
						Return(amqp.Queue{Name: "test.message_dlq"}, nil).Times(1),

					m.QueueBind(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
						Return(nil).Times(1),

					m.ExchangeDeclare(gomock.Any(), amqp.ExchangeFanout, true, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
						Return(nil).Times(1),

					m.QueueDeclare(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
						Return(amqp.Queue{Name: "test.message"}, nil).Times(1),

					m.QueueBind(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
						Return(nil).Times(1),
				)

				_, err := r.Queue(new(testpb.Message))
				if err != nil {
					panic(err)
				}
			},
			optFn: func(*pamqp.RegistryOptions) {},
			exp:   "test.message",
		},
	}

	for _, tt := range tests {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ch := mocks.NewMockChannel(ctrl)
		sut, err := pamqp.NewRegistry(nil, tt.optFn, func(o *pamqp.RegistryOptions) {
			o.ChannelFn = func(*amqp.Connection) (pamqp.Channel, error) {
				return ch, nil
			}
		})

		tt.setup(sut, ch.EXPECT())

		assertErrorExists(t, err, false)
		if err != nil {
			return
		}

		act, err := sut.Queue(new(testpb.Message))
		assertErrorExists(t, err, tt.err)

		if act != tt.exp {
			t.Errorf("got %s, expected %s", act, tt.exp)
		}
	}
}

func TestRegistry_Close(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*mocks.MockChannelMockRecorder)
		err   bool
	}{
		{
			name: "should return channel close errors",
			setup: func(m *mocks.MockChannelMockRecorder) {
				m.Close().Return(errors.New("error")).Times(1)
			},
			err: true,
		},
		{
			name: "should close the channel",
			setup: func(m *mocks.MockChannelMockRecorder) {
				m.Close().Return(nil).Times(1)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			ch := mocks.NewMockChannel(ctrl)
			tt.setup(ch.EXPECT())

			sut, err := pamqp.NewRegistry(nil, func(o *pamqp.RegistryOptions) {
				o.ChannelFn = func(*amqp.Connection) (pamqp.Channel, error) {
					return ch, nil
				}
			})

			assertErrorExists(t, err, false)
			if err != nil {
				return
			}

			err = sut.Close()
			assertErrorExists(t, err, tt.err)
		})
	}
}

func TestWithConsumerNaming(t *testing.T) {
	t.Run("should configure the options", func(t *testing.T) {
		var o pamqp.RegistryOptions
		pamqp.WithConsumerNaming("consumer")(&o)

		msg := new(testpb.Message)

		if act, exp := o.ExchangeNameFn(msg), "test.message"; act != exp {
			t.Errorf("got %s, expected %s", act, exp)
		}

		if act, exp := o.QueueNameFn(msg), "consumer.test.message"; act != exp {
			t.Errorf("got %s, expected %s", act, exp)
		}

		if act, exp := o.DeadLetterExchangeNameFn(msg), "default_dlx"; act != exp {
			t.Errorf("got %s, expected %s", act, exp)
		}

		if act, exp := o.DeadLetterQueueNameFn(msg), "consumer.test.message_dlq"; act != exp {
			t.Errorf("got %s, expected %s", act, exp)
		}

		if act, exp := o.DeadLetterKeyFn(msg), "consumer.test.message"; act != exp {
			t.Errorf("got %s, expected %s", act, exp)
		}
	})

	t.Run("should configure the options with prefixes", func(t *testing.T) {
		var o pamqp.RegistryOptions
		pamqp.WithConsumerNaming("consumer", "p1", "p2")(&o)

		msg := new(testpb.Message)

		if act, exp := o.ExchangeNameFn(msg), "p1.p2.test.message"; act != exp {
			t.Errorf("got %s, expected %s", act, exp)
		}

		if act, exp := o.QueueNameFn(msg), "p1.p2.consumer.test.message"; act != exp {
			t.Errorf("got %s, expected %s", act, exp)
		}

		if act, exp := o.DeadLetterExchangeNameFn(msg), "p1.p2.default_dlx"; act != exp {
			t.Errorf("got %s, expected %s", act, exp)
		}

		if act, exp := o.DeadLetterQueueNameFn(msg), "p1.p2.consumer.test.message_dlq"; act != exp {
			t.Errorf("got %s, expected %s", act, exp)
		}

		if act, exp := o.DeadLetterKeyFn(msg), "p1.p2.consumer.test.message"; act != exp {
			t.Errorf("got %s, expected %s", act, exp)
		}
	})
}
