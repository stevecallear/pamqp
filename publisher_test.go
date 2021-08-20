package pamqp_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/streadway/amqp"
	"google.golang.org/protobuf/proto"

	"github.com/stevecallear/pamqp"
	"github.com/stevecallear/pamqp/internal/mocks"
	"github.com/stevecallear/pamqp/internal/proto/testpb"
)

func TestPublisher_Publish(t *testing.T) {
	const id = "message-id"
	rid := setNewID(id)
	defer rid()

	now := time.Now().UTC()
	run := setUTCNow(now)
	defer run()

	msg := &testpb.Message{Value: "value"}

	tests := []struct {
		name     string
		setup    func(*mocks.MockChannelMockRecorder)
		optFn    func(*pamqp.Options)
		input    *testpb.Message
		mdFn     func(*pamqp.Metadata)
		errCount int
	}{
		{
			name:  "should return an error if the channel cannot be created",
			setup: func(m *mocks.MockChannelMockRecorder) {},
			optFn: func(o *pamqp.Options) {
				o.ChannelFn = func(*amqp.Connection) (pamqp.Channel, error) {
					return nil, errors.New("error")
				}
			},
			errCount: 1,
		},
		{
			name:  "should return an error if the exchange name cannot be resolved",
			setup: func(m *mocks.MockChannelMockRecorder) {},
			optFn: func(o *pamqp.Options) {
				o.ExchangeNameFn = func(proto.Message) (string, error) {
					return "", errors.New("error")
				}
			},
			mdFn:     func(*pamqp.Metadata) {},
			input:    msg,
			errCount: 1,
		},
		{
			name:  "should return publish errors",
			optFn: func(o *pamqp.Options) {},
			setup: func(m *mocks.MockChannelMockRecorder) {
				m.Publish(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(errors.New("error")).Times(1)
			},
			mdFn:     func(*pamqp.Metadata) {},
			input:    msg,
			errCount: 1,
		},
		{
			name:  "should publish the message",
			optFn: func(o *pamqp.Options) {},
			setup: func(m *mocks.MockChannelMockRecorder) {
				m.Publish(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), amqp.Publishing{
					Headers:       map[string]interface{}{},
					Body:          marshal(msg),
					ContentType:   "application/protobuf",
					MessageId:     id,
					CorrelationId: "",
					Type:          "test.Message",
					Timestamp:     now,
				}).Return(nil).Times(1)
			},
			mdFn:  func(*pamqp.Metadata) {},
			input: msg,
		},
		{
			name:  "should apply the metadata func",
			optFn: func(o *pamqp.Options) {},
			setup: func(m *mocks.MockChannelMockRecorder) {
				m.Publish(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), amqp.Publishing{
					Headers:       map[string]interface{}{},
					Body:          marshal(msg),
					ContentType:   "application/protobuf",
					MessageId:     id,
					CorrelationId: "correlation-id",
					Type:          "test.Message",
					Timestamp:     now,
				}).Return(nil).Times(1)
			},
			mdFn: func(md *pamqp.Metadata) {
				md.CorrelationID = "correlation-id"
			},
			input: msg,
		},
		{
			name: "should apply the middleware func",
			optFn: func(o *pamqp.Options) {
				o.MiddlewareFn = func(n pamqp.HandlerFunc) pamqp.HandlerFunc {
					return func(ctx context.Context, m proto.Message, md pamqp.Metadata) error {
						md.CorrelationID = "correlation-id"
						return n(ctx, m, md)
					}
				}
			},
			setup: func(m *mocks.MockChannelMockRecorder) {
				m.Publish(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), amqp.Publishing{
					Headers:       map[string]interface{}{},
					Body:          marshal(msg),
					ContentType:   "application/protobuf",
					MessageId:     id,
					CorrelationId: "correlation-id",
					Type:          "test.Message",
					Timestamp:     now,
				}).Return(nil).Times(1)
			},
			mdFn:  func(md *pamqp.Metadata) {},
			input: msg,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			ch := mocks.NewMockChannel(ctrl)
			tt.setup(ch.EXPECT())

			var errCount int

			sut, err := pamqp.NewPublisher(nil, func(o *pamqp.Options) {
				o.ChannelFn = func(*amqp.Connection) (pamqp.Channel, error) {
					return ch, nil
				}
			}, tt.optFn)

			if err != nil {
				errCount++
			} else if err = sut.Publish(context.Background(), tt.input, tt.mdFn); err != nil {
				errCount++
			}

			if errCount != tt.errCount {
				t.Errorf("got %d errors, expected %d", errCount, tt.errCount)
			}
		})
	}
}

func TestPublisher_Close(t *testing.T) {
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

			sut, err := pamqp.NewPublisher(nil, func(o *pamqp.Options) {
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
