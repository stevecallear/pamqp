// Code generated by MockGen. DO NOT EDIT.
// Source: ./pamqp.go

// Package mocks is a generated GoMock package.
package mocks

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	amqp "github.com/streadway/amqp"
)

// MockChannel is a mock of Channel interface.
type MockChannel struct {
	ctrl     *gomock.Controller
	recorder *MockChannelMockRecorder
}

// MockChannelMockRecorder is the mock recorder for MockChannel.
type MockChannelMockRecorder struct {
	mock *MockChannel
}

// NewMockChannel creates a new mock instance.
func NewMockChannel(ctrl *gomock.Controller) *MockChannel {
	mock := &MockChannel{ctrl: ctrl}
	mock.recorder = &MockChannelMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockChannel) EXPECT() *MockChannelMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *MockChannel) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockChannelMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockChannel)(nil).Close))
}

// Consume mocks base method.
func (m *MockChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Consume", queue, consumer, autoAck, exclusive, noLocal, noWait, args)
	ret0, _ := ret[0].(<-chan amqp.Delivery)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Consume indicates an expected call of Consume.
func (mr *MockChannelMockRecorder) Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Consume", reflect.TypeOf((*MockChannel)(nil).Consume), queue, consumer, autoAck, exclusive, noLocal, noWait, args)
}

// ExchangeDeclare mocks base method.
func (m *MockChannel) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ExchangeDeclare", name, kind, durable, autoDelete, internal, noWait, args)
	ret0, _ := ret[0].(error)
	return ret0
}

// ExchangeDeclare indicates an expected call of ExchangeDeclare.
func (mr *MockChannelMockRecorder) ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ExchangeDeclare", reflect.TypeOf((*MockChannel)(nil).ExchangeDeclare), name, kind, durable, autoDelete, internal, noWait, args)
}

// Publish mocks base method.
func (m *MockChannel) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Publish", exchange, key, mandatory, immediate, msg)
	ret0, _ := ret[0].(error)
	return ret0
}

// Publish indicates an expected call of Publish.
func (mr *MockChannelMockRecorder) Publish(exchange, key, mandatory, immediate, msg interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Publish", reflect.TypeOf((*MockChannel)(nil).Publish), exchange, key, mandatory, immediate, msg)
}

// QueueBind mocks base method.
func (m *MockChannel) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "QueueBind", name, key, exchange, noWait, args)
	ret0, _ := ret[0].(error)
	return ret0
}

// QueueBind indicates an expected call of QueueBind.
func (mr *MockChannelMockRecorder) QueueBind(name, key, exchange, noWait, args interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueueBind", reflect.TypeOf((*MockChannel)(nil).QueueBind), name, key, exchange, noWait, args)
}

// QueueDeclare mocks base method.
func (m *MockChannel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "QueueDeclare", name, durable, autoDelete, exclusive, noWait, args)
	ret0, _ := ret[0].(amqp.Queue)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueueDeclare indicates an expected call of QueueDeclare.
func (mr *MockChannelMockRecorder) QueueDeclare(name, durable, autoDelete, exclusive, noWait, args interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueueDeclare", reflect.TypeOf((*MockChannel)(nil).QueueDeclare), name, durable, autoDelete, exclusive, noWait, args)
}
