// Package bus provides the messaging abstraction for aegis-swarm.
// Events are fanned out to topic subscribers either in-process
// (InMemoryBus, air-gapped test mode) or over a NATS server.
package bus

import (
	"encoding/json"
	"errors"
	"sync"

	"github.com/nats-io/nats.go"

	"aegis-swarm/internal/types"
)

// Bus is the publish/subscribe contract shared by all components.
type Bus interface {
	Publish(ev types.Event) error
	Subscribe(topic string, handler func(types.Event)) (unsubscribe func(), err error)
	Close() error
}

// ErrClosed is returned when operating on a closed in-memory bus.
var ErrClosed = errors.New("bus: closed")

// InMemoryBus is a goroutine-safe, in-process fan-out pub/sub Bus.
type InMemoryBus struct {
	mu     sync.RWMutex
	subs   map[string]map[int]func(types.Event)
	nextID int
	closed bool
}

// NewInMemory returns a ready-to-use in-memory bus.
func NewInMemory() *InMemoryBus {
	return &InMemoryBus{subs: make(map[string]map[int]func(types.Event))}
}

// Publish delivers ev to every subscriber of ev.Topic. Handlers run in
// their own goroutines so a slow or panicking handler never blocks a
// publisher.
func (b *InMemoryBus) Publish(ev types.Event) error {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return ErrClosed
	}
	var handlers []func(types.Event)
	for _, h := range b.subs[ev.Topic] {
		handlers = append(handlers, h)
	}
	b.mu.RUnlock()
	for _, h := range handlers {
		h := h
		go func() {
			defer func() { _ = recover() }()
			h(ev)
		}()
	}
	return nil
}

// Subscribe registers handler for exact topic matches and returns an
// idempotent unsubscribe function.
func (b *InMemoryBus) Subscribe(topic string, handler func(types.Event)) (func(), error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, ErrClosed
	}
	id := b.nextID
	b.nextID++
	if b.subs[topic] == nil {
		b.subs[topic] = make(map[int]func(types.Event))
	}
	b.subs[topic][id] = handler
	var once sync.Once
	return func() {
		once.Do(func() {
			b.mu.Lock()
			delete(b.subs[topic], id)
			b.mu.Unlock()
		})
	}, nil
}

// Close shuts the bus down; subsequent Publish/Subscribe calls fail.
func (b *InMemoryBus) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	b.subs = make(map[string]map[int]func(types.Event))
	return nil
}

// NATSBus implements Bus over a NATS server. Each event is JSON-encoded
// and published on the subject equal to its topic.
type NATSBus struct {
	conn *nats.Conn
	mu   sync.Mutex
	subs []*nats.Subscription
}

// NewNATS connects to the NATS server at url (e.g. nats://127.0.0.1:4222).
func NewNATS(url string) (Bus, error) {
	conn, err := nats.Connect(url)
	if err != nil {
		return nil, err
	}
	return &NATSBus{conn: conn}, nil
}

// Publish JSON-encodes ev and publishes it on subject = ev.Topic.
func (b *NATSBus) Publish(ev types.Event) error {
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	return b.conn.Publish(ev.Topic, data)
}

// Subscribe wires a NATS subscription for topic to handler. Messages that
// fail to decode are dropped so publishers are never blocked.
func (b *NATSBus) Subscribe(topic string, handler func(types.Event)) (func(), error) {
	sub, err := b.conn.Subscribe(topic, func(m *nats.Msg) {
		var ev types.Event
		if err := json.Unmarshal(m.Data, &ev); err != nil {
			return
		}
		handler(ev)
	})
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	b.subs = append(b.subs, sub)
	b.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() { _ = sub.Unsubscribe() })
	}, nil
}

// Close unsubscribes all subscriptions and closes the NATS connection.
func (b *NATSBus) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, s := range b.subs {
		_ = s.Unsubscribe()
	}
	b.subs = nil
	b.conn.Close()
	return nil
}
