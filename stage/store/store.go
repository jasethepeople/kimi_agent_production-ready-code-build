// Package store provides the state/queue abstraction for aegis-swarm.
// Task queues, results, dead-end sets, and job state are held either
// in-process (InMemoryStore, air-gapped test mode) or in Redis.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"aegis-swarm/internal/types"
)

// dequeuePoll caps how long Dequeue blocks waiting for work.
const dequeuePoll = time.Second

// Store is the shared persistence contract for tasks, results, dead-ends,
// and per-job state.
type Store interface {
	Enqueue(t types.Task) error
	Dequeue(jobID string) (types.Task, bool, error)
	RecordResult(r types.Result) error
	AddDeadEnd(jobID, candidate string) error
	DeadEndCount(jobID string) (int, error)
	GetState(jobID string) (types.JobState, error)
	PutState(s types.JobState) error
	Close() error
}

// ErrClosed is returned when operating on a closed in-memory store.
var ErrClosed = errors.New("store: closed")

// InMemoryStore is a mutex-protected in-process Store.
type InMemoryStore struct {
	mu       sync.Mutex
	queues   map[string][]types.Task
	results  []types.Result
	deadEnds map[string]map[string]struct{}
	states   map[string]types.JobState
	closed   bool
	// signal wakes blocked Dequeue callers when new work arrives.
	signal chan struct{}
}

// NewInMemory returns a ready-to-use in-memory store.
func NewInMemory() *InMemoryStore {
	return &InMemoryStore{
		queues:   make(map[string][]types.Task),
		deadEnds: make(map[string]map[string]struct{}),
		states:   make(map[string]types.JobState),
		signal:   make(chan struct{}, 1),
	}
}

// Enqueue appends t to the FIFO queue for t.JobID and wakes a waiter.
func (s *InMemoryStore) Enqueue(t types.Task) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	s.queues[t.JobID] = append(s.queues[t.JobID], t)
	s.mu.Unlock()
	select {
	case s.signal <- struct{}{}:
	default:
	}
	return nil
}

// Dequeue pops the oldest queued task for jobID, blocking up to ~1s for
// work to appear. It returns ok=false when the queue is empty.
func (s *InMemoryStore) Dequeue(jobID string) (types.Task, bool, error) {
	deadline := time.Now().Add(dequeuePoll)
	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return types.Task{}, false, ErrClosed
		}
		if q := s.queues[jobID]; len(q) > 0 {
			t := q[0]
			s.queues[jobID] = q[1:]
			s.mu.Unlock()
			return t, true, nil
		}
		s.mu.Unlock()

		remaining := time.Until(deadline)
		if remaining <= 0 {
			return types.Task{}, false, nil
		}
		wait := remaining
		if wait > 50*time.Millisecond {
			wait = 50 * time.Millisecond
		}
		timer := time.NewTimer(wait)
		select {
		case <-s.signal:
			timer.Stop()
		case <-timer.C:
		}
	}
}

// RecordResult appends r to the results log.
func (s *InMemoryStore) RecordResult(r types.Result) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	s.results = append(s.results, r)
	return nil
}

// AddDeadEnd records candidate in jobID's dead-end set (deduplicated).
func (s *InMemoryStore) AddDeadEnd(jobID, candidate string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if s.deadEnds[jobID] == nil {
		s.deadEnds[jobID] = make(map[string]struct{})
	}
	s.deadEnds[jobID][candidate] = struct{}{}
	return nil
}

// DeadEndCount returns the number of distinct dead-end candidates for jobID.
func (s *InMemoryStore) DeadEndCount(jobID string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, ErrClosed
	}
	return len(s.deadEnds[jobID]), nil
}

// GetState returns the stored JobState for jobID (zero value if unset).
func (s *InMemoryStore) GetState(jobID string) (types.JobState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return types.JobState{}, ErrClosed
	}
	return s.states[jobID], nil
}

// PutState stores st keyed by st.JobID.
func (s *InMemoryStore) PutState(st types.JobState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	s.states[st.JobID] = st
	return nil
}

// Close marks the store closed; further operations fail.
func (s *InMemoryStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

// RedisStore implements Store against a Redis server.
// Key layout (per job id):
//
//	job:{id}:queue      LIST   LPUSH/BRPOP (FIFO)
//	job:{id}:results    LIST   JSON-encoded results
//	job:{id}:dead_ends  SET    SADD/SCARD (dedup)
//	job:{id}:state      HASH   HSET/HGETALL
type RedisStore struct {
	client *redis.Client
	ctx    context.Context
}

// NewRedis connects to the Redis server at addr (e.g. "127.0.0.1:6379").
func NewRedis(addr string) (Store, error) {
	client := redis.NewClient(&redis.Options{Addr: addr})
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("store: redis ping %s: %w", addr, err)
	}
	return &RedisStore{client: client, ctx: ctx}, nil
}

func queueKey(jobID string) string    { return "job:" + jobID + ":queue" }
func resultsKey(jobID string) string  { return "job:" + jobID + ":results" }
func deadEndsKey(jobID string) string { return "job:" + jobID + ":dead_ends" }
func stateKey(jobID string) string    { return "job:" + jobID + ":state" }

// Enqueue LPUSHes the JSON-encoded task onto job:{t.JobID}:queue.
func (s *RedisStore) Enqueue(t types.Task) error {
	data, err := json.Marshal(t)
	if err != nil {
		return err
	}
	return s.client.LPush(s.ctx, queueKey(t.JobID), data).Err()
}

// Dequeue BRPOPs from job:{jobID}:queue with a 1s timeout; returns
// ok=false when the queue is empty.
func (s *RedisStore) Dequeue(jobID string) (types.Task, bool, error) {
	vals, err := s.client.BRPop(s.ctx, dequeuePoll, queueKey(jobID)).Result()
	if errors.Is(err, redis.Nil) {
		return types.Task{}, false, nil
	}
	if err != nil {
		return types.Task{}, false, err
	}
	if len(vals) < 2 {
		return types.Task{}, false, nil
	}
	var t types.Task
	if err := json.Unmarshal([]byte(vals[1]), &t); err != nil {
		return types.Task{}, false, err
	}
	return t, true, nil
}

// RecordResult RPushEs the JSON-encoded result onto job:{r.JobID}:results.
func (s *RedisStore) RecordResult(r types.Result) error {
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	return s.client.RPush(s.ctx, resultsKey(r.JobID), data).Err()
}

// AddDeadEnd SADDs candidate to job:{jobID}:dead_ends (deduplicated).
func (s *RedisStore) AddDeadEnd(jobID, candidate string) error {
	return s.client.SAdd(s.ctx, deadEndsKey(jobID), candidate).Err()
}

// DeadEndCount SCARDs job:{jobID}:dead_ends.
func (s *RedisStore) DeadEndCount(jobID string) (int, error) {
	n, err := s.client.SCard(s.ctx, deadEndsKey(jobID)).Result()
	return int(n), err
}

// GetState HGETALLs job:{jobID}:state into a JobState.
func (s *RedisStore) GetState(jobID string) (types.JobState, error) {
	m, err := s.client.HGetAll(s.ctx, stateKey(jobID)).Result()
	if err != nil {
		return types.JobState{}, err
	}
	var st types.JobState
	st.JobID = jobID
	st.Status = m["status"]
	st.Plaintext = m["plaintext"]
	if v, ok := m["total_attempts"]; ok {
		_, _ = fmt.Sscanf(v, "%d", &st.TotalAttempts)
	}
	if v, ok := m["dead_ends"]; ok {
		_, _ = fmt.Sscanf(v, "%d", &st.DeadEnds)
	}
	if v, ok := m["started_unix"]; ok {
		_, _ = fmt.Sscanf(v, "%d", &st.StartedUnix)
	}
	return st, nil
}

// PutState HSETs job:{st.JobID}:state.
func (s *RedisStore) PutState(st types.JobState) error {
	return s.client.HSet(s.ctx, stateKey(st.JobID), map[string]any{
		"status":         st.Status,
		"total_attempts": st.TotalAttempts,
		"dead_ends":      st.DeadEnds,
		"started_unix":   st.StartedUnix,
		"plaintext":      st.Plaintext,
	}).Err()
}

// Close closes the Redis client.
func (s *RedisStore) Close() error {
	return s.client.Close()
}
