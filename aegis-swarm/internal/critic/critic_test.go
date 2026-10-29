package critic

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"aegis-swarm/internal/types"
)

// --- minimal local fakes implementing store.Store / bus.Bus ---

type fakeStore struct {
	mu     sync.Mutex
	state  types.JobState
	deadLn []string
}

func (f *fakeStore) Enqueue(types.Task) error                 { return nil }
func (f *fakeStore) Dequeue(string) (types.Task, bool, error) { return types.Task{}, false, nil }
func (f *fakeStore) RecordResult(types.Result) error          { return nil }
func (f *fakeStore) AddDeadEnd(jobID, c string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deadLn = append(f.deadLn, c)
	f.state.DeadEnds++
	return nil
}
func (f *fakeStore) DeadEndCount(string) (int, error) { return len(f.deadLn), nil }
func (f *fakeStore) GetState(string) (types.JobState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.state, nil
}
func (f *fakeStore) PutState(s types.JobState) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.state = s
	return nil
}
func (f *fakeStore) Close() error { return nil }

func (f *fakeStore) deadEnds(string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.deadLn...), nil
}

type fakeBus struct {
	mu     sync.Mutex
	events []types.Event
}

func (f *fakeBus) Publish(ev types.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, ev)
	return nil
}
func (f *fakeBus) Subscribe(topic string, h func(types.Event)) (func(), error) {
	return func() {}, nil
}
func (f *fakeBus) Close() error { return nil }

func (f *fakeBus) byTopic(topic string) []types.Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []types.Event
	for _, e := range f.events {
		if e.Topic == topic {
			out = append(out, e)
		}
	}
	return out
}

func cfg() Config { return Config{PivotAfterSeconds: 3600, DeadEndThreshold: 3, PollMillis: 5} }

func TestCriticPublishesHaltOnSuccess(t *testing.T) {
	st := &fakeStore{state: types.JobState{JobID: "j1", Status: types.StatusSucceeded}}
	b := &fakeBus{}
	c := New(st, b, cfg(), st.deadEnds)
	if err := c.Run(context.Background(), "j1", types.Target{}, nil); err != nil {
		t.Fatal(err)
	}
	if n := len(b.byTopic(types.TopicHalt)); n != 1 {
		t.Fatalf("halt events = %d", n)
	}
	if n := len(b.byTopic(types.TopicMutate)); n != 0 {
		t.Fatalf("unexpected mutate events = %d", n)
	}
}

func TestCriticPivotOnDeadEndThreshold(t *testing.T) {
	st := &fakeStore{state: types.JobState{JobID: "j1-2", Status: types.StatusRunning}}
	for _, d := range []string{"a1b2c3d4", "x9y8z7w6", "q1w2e3r4", "mnbv1234"} {
		_ = st.AddDeadEnd("j1-2", d)
	}
	b := &fakeBus{}
	c := New(st, b, cfg(), st.deadEnds)

	pivoted := make(chan types.Plan, 100)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := c.Run(ctx, "j1-2", types.Target{}, func(p types.Plan) { pivoted <- p })
	if err != nil {
		t.Fatal(err)
	}
	select {
	case p := <-pivoted:
		if p.Strategy != "genetic-pivot-2" {
			t.Fatalf("strategy = %q", p.Strategy)
		}
	default:
		t.Fatal("onPivot not called")
	}
	evs := b.byTopic(types.TopicMutate)
	if len(evs) == 0 {
		t.Fatal("no mutate event published")
	}
	var plan types.Plan
	if err := json.Unmarshal([]byte(evs[0].Payload), &plan); err != nil {
		t.Fatalf("mutate payload not a JSON plan: %v", err)
	}
	if plan.JobID != "j1-2" {
		t.Fatalf("plan job = %q", plan.JobID)
	}
}

func TestCriticPivotOnTimeout(t *testing.T) {
	st := &fakeStore{state: types.JobState{JobID: "j1", Status: types.StatusRunning}}
	b := &fakeBus{}
	c := New(st, b, Config{PivotAfterSeconds: 0, DeadEndThreshold: 0, PollMillis: 5}, st.deadEnds)
	// thresholds disabled: nothing should happen before ctx cancel
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := c.Run(ctx, "j1", types.Target{}, func(types.Plan) { t.Error("unexpected pivot") }); err != nil {
		t.Fatal(err)
	}
	if n := len(b.byTopic(types.TopicMutate)); n != 0 {
		t.Fatalf("mutate events = %d", n)
	}

	// tiny pivot window: should pivot quickly
	c2 := New(st, b, Config{PivotAfterSeconds: 1, DeadEndThreshold: 0, PollMillis: 5}, st.deadEnds)
	pivoted := make(chan struct{}, 10)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()
	_ = c2.Run(ctx2, "j1", types.Target{}, func(types.Plan) { pivoted <- struct{}{} })
	select {
	case <-pivoted:
	default:
		t.Fatal("no pivot after timeout window")
	}
}
