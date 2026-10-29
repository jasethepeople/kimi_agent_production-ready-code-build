package bus

import (
	"sync"
	"testing"
	"time"

	"aegis-swarm/internal/types"
)

func waitFor(t *testing.T, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", msg)
}

func TestInMemoryPublishSubscribeFanOut(t *testing.T) {
	b := NewInMemory()
	defer b.Close()

	var mu sync.Mutex
	got := make(map[string][]types.Event)
	for _, name := range []string{"a", "b", "c"} {
		name := name
		_, err := b.Subscribe(types.TopicBreakthrough, func(ev types.Event) {
			mu.Lock()
			got[name] = append(got[name], ev)
			mu.Unlock()
		})
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
	}

	ev := types.Event{Topic: types.TopicBreakthrough, JobID: "j1", Payload: "{}"}
	if err := b.Publish(ev); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	waitFor(t, "fan-out delivery", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) == 3
	})
	mu.Lock()
	defer mu.Unlock()
	for name, evs := range got {
		if len(evs) != 1 || evs[0] != ev {
			t.Fatalf("subscriber %s got %v, want [%v]", name, evs, ev)
		}
	}
}

func TestInMemoryExactTopicMatch(t *testing.T) {
	b := NewInMemory()
	defer b.Close()

	var mu sync.Mutex
	var deadEnds, breakthroughs int
	_, _ = b.Subscribe(types.TopicDeadEnd, func(types.Event) {
		mu.Lock()
		deadEnds++
		mu.Unlock()
	})
	_, _ = b.Subscribe(types.TopicBreakthrough, func(types.Event) {
		mu.Lock()
		breakthroughs++
		mu.Unlock()
	})

	_ = b.Publish(types.Event{Topic: types.TopicDeadEnd, JobID: "j"})
	_ = b.Publish(types.Event{Topic: types.TopicDeadEnd, JobID: "j"})
	_ = b.Publish(types.Event{Topic: "cracking.dead", JobID: "j"}) // prefix, no match
	waitFor(t, "two dead_end events", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return deadEnds == 2
	})
	mu.Lock()
	defer mu.Unlock()
	if breakthroughs != 0 {
		t.Fatalf("breakthroughs = %d, want 0", breakthroughs)
	}
}

func TestInMemoryUnsubscribe(t *testing.T) {
	b := NewInMemory()
	defer b.Close()

	var mu sync.Mutex
	count := 0
	unsub, err := b.Subscribe(types.TopicMutate, func(types.Event) {
		mu.Lock()
		count++
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	_ = b.Publish(types.Event{Topic: types.TopicMutate, JobID: "j"})
	waitFor(t, "first delivery", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return count == 1
	})

	unsub()
	unsub() // idempotent

	_ = b.Publish(types.Event{Topic: types.TopicMutate, JobID: "j"})
	time.Sleep(100 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if count != 1 {
		t.Fatalf("count = %d after unsubscribe, want 1", count)
	}
}

func TestInMemorySlowHandlerDoesNotBlockPublisher(t *testing.T) {
	b := NewInMemory()
	defer b.Close()

	release := make(chan struct{})
	_, _ = b.Subscribe(types.TopicDiscovery, func(types.Event) { <-release })

	start := time.Now()
	for i := 0; i < 50; i++ {
		if err := b.Publish(types.Event{Topic: types.TopicDiscovery, JobID: "j"}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Fatalf("publishing blocked for %v", d)
	}
	close(release)
}

func TestInMemoryPanickingHandlerDoesNotBlock(t *testing.T) {
	b := NewInMemory()
	defer b.Close()

	_, _ = b.Subscribe(types.TopicDeadEnd, func(types.Event) { panic("boom") })
	var mu sync.Mutex
	var got bool
	_, _ = b.Subscribe(types.TopicDeadEnd, func(types.Event) {
		mu.Lock()
		got = true
		mu.Unlock()
	})
	if err := b.Publish(types.Event{Topic: types.TopicDeadEnd, JobID: "j"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	waitFor(t, "healthy handler delivery", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return got
	})
}

func TestInMemoryClose(t *testing.T) {
	b := NewInMemory()
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := b.Publish(types.Event{Topic: types.TopicHalt, JobID: "j"}); err == nil {
		t.Fatal("Publish after Close: want error")
	}
	if _, err := b.Subscribe(types.TopicHalt, func(types.Event) {}); err == nil {
		t.Fatal("Subscribe after Close: want error")
	}
}

func TestInMemoryConcurrentAccess(t *testing.T) {
	b := NewInMemory()
	defer b.Close()

	var mu sync.Mutex
	count := 0
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			unsub, err := b.Subscribe(types.TopicBreakthrough, func(types.Event) {
				mu.Lock()
				count++
				mu.Unlock()
			})
			if err != nil {
				t.Errorf("Subscribe: %v", err)
				return
			}
			for j := 0; j < 25; j++ {
				_ = b.Publish(types.Event{Topic: types.TopicBreakthrough, JobID: "j"})
			}
			if i%2 == 0 {
				unsub()
			}
		}(i)
	}
	wg.Wait()
	// No assertion on exact count (unsubscribe timing); -race validates
	// safety. Handlers may still be in flight, so drain before reading.
	waitFor(t, "in-flight handlers to finish", func() bool {
		mu.Lock()
		before := count
		mu.Unlock()
		time.Sleep(20 * time.Millisecond)
		mu.Lock()
		defer mu.Unlock()
		return count == before
	})
}

func natsAvailable(t *testing.T) string {
	t.Helper()
	url := "nats://127.0.0.1:4222"
	b, err := NewNATS(url)
	if err != nil {
		t.Skipf("NATS not available at %s: %v", url, err)
	}
	_ = b.Close()
	return url
}

func TestNATSRoundTrip(t *testing.T) {
	url := natsAvailable(t)
	b, err := NewNATS(url)
	if err != nil {
		t.Fatalf("NewNATS: %v", err)
	}
	defer b.Close()

	ch := make(chan types.Event, 1)
	unsub, err := b.Subscribe(types.TopicBreakthrough, func(ev types.Event) { ch <- ev })
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer unsub()

	want := types.Event{Topic: types.TopicBreakthrough, JobID: "j1", Payload: `{"p":"x"}`}
	time.Sleep(100 * time.Millisecond) // let subscription reach server
	if err := b.Publish(want); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	select {
	case got := <-ch:
		if got != want {
			t.Fatalf("got %+v, want %+v", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for NATS delivery")
	}
}

func TestNATSUnsubscribe(t *testing.T) {
	url := natsAvailable(t)
	b, err := NewNATS(url)
	if err != nil {
		t.Fatalf("NewNATS: %v", err)
	}
	defer b.Close()

	var mu sync.Mutex
	count := 0
	unsub, err := b.Subscribe(types.TopicDeadEnd, func(types.Event) {
		mu.Lock()
		count++
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	_ = b.Publish(types.Event{Topic: types.TopicDeadEnd, JobID: "j"})
	waitFor(t, "delivery", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return count == 1
	})
	unsub()
	time.Sleep(100 * time.Millisecond)
	_ = b.Publish(types.Event{Topic: types.TopicDeadEnd, JobID: "j"})
	time.Sleep(200 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if count != 1 {
		t.Fatalf("count = %d after unsubscribe, want 1", count)
	}
}
