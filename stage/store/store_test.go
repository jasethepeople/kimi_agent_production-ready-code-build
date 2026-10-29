package store

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"aegis-swarm/internal/types"
)

func makeTask(id, jobID string) types.Task {
	return types.Task{ID: id, JobID: jobID, Phase: 0, Mode: types.ModeMask, Payload: "?u?l?l", Limit: 100}
}

// runSuite exercises the full Store contract against any implementation.
func runSuite(t *testing.T, st Store) {
	t.Helper()

	t.Run("EnqueueDequeueOrder", func(t *testing.T) {
		job := "order"
		a, b, c := makeTask("a", job), makeTask("b", job), makeTask("c", job)
		for _, task := range []types.Task{a, b, c} {
			if err := st.Enqueue(task); err != nil {
				t.Fatalf("Enqueue: %v", err)
			}
		}
		for _, want := range []types.Task{a, b, c} {
			got, ok, err := st.Dequeue(job)
			if err != nil || !ok {
				t.Fatalf("Dequeue: ok=%v err=%v", ok, err)
			}
			if got != want {
				t.Fatalf("Dequeue = %+v, want %+v", got, want)
			}
		}
	})

	t.Run("DequeueEmptyReturnsFalse", func(t *testing.T) {
		start := time.Now()
		_, ok, err := st.Dequeue("empty-job")
		if err != nil {
			t.Fatalf("Dequeue: %v", err)
		}
		if ok {
			t.Fatal("Dequeue on empty queue: ok=true, want false")
		}
		if d := time.Since(start); d < 900*time.Millisecond || d > 3*time.Second {
			t.Fatalf("Dequeue empty poll took %v, want ~1s", d)
		}
	})

	t.Run("DequeueWakesOnEnqueue", func(t *testing.T) {
		job := "wake"
		go func() {
			time.Sleep(150 * time.Millisecond)
			_ = st.Enqueue(makeTask("late", job))
		}()
		got, ok, err := st.Dequeue(job)
		if err != nil || !ok {
			t.Fatalf("Dequeue: ok=%v err=%v", ok, err)
		}
		if got.ID != "late" {
			t.Fatalf("Dequeue = %+v, want task late", got)
		}
	})

	t.Run("QueuesArePerJob", func(t *testing.T) {
		if err := st.Enqueue(makeTask("x", "jobA")); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		if _, ok, _ := st.Dequeue("jobB"); ok {
			t.Fatal("Dequeue jobB returned task enqueued for jobA")
		}
		got, ok, err := st.Dequeue("jobA")
		if err != nil || !ok || got.ID != "x" {
			t.Fatalf("Dequeue jobA = %+v ok=%v err=%v", got, ok, err)
		}
	})

	t.Run("RecordResult", func(t *testing.T) {
		r := types.Result{TaskID: "t1", JobID: "res", Found: true, Plaintext: "pw", Method: "mask:?u?l?l", Attempts: 42, WorkerID: "w1"}
		if err := st.RecordResult(r); err != nil {
			t.Fatalf("RecordResult: %v", err)
		}
	})

	t.Run("DeadEndDedupAndCount", func(t *testing.T) {
		job := "dedup"
		for i := 0; i < 3; i++ {
			for _, c := range []string{"Password1", "Summer2026!", "Password1"} {
				if err := st.AddDeadEnd(job, c); err != nil {
					t.Fatalf("AddDeadEnd: %v", err)
				}
			}
		}
		n, err := st.DeadEndCount(job)
		if err != nil {
			t.Fatalf("DeadEndCount: %v", err)
		}
		if n != 2 {
			t.Fatalf("DeadEndCount = %d, want 2 (dedup)", n)
		}
		n, err = st.DeadEndCount("never-touched")
		if err != nil {
			t.Fatalf("DeadEndCount unknown job: %v", err)
		}
		if n != 0 {
			t.Fatalf("DeadEndCount unknown job = %d, want 0", n)
		}
	})

	t.Run("StateRoundTrip", func(t *testing.T) {
		want := types.JobState{JobID: "st", Status: types.StatusRunning, TotalAttempts: 1 << 40, DeadEnds: 7, StartedUnix: 1730000000, Plaintext: "pw!"}
		if err := st.PutState(want); err != nil {
			t.Fatalf("PutState: %v", err)
		}
		got, err := st.GetState("st")
		if err != nil {
			t.Fatalf("GetState: %v", err)
		}
		if got != want {
			t.Fatalf("GetState = %+v, want %+v", got, want)
		}
		// Update overwrites.
		want.Status = types.StatusSucceeded
		if err := st.PutState(want); err != nil {
			t.Fatalf("PutState update: %v", err)
		}
		got, err = st.GetState("st")
		if err != nil || got != want {
			t.Fatalf("GetState after update = %+v err=%v, want %+v", got, err, want)
		}
	})

	t.Run("GetStateUnknownJob", func(t *testing.T) {
		got, err := st.GetState("nope")
		if err != nil {
			t.Fatalf("GetState: %v", err)
		}
		if got != (types.JobState{}) && got.JobID != "nope" {
			t.Fatalf("GetState unknown = %+v, want zero-ish state", got)
		}
	})
}

func TestInMemoryStore(t *testing.T) {
	st := NewInMemory()
	defer st.Close()
	runSuite(t, st)
}

func TestInMemoryClose(t *testing.T) {
	st := NewInMemory()
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := st.Enqueue(makeTask("a", "j")); err == nil {
		t.Fatal("Enqueue after Close: want error")
	}
	if _, _, err := st.Dequeue("j"); err == nil {
		t.Fatal("Dequeue after Close: want error")
	}
	if err := st.AddDeadEnd("j", "x"); err == nil {
		t.Fatal("AddDeadEnd after Close: want error")
	}
}

func TestInMemoryConcurrentAccess(t *testing.T) {
	st := NewInMemory()
	defer st.Close()
	job := "conc"

	var wg sync.WaitGroup
	const workers = 8
	const per = 50
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(w int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				_ = st.Enqueue(makeTask("t", job))
				_ = st.AddDeadEnd(job, "cand")
				_ = st.PutState(types.JobState{JobID: job, Status: types.StatusRunning, TotalAttempts: uint64(i)})
				_, _ = st.GetState(job)
				_ = st.RecordResult(types.Result{TaskID: "t", JobID: job, WorkerID: "w"})
				_, _, _ = st.Dequeue(job)
			}
		}(w)
	}
	wg.Wait()

	n, err := st.DeadEndCount(job)
	if err != nil || n != 1 {
		t.Fatalf("DeadEndCount = %d err=%v, want 1", n, err)
	}
	// Queue drained by concurrent dequeues.
	_, ok, err := st.Dequeue("conc-other")
	if err != nil || ok {
		t.Fatalf("Dequeue unrelated job: ok=%v err=%v", ok, err)
	}
}

func redisAvailable(t *testing.T) string {
	t.Helper()
	addr := "127.0.0.1:6379"
	client := redis.NewClient(&redis.Options{Addr: addr})
	if err := client.Ping(context.Background()).Err(); err != nil {
		_ = client.Close()
		t.Skipf("Redis not available at %s: %v", addr, err)
	}
	_ = client.Close()
	return addr
}

func flushTestKeys(t *testing.T, addr string, jobs ...string) {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: addr})
	defer client.Close()
	ctx := context.Background()
	for _, j := range jobs {
		for _, suffix := range []string{"queue", "results", "dead_ends", "state"} {
			client.Del(ctx, "job:"+j+":"+suffix)
		}
	}
}

func TestRedisStore(t *testing.T) {
	addr := redisAvailable(t)
	flushTestKeys(t, addr, "order", "empty-job", "wake", "jobA", "jobB", "res", "dedup", "never-touched", "st", "nope")
	st, err := NewRedis(addr)
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	defer st.Close()
	runSuite(t, st)
}
