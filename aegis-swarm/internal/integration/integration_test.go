// Package integration runs the full aegis-swarm stack end-to-end in memory.
// Authorized security-research use only.
package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"aegis-swarm/internal/api"
	"aegis-swarm/internal/bus"
	"aegis-swarm/internal/runner"
	"aegis-swarm/internal/store"
	"aegis-swarm/internal/types"
)

// simWorker mimics cmd/worker: dequeue -> run -> record result -> publish events.
type simWorker struct {
	id     string
	st     store.Store
	b      bus.Bus
	jobID  string
	runner runner.Runner
	halted atomic.Bool
	wg     sync.WaitGroup
}

func (w *simWorker) run(ctx context.Context) {
	w.wg.Add(1)
	defer w.wg.Done()
	unsub, err := w.b.Subscribe(types.TopicHalt, func(ev types.Event) {
		if ev.JobID == w.jobID {
			w.halted.Store(true)
		}
	})
	if err != nil {
		return
	}
	defer unsub()

	for !w.halted.Load() && ctx.Err() == nil {
		task, ok, err := w.st.Dequeue(w.jobID)
		if err != nil || !ok {
			if w.halted.Load() || ctx.Err() != nil {
				return
			}
			// queue momentarily empty; the manager may advance phases — keep polling briefly
			select {
			case <-ctx.Done():
				return
			case <-time.After(50 * time.Millisecond):
			}
			if empty, _, _ := pollState(w.st, w.jobID); empty == types.StatusSucceeded ||
				empty == types.StatusHalted || empty == types.StatusExhausted {
				return
			}
			continue
		}
		res, err := w.runner.Run(ctx, task, "", 22000)
		if err != nil {
			continue
		}
		_ = w.st.RecordResult(res)
		if res.Found {
			payload, _ := json.Marshal(res)
			_ = w.b.Publish(types.Event{Topic: types.TopicBreakthrough, JobID: w.jobID, Payload: string(payload)})
			return
		}
		// publish a dead-end sample so peers/orchestrator can share failures
		cands := []string{task.Payload}
		if len(task.Payload) > 100 {
			cands = []string{task.Payload[:100]}
		}
		payload, _ := json.Marshal(cands)
		_ = w.b.Publish(types.Event{Topic: types.TopicDeadEnd, JobID: w.jobID, Payload: string(payload)})
	}
}

func pollState(st store.Store, jobID string) (string, uint64, error) {
	s, err := st.GetState(jobID)
	return s.Status, s.TotalAttempts, err
}

// TestEndToEndCrack drives a full job: a known password hidden in a generated
// wordlist must be found, a breakthrough observed, dead-ends shared, the job
// marked succeeded, and all workers halted.
func TestEndToEndCrack(t *testing.T) {
	const secret = "Summer2026!"

	// Generate a wordlist with the secret embedded mid-file.
	dir := t.TempDir()
	wl := filepath.Join(dir, "lab-wordlist.txt")
	f, err := os.Create(wl)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4000; i++ {
		fmt.Fprintf(f, "candidate%04d\n", i)
	}
	fmt.Fprintln(f, secret)
	for i := 0; i < 4000; i++ {
		fmt.Fprintf(f, "zz%06d\n", i)
	}
	f.Close()

	st := store.NewInMemory()
	b := bus.NewInMemory()
	defer st.Close()
	defer b.Close()

	mgr := api.NewJobManager(st, b, nil)
	mgr.CriticCfg = api.CriticConfig{PivotAfterSeconds: 30, DeadEndThreshold: 5000, PollMillis: 100}

	// Observe bus traffic.
	var breakthroughs, deadEnds atomic.Int32
	sub1, _ := b.Subscribe(types.TopicBreakthrough, func(ev types.Event) { breakthroughs.Add(1) })
	defer sub1()
	sub2, _ := b.Subscribe(types.TopicDeadEnd, func(ev types.Event) { deadEnds.Add(1) })
	defer sub2()

	target := types.Target{ESSID: "LabNet", BSSID: "AA:BB:CC:DD:EE:FF", HashFile: filepath.Join(dir, "lab.hc22000"), HashMode: 22000}

	// Phase 1: dud job — secret NOT in wordlist. Verifies dead-end sharing.
	dud := filepath.Join(dir, "dud.txt")
	os.WriteFile(dud, []byte("alpha\nbeta\ngamma\n"), 0o644)
	dudPlan, err := mgr.Submit(target, dud)
	if err != nil {
		t.Fatalf("submit dud: %v", err)
	}
	dudRunner := runner.NewSimulated(secret, 3)
	dctx, dcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dcancel()
	dw := &simWorker{id: "dud", st: st, b: b, jobID: dudPlan.JobID, runner: dudRunner}
	go dw.run(dctx)
	deadline1 := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline1) && deadEnds.Load() == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	if deadEnds.Load() == 0 {
		t.Fatal("no dead-end sharing observed on dud job")
	}
	if err := mgr.Halt(dudPlan.JobID); err != nil {
		t.Fatalf("halt dud: %v", err)
	}
	dcancel()
	dw.wg.Wait()
	t.Logf("dead-end sharing verified (%d events)", deadEnds.Load())

	// Phase 2: real job — secret present in the wordlist.
	plan, err := mgr.Submit(target, wl)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if plan.JobID == "" || len(plan.Phases) == 0 {
		t.Fatalf("bad plan: %+v", plan)
	}

	// Launch 4 simulated workers.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	workers := make([]*simWorker, 4)
	for i := range workers {
		workers[i] = &simWorker{
			id: fmt.Sprintf("w%d", i), st: st, b: b, jobID: plan.JobID,
			runner: runner.NewSimulated(secret, 1000),
		}
		go workers[i].run(ctx)
	}

	// Wait for success.
	deadline := time.Now().Add(15 * time.Second)
	var final types.JobState
	for time.Now().Before(deadline) {
		final, err = mgr.Status(plan.JobID)
		if err != nil {
			t.Fatalf("status: %v", err)
		}
		if final.Status == types.StatusSucceeded {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if final.Status != types.StatusSucceeded {
		t.Fatalf("job did not succeed, status=%q state=%+v", final.Status, final)
	}
	if final.Plaintext != secret {
		t.Fatalf("wrong plaintext: %q want %q", final.Plaintext, secret)
	}
	if breakthroughs.Load() < 1 {
		t.Fatal("no breakthrough event observed")
	}

	// Workers must all stop after the halt broadcast.
	done := make(chan struct{})
	go func() {
		for _, w := range workers {
			w.wg.Wait()
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("workers did not halt after breakthrough")
	}

	t.Logf("SUCCESS: plaintext=%q attempts=%d deadEndsSeen=%d", final.Plaintext, final.TotalAttempts, deadEnds.Load())
}
