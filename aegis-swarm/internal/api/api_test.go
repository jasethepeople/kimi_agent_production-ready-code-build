package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"aegis-swarm/internal/types"
)

// ---- fakes ----

type fakeStore struct {
	mu       sync.Mutex
	queues   map[string][]types.Task
	states   map[string]types.JobState
	results  []types.Result
	deadEnds map[string]map[string]struct{}
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		queues:   make(map[string][]types.Task),
		states:   make(map[string]types.JobState),
		deadEnds: make(map[string]map[string]struct{}),
	}
}

func (f *fakeStore) Enqueue(t types.Task) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queues[t.JobID] = append(f.queues[t.JobID], t)
	return nil
}

func (f *fakeStore) Dequeue(jobID string) (types.Task, bool, error) {
	deadline := time.Now().Add(time.Second)
	for {
		f.mu.Lock()
		if q := f.queues[jobID]; len(q) > 0 {
			t := q[0]
			f.queues[jobID] = q[1:]
			f.mu.Unlock()
			return t, true, nil
		}
		f.mu.Unlock()
		if time.Now().After(deadline) {
			return types.Task{}, false, nil
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (f *fakeStore) RecordResult(r types.Result) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.results = append(f.results, r)
	st := f.states[r.JobID]
	st.TotalAttempts += r.Attempts
	f.states[r.JobID] = st
	return nil
}

func (f *fakeStore) AddDeadEnd(jobID, candidate string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.deadEnds[jobID] == nil {
		f.deadEnds[jobID] = make(map[string]struct{})
	}
	f.deadEnds[jobID][candidate] = struct{}{}
	return nil
}

func (f *fakeStore) DeadEndCount(jobID string) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.deadEnds[jobID]), nil
}

func (f *fakeStore) GetState(jobID string) (types.JobState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	st, ok := f.states[jobID]
	if !ok {
		return types.JobState{}, errors.New("unknown job")
	}
	return st, nil
}

func (f *fakeStore) PutState(s types.JobState) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.states[s.JobID] = s
	return nil
}

func (f *fakeStore) Close() error { return nil }

func (f *fakeStore) queueLen(jobID string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.queues[jobID])
}

type fakeBus struct {
	mu       sync.Mutex
	handlers map[string][]func(types.Event)
	events   []types.Event
}

func newFakeBus() *fakeBus { return &fakeBus{handlers: make(map[string][]func(types.Event))} }

func (f *fakeBus) Publish(ev types.Event) error {
	f.mu.Lock()
	f.events = append(f.events, ev)
	hs := append([]func(types.Event){}, f.handlers[ev.Topic]...)
	f.mu.Unlock()
	for _, h := range hs {
		h(ev)
	}
	return nil
}

func (f *fakeBus) Subscribe(topic string, handler func(types.Event)) (func(), error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.handlers[topic] = append(f.handlers[topic], handler)
	return func() {}, nil
}

func (f *fakeBus) Close() error { return nil }

func (f *fakeBus) published(topic string) []types.Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []types.Event
	for _, ev := range f.events {
		if ev.Topic == topic {
			out = append(out, ev)
		}
	}
	return out
}

type fakeSpawner struct {
	mu      sync.Mutex
	spawned int
	killed  []string
}

func (f *fakeSpawner) Spawn(workerID string, env map[string]string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.spawned++
	return "handle-" + workerID, nil
}

func (f *fakeSpawner) Kill(handle string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.killed = append(f.killed, handle)
	return nil
}

// ---- helpers ----

func writeWordlist(t *testing.T, lines []string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "words.txt")
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func testTarget() types.Target {
	return types.Target{ESSID: "lab-net", BSSID: "00:11:22:33:44:55", HashFile: "/tmp/lab.hc22000", HashMode: 22000}
}

func waitFor(t *testing.T, d time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", msg)
}

// ---- tests ----

func TestSubmitEnqueuesPlanAndTasks(t *testing.T) {
	lines := []string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot"}
	wl := writeWordlist(t, lines)
	st := newFakeStore()
	b := newFakeBus()
	mgr := NewJobManager(st, b, &fakeSpawner{})
	mgr.CriticCfg.PollMillis = 50
	mgr.CriticCfg.PivotAfterSeconds = 3600 // no pivot during test

	plan, err := mgr.Submit(testTarget(), wl)
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if plan.JobID == "" {
		t.Fatal("empty job ID")
	}
	if len(plan.Phases) == 0 {
		t.Fatal("plan has no phases")
	}
	// Phase 0 of the default plan is dictionary over the submitted wordlist:
	// its tasks must be enqueued immediately.
	if got := st.queueLen(plan.JobID); got == 0 {
		t.Fatal("no phase-0 tasks enqueued")
	}
	task, ok, err := st.Dequeue(plan.JobID)
	if err != nil || !ok {
		t.Fatalf("dequeue: ok=%v err=%v", ok, err)
	}
	if task.Mode != types.ModeDictionary {
		t.Fatalf("phase-0 task mode = %q, want dictionary", task.Mode)
	}
	if task.Payload == "" {
		t.Fatal("empty dictionary payload")
	}
	// All wordlist lines must appear across phase-0 task payloads.
	waitFor(t, 2*time.Second, func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		var all []string
		all = append(all, strings.Split(task.Payload, ",")...)
		for _, rem := range st.queues[plan.JobID] {
			if rem.Mode == types.ModeDictionary {
				all = append(all, strings.Split(rem.Payload, ",")...)
			}
		}
		seen := make(map[string]bool)
		for _, c := range all {
			seen[c] = true
		}
		for _, l := range lines {
			if !seen[l] {
				return false
			}
		}
		return true
	}, "all wordlist lines covered by enqueued tasks")

	// State recorded.
	state, err := mgr.Status(plan.JobID)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if state.JobID != plan.JobID {
		t.Fatalf("state job ID %q", state.JobID)
	}
	_ = mgr.Halt(plan.JobID)
}

func TestChunkingSplitsBySizeAndCapsPayload(t *testing.T) {
	var lines []string
	for i := 0; i < 10; i++ {
		lines = append(lines, strings.Repeat("x", 100))
	}
	wl := writeWordlist(t, lines)
	tasks, err := chunkDictionary("job1", 0, wl, 3)
	if err != nil {
		t.Fatal(err)
	}
	// 10 lines / 3 per chunk -> 4 tasks (3,3,3,1).
	if len(tasks) != 4 {
		t.Fatalf("got %d tasks, want 4", len(tasks))
	}
	var skip uint64
	for i, task := range tasks {
		if task.Skip != skip {
			t.Fatalf("task %d skip=%d want %d", i, task.Skip, skip)
		}
		skip += task.Limit
		if len(task.Payload) > maxPayloadChars {
			t.Fatalf("task %d payload exceeds cap", i)
		}
	}
	if skip != 10 {
		t.Fatalf("total limit %d, want 10", skip)
	}
}

func TestStatusUnknownJob(t *testing.T) {
	mgr := NewJobManager(newFakeStore(), newFakeBus(), nil)
	if _, err := mgr.Status("nope"); err == nil {
		t.Fatal("expected error for unknown job")
	}
}

func TestHaltPublishesEventSetsStatusAndKills(t *testing.T) {
	wl := writeWordlist(t, []string{"one", "two"})
	st := newFakeStore()
	b := newFakeBus()
	sp := &fakeSpawner{}
	mgr := NewJobManager(st, b, sp)

	plan, err := mgr.Submit(testTarget(), wl)
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	mgr.TrackHandle(plan.JobID, "h1")
	mgr.TrackHandle("", "global-1")

	if err := mgr.Halt(plan.JobID); err != nil {
		t.Fatalf("Halt: %v", err)
	}
	if evs := b.published(types.TopicHalt); len(evs) == 0 {
		t.Fatal("no halt event published")
	}
	st2, err := mgr.Status(plan.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if st2.Status != types.StatusHalted {
		t.Fatalf("status = %q, want halted", st2.Status)
	}
	sp.mu.Lock()
	defer sp.mu.Unlock()
	if len(sp.killed) != 2 {
		t.Fatalf("killed handles = %v, want 2", sp.killed)
	}
	if err := mgr.Halt("unknown"); err == nil {
		t.Fatal("expected error halting unknown job")
	}
}

func TestHandler(t *testing.T) {
	wl := writeWordlist(t, []string{"passw0rd", "letmein"})
	mgr := NewJobManager(newFakeStore(), newFakeBus(), nil)
	h := NewHandler(mgr)

	// healthz
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("healthz = %d", rec.Code)
	}

	// POST /jobs bad body
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader("{")))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bad body = %d, want 400", rec.Code)
	}

	// POST /jobs missing hash file
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/jobs",
		strings.NewReader(`{"target":{"hash_mode":22000},"wordlist":"`+wl+`"}`)))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("missing hash_file = %d, want 400", rec.Code)
	}

	// POST /jobs ok -> 202
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/jobs",
		strings.NewReader(`{"target":{"essid":"lab","hash_file":"/tmp/lab.hc22000","hash_mode":22000},"wordlist":"`+wl+`"}`)))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("submit = %d, want 202 (body %s)", rec.Code, rec.Body.String())
	}
	var resp submitResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.JobID == "" || len(resp.Plan.Phases) == 0 {
		t.Fatalf("bad response: %+v", resp)
	}

	// GET /jobs/{id}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/jobs/"+resp.JobID, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	// GET unknown job -> 404
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/jobs/nope", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown job = %d, want 404", rec.Code)
	}

	// POST halt -> 202
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/jobs/"+resp.JobID+"/halt", nil))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("halt = %d, want 202", rec.Code)
	}

	// wrong method -> 405
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/jobs/"+resp.JobID, nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("delete = %d, want 405", rec.Code)
	}
}

// Regression (review H5): rules-phase tasks must carry "<wordlist>|<rulefile>"
// as the payload — both runners split on "|".
func TestBuildPhaseTasksRulesPayload(t *testing.T) {
	plan := types.Plan{JobID: "j1", Phases: []types.Phase{{
		Type: types.ModeRules, Wordlist: "wl.txt", RuleFile: "best64.rule", ChunkSize: 10,
	}}}
	tasks, err := buildPhaseTasks(plan, 0, "fallback.txt")
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || tasks[0].Payload != "wl.txt|best64.rule" {
		t.Fatalf("rules payload = %+v, want wl.txt|best64.rule", tasks)
	}
	// Falls back to the job-level wordlist when the phase omits one.
	plan.Phases[0].Wordlist = ""
	tasks, err = buildPhaseTasks(plan, 0, "fallback.txt")
	if err != nil || tasks[0].Payload != "fallback.txt|best64.rule" {
		t.Fatalf("fallback rules payload = %+v err=%v", tasks, err)
	}
	// No wordlist anywhere -> error, not a silently broken task.
	if _, err = buildPhaseTasks(plan, 0, ""); err == nil {
		t.Fatal("expected error for rules phase without wordlist")
	}
}
