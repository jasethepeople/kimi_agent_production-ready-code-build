// Package api implements the aegis-swarm REST API and job orchestration.
// Authorized security-research use only.
//
// JobManager owns plan creation (via strategist), task chunking & enqueueing,
// a per-job critic goroutine, phase advancement, and pivot re-enqueueing.
// It is safe for concurrent use.
package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"aegis-swarm/internal/bus"
	"aegis-swarm/internal/critic"
	"aegis-swarm/internal/spawner"
	"aegis-swarm/internal/store"
	"aegis-swarm/internal/strategist"
	"aegis-swarm/internal/types"
)

// maxPayloadChars caps the comma-separated candidate payload of a single
// dictionary task; chunks larger than this are split further.
const maxPayloadChars = 200_000

// defaultChunkSize is used when a plan phase specifies ChunkSize <= 0.
const defaultChunkSize = 1000

// CriticConfig mirrors critic.Config so callers can tune the per-job critic
// without importing the critic package directly.
type CriticConfig = critic.Config

// DefaultCriticConfig is used when JobManager.CriticCfg is the zero value.
var DefaultCriticConfig = critic.Config{
	PivotAfterSeconds: 120,
	DeadEndThreshold:  200,
	PollMillis:        500,
}

type job struct {
	id     string
	target types.Target
	plan   types.Plan
	cancel context.CancelFunc
	// pending holds not-yet-enqueued tasks for phases > curPhase.
	pending  [][]types.Task
	curPhase int
	handles  []string // spawned worker handles tracked for this job
}

// JobManager coordinates jobs: planning, chunking, enqueueing, the critic
// feedback loop, and worker lifecycle. Concurrency-safe.
type JobManager struct {
	St        store.Store
	B         bus.Bus
	Sp        spawner.Spawner // may be nil (no local workers managed)
	CriticCfg critic.Config

	// OnJobSubmit, if set, is called with the new job ID and target after a
	// job has been fully planned and its phase-0 tasks enqueued. The
	// orchestrator uses it to spawn per-job workers. It must be non-blocking.
	OnJobSubmit func(jobID string, t types.Target)

	mu       sync.Mutex
	jobs     map[string]*job
	deadEnds map[string][]string // jobID -> observed dead-end candidates
}

// NewJobManager wires a JobManager against the given store and bus. The
// spawner may be nil if this orchestrator does not manage local workers.
func NewJobManager(st store.Store, b bus.Bus, sp spawner.Spawner) *JobManager {
	m := &JobManager{
		St:        st,
		B:         b,
		Sp:        sp,
		CriticCfg: DefaultCriticConfig,
		jobs:      make(map[string]*job),
		deadEnds:  make(map[string][]string),
	}
	// Track dead-end candidates so the critic's lister can feed strategist.Mutate.
	_, _ = b.Subscribe(types.TopicDeadEnd, func(ev types.Event) {
		var cands []string
		if err := json.Unmarshal([]byte(ev.Payload), &cands); err != nil {
			return
		}
		m.mu.Lock()
		m.deadEnds[ev.JobID] = append(m.deadEnds[ev.JobID], cands...)
		m.mu.Unlock()
	})
	// Mark jobs succeeded on breakthrough and stop their workers.
	_, _ = b.Subscribe(types.TopicBreakthrough, func(ev types.Event) {
		var r types.Result
		if err := json.Unmarshal([]byte(ev.Payload), &r); err != nil {
			return
		}
		if st2, err := m.St.GetState(ev.JobID); err == nil {
			st2.Status = types.StatusSucceeded
			st2.Plaintext = r.Plaintext
			_ = m.St.PutState(st2)
		}
		_ = m.B.Publish(types.Event{Topic: types.TopicHalt, JobID: ev.JobID, Payload: `{"reason":"succeeded"}`})
		m.mu.Lock()
		j := m.jobs[ev.JobID]
		m.mu.Unlock()
		if j != nil {
			j.cancel()
			m.killHandles(j)
		}
	})
	return m
}

// listDeadEnds returns a snapshot of observed dead-end candidates for a job.
// It is passed to critic.New as the dead-end lister hook.
func (m *JobManager) listDeadEnds(jobID string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.deadEnds[jobID]))
	copy(out, m.deadEnds[jobID])
	return out, nil
}

func newJobID() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// chunkDictionary splits a wordlist file into tasks of chunkSize lines each.
// Payload is the comma-separated candidate lines; any chunk whose payload
// would exceed maxPayloadChars is split further. Skip/Limit record the line
// offset/count within the wordlist.
func chunkDictionary(jobID string, phaseIdx int, path string, chunkSize int) ([]types.Task, error) {
	if chunkSize <= 0 {
		chunkSize = defaultChunkSize
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read wordlist %q: %w", path, err)
	}
	lines := strings.Split(strings.ReplaceAll(string(data), "\r\n", "\n"), "\n")
	// Drop trailing empty line(s) from final newline.
	for len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	var tasks []types.Task
	var skip uint64
	for start := 0; start < len(lines); {
		end := start + chunkSize
		if end > len(lines) {
			end = len(lines)
		}
		// Split the window further while the payload exceeds the char cap.
		segEnd := start
		var sb strings.Builder
		for segEnd < end {
			line := lines[segEnd]
			add := len(line)
			if segEnd > start {
				add++ // comma
			}
			if sb.Len()+add > maxPayloadChars && segEnd > start {
				break
			}
			if segEnd > start {
				sb.WriteByte(',')
			}
			sb.WriteString(line)
			segEnd++
		}
		n := uint64(segEnd - start)
		tasks = append(tasks, types.Task{
			ID:      fmt.Sprintf("%s-p%d-%d", jobID, phaseIdx, len(tasks)),
			JobID:   jobID,
			Phase:   phaseIdx,
			Mode:    types.ModeDictionary,
			Payload: sb.String(),
			Skip:    skip,
			Limit:   n,
		})
		skip += n
		start = segEnd
	}
	return tasks, nil
}

// buildPhaseTasks materializes the tasks for one plan phase.
func buildPhaseTasks(plan types.Plan, phaseIdx int, wordlist string) ([]types.Task, error) {
	ph := plan.Phases[phaseIdx]
	switch ph.Type {
	case types.ModeDictionary:
		wl := ph.Wordlist
		if wl == "" {
			wl = wordlist
		}
		if wl == "" {
			return nil, errors.New("dictionary phase without wordlist")
		}
		return chunkDictionary(plan.JobID, phaseIdx, wl, ph.ChunkSize)
	case types.ModeMask:
		return []types.Task{{
			ID:      fmt.Sprintf("%s-p%d-mask", plan.JobID, phaseIdx),
			JobID:   plan.JobID,
			Phase:   phaseIdx,
			Mode:    types.ModeMask,
			Payload: ph.Mask,
			Limit:   uint64(ph.ChunkSize),
		}}, nil
	case types.ModeRules:
		// Runners expect the rules payload as "<wordlist>|<rulefile>".
		wl := ph.Wordlist
		if wl == "" {
			wl = wordlist
		}
		if wl == "" {
			return nil, errors.New("rules phase without wordlist")
		}
		return []types.Task{{
			ID:      fmt.Sprintf("%s-p%d-rules", plan.JobID, phaseIdx),
			JobID:   plan.JobID,
			Phase:   phaseIdx,
			Mode:    types.ModeRules,
			Payload: wl + "|" + ph.RuleFile,
			Limit:   uint64(ph.ChunkSize),
		}}, nil
	default:
		return nil, fmt.Errorf("unknown phase type %q", ph.Type)
	}
}

// Submit creates a job for target t with the given wordlist, builds the
// default plan, chunks phase-0 tasks and enqueues them, keeps later phases
// pending, and starts the critic + phase-advancer goroutines.
func (m *JobManager) Submit(t types.Target, wordlist string) (types.Plan, error) {
	jobID, err := newJobID()
	if err != nil {
		return types.Plan{}, fmt.Errorf("job id: %w", err)
	}
	plan := strategist.DefaultPlan(jobID, t, wordlist)
	if len(plan.Phases) == 0 {
		return types.Plan{}, errors.New("strategist returned plan with no phases")
	}
	perPhase := make([][]types.Task, len(plan.Phases))
	for i := range plan.Phases {
		tasks, err := buildPhaseTasks(plan, i, wordlist)
		if err != nil {
			return types.Plan{}, fmt.Errorf("phase %d: %w", i, err)
		}
		perPhase[i] = tasks
	}
	for _, task := range perPhase[0] {
		if err := m.St.Enqueue(task); err != nil {
			return types.Plan{}, fmt.Errorf("enqueue: %w", err)
		}
	}
	if err := m.St.PutState(types.JobState{
		JobID:       jobID,
		Status:      types.StatusQueued,
		StartedUnix: time.Now().Unix(),
	}); err != nil {
		return types.Plan{}, fmt.Errorf("put state: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	j := &job{id: jobID, target: t, plan: plan, cancel: cancel, pending: perPhase[1:]}
	m.mu.Lock()
	m.jobs[jobID] = j
	m.mu.Unlock()

	cfg := m.CriticCfg
	if cfg == (critic.Config{}) {
		cfg = DefaultCriticConfig
	}
	c := critic.New(m.St, m.B, cfg, m.listDeadEnds)
	go func() {
		if err := c.Run(ctx, jobID, t, func(newPlan types.Plan) { m.onPivot(j, newPlan) }); err != nil &&
			!errors.Is(err, context.Canceled) {
			log.Printf("api: critic job %s: %v", jobID, err)
		}
	}()
	go m.advanceLoop(ctx, j)
	if m.OnJobSubmit != nil {
		m.OnJobSubmit(jobID, t)
	}
	return plan, nil
}

// onPivot enqueues the mutated plan produced by the critic. Phase 0 tasks are
// enqueued immediately; later phases replace the pending queue.
func (m *JobManager) onPivot(j *job, newPlan types.Plan) {
	perPhase := make([][]types.Task, len(newPlan.Phases))
	for i := range newPlan.Phases {
		tasks, err := buildPhaseTasks(newPlan, i, "")
		if err != nil {
			log.Printf("api: pivot plan phase %d: %v", i, err)
			return
		}
		perPhase[i] = tasks
	}
	for _, task := range perPhase[0] {
		if err := m.St.Enqueue(task); err != nil {
			log.Printf("api: pivot enqueue: %v", err)
			return
		}
	}
	m.mu.Lock()
	j.plan = newPlan
	j.pending = perPhase[1:]
	j.curPhase = 0
	m.mu.Unlock()
}

// queueEmpty probes the store: it dequeues one task and immediately
// re-enqueues it if present. Returns true when no task was available.
func (m *JobManager) queueEmpty(jobID string) bool {
	task, ok, err := m.St.Dequeue(jobID)
	if err != nil {
		return false
	}
	if !ok {
		return true
	}
	if err := m.St.Enqueue(task); err != nil {
		log.Printf("api: re-enqueue probe task: %v", err)
	}
	return false
}

// advanceLoop keeps later phases pending until the prior phase exhausts
// (queue drains), then enqueues the next phase's tasks.
func (m *JobManager) advanceLoop(ctx context.Context, j *job) {
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	emptyTicks := 0 // consecutive ticks with no pending phases and a drained queue
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		st, err := m.St.GetState(j.id)
		if err == nil && (st.Status == types.StatusHalted || st.Status == types.StatusSucceeded) {
			return
		}
		m.mu.Lock()
		hasPending := len(j.pending) > 0
		m.mu.Unlock()
		if !hasPending {
			// No phases left: when the queue has fully drained without a
			// breakthrough, the job is exhausted. Require several consecutive
			// empty ticks so a task currently being processed by a worker
			// (dequeued but not yet reported) doesn't trigger a premature
			// exhaustion.
			if m.queueEmpty(j.id) {
				emptyTicks++
				if emptyTicks >= 4 {
					if st2, err := m.St.GetState(j.id); err == nil &&
						(st2.Status == types.StatusQueued || st2.Status == types.StatusRunning) {
						st2.Status = types.StatusExhausted
						_ = m.St.PutState(st2)
						_ = m.B.Publish(types.Event{Topic: types.TopicHalt, JobID: j.id, Payload: `{"reason":"exhausted"}`})
					}
					return
				}
			} else {
				emptyTicks = 0
			}
			continue
		}
		if !m.queueEmpty(j.id) {
			continue
		}
		m.mu.Lock()
		if len(j.pending) == 0 {
			m.mu.Unlock()
			continue
		}
		next := j.pending[0]
		j.pending = j.pending[1:]
		j.curPhase++
		m.mu.Unlock()
		for _, task := range next {
			if err := m.St.Enqueue(task); err != nil {
				log.Printf("api: advance enqueue job %s: %v", j.id, err)
			}
		}
		if st2, err := m.St.GetState(j.id); err == nil && st2.Status == types.StatusQueued {
			st2.Status = types.StatusRunning
			_ = m.St.PutState(st2)
		}
	}
}

// Status returns the current JobState for jobID.
func (m *JobManager) Status(jobID string) (types.JobState, error) {
	st, err := m.St.GetState(jobID)
	if err != nil {
		return types.JobState{}, err
	}
	if n, err := m.St.DeadEndCount(jobID); err == nil {
		st.DeadEnds = n
	}
	return st, nil
}

// TrackHandle records a spawned worker handle against a job so Halt can kill
// it. Workers started before any job exists may be tracked with jobID "" and
// are killed on every Halt.
func (m *JobManager) TrackHandle(jobID, handle string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j := m.jobs[jobID]
	if j == nil {
		j = &job{id: jobID, cancel: func() {}}
		m.jobs[jobID] = j
	}
	j.handles = append(j.handles, handle)
}

// killHandles terminates all workers tracked for a job (and untracked ones).
func (m *JobManager) killHandles(j *job) {
	if m.Sp == nil {
		return
	}
	m.mu.Lock()
	handles := append([]string(nil), j.handles...)
	if global, ok := m.jobs[""]; ok && global != j {
		handles = append(handles, global.handles...)
	}
	m.mu.Unlock()
	for _, h := range handles {
		if err := m.Sp.Kill(h); err != nil {
			log.Printf("api: kill handle %s: %v", h, err)
		}
	}
}

// Halt publishes a halt event for jobID, marks the job halted, and kills any
// spawned worker handles.
func (m *JobManager) Halt(jobID string) error {
	if _, err := m.St.GetState(jobID); err != nil {
		return fmt.Errorf("unknown job %q", jobID)
	}
	if err := m.B.Publish(types.Event{Topic: types.TopicHalt, JobID: jobID, Payload: `{"reason":"manual"}`}); err != nil {
		return fmt.Errorf("publish halt: %w", err)
	}
	st, err := m.St.GetState(jobID)
	if err != nil {
		return err
	}
	st.Status = types.StatusHalted
	if err := m.St.PutState(st); err != nil {
		return err
	}
	m.mu.Lock()
	j := m.jobs[jobID]
	m.mu.Unlock()
	if j != nil {
		j.cancel()
		m.killHandles(j)
	}
	return nil
}

// ---- HTTP handler ----

type submitRequest struct {
	Target   types.Target `json:"target"`
	Wordlist string       `json:"wordlist"`
}

type submitResponse struct {
	JobID string     `json:"job_id"`
	Plan  types.Plan `json:"plan"`
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

// NewHandler returns the aegis-swarm REST API handler (stdlib net/http only):
//
//	POST /jobs            {target, wordlist} -> 202 {job_id, plan}
//	GET  /jobs/{id}       -> 200 JobState
//	POST /jobs/{id}/halt  -> 202
//	GET  /healthz         -> 200
func NewHandler(mgr *JobManager) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("/jobs", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		var req submitRequest
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
			return
		}
		if req.Target.HashFile == "" {
			writeErr(w, http.StatusBadRequest, "target.hash_file is required")
			return
		}
		if req.Target.HashMode != 22000 && req.Target.HashMode != 22001 {
			writeErr(w, http.StatusBadRequest, "target.hash_mode must be 22000 or 22001")
			return
		}
		plan, err := mgr.Submit(req.Target, req.Wordlist)
		if err != nil {
			writeErr(w, http.StatusUnprocessableEntity, err.Error())
			return
		}
		writeJSON(w, http.StatusAccepted, submitResponse{JobID: plan.JobID, Plan: plan})
	})
	mux.HandleFunc("/jobs/", func(w http.ResponseWriter, r *http.Request) {
		rest := strings.TrimPrefix(r.URL.Path, "/jobs/")
		id, halt, _ := strings.Cut(rest, "/")
		if id == "" {
			writeErr(w, http.StatusNotFound, "missing job id")
			return
		}
		switch {
		case halt == "halt" && r.Method == http.MethodPost:
			if err := mgr.Halt(id); err != nil {
				writeErr(w, http.StatusNotFound, err.Error())
				return
			}
			writeJSON(w, http.StatusAccepted, map[string]string{"job_id": id, "status": types.StatusHalted})
		case halt == "" && r.Method == http.MethodGet:
			st, err := mgr.Status(id)
			if err != nil {
				writeErr(w, http.StatusNotFound, "unknown job "+id)
				return
			}
			writeJSON(w, http.StatusOK, st)
		default:
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	})
	return mux
}
