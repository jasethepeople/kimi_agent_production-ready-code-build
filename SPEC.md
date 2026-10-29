# SPEC — "aegis-swarm": Distributed WPA/WPA2 Cracking Swarm (Authorized Lab Use)

Authorized-security-research tool. Functionally analogous to Hashtopolis: a Go
orchestrator distributes hashcat jobs to worker agents, with a strategy engine
and a critic feedback loop. All inter-agent messaging is abstracted so the
system runs either against real Redis+NATS or fully in-memory (air-gapped test
mode).

## Go module
`aegis-swarm` — Go 1.22. Allowed external deps (pinned in go.mod):
- github.com/redis/go-redis/v9
- github.com/nats-io/nats.go
- (docker client is OPTIONAL: container spawning is behind an interface; default
  implementation is local process exec, no docker SDK dep)

No other external dependencies. Everything else is stdlib.

## Repo layout
```
aegis-swarm/
├── go.mod, go.sum, Makefile, README.md, .gitignore
├── docker-compose.yml            # redis + nats + orchestrator + worker (optional path)
├── Dockerfile.worker             # hashcat worker image (documents real deployment)
├── cmd/
│   ├── orchestrator/main.go      # REST API server + critic loop + spawner
│   └── worker/main.go            # worker agent CLI
├── internal/
│   ├── types/types.go            # shared domain types (NO other deps)
│   ├── bus/bus.go                # messaging abstraction + in-mem + NATS impls
│   ├── store/store.go            # state/queue abstraction + in-mem + Redis impls
│   ├── strategist/strategist.go  # attack-plan generation engine
│   ├── critic/critic.go          # progress evaluation & pivot loop
│   ├── spawner/spawner.go        # worker provisioning (local exec; docker iface)
│   ├── runner/runner.go          # hashcat exec wrapper + simulated runner
│   └── api/api.go                # REST API (stdlib net/http only)
└── tests: *_test.go colocated per package + internal/integration/integration_test.go
```

## Domain types (internal/types) — EXACT contract
```go
package types

type AttackMode string
const (
    ModeDictionary AttackMode = "dictionary"
    ModeMask       AttackMode = "mask"
    ModeRules      AttackMode = "rules"
)

type Phase struct {
    Type      AttackMode `json:"type"`
    Wordlist  string     `json:"wordlist,omitempty"`
    Mask      string     `json:"mask,omitempty"`      // hashcat mask, e.g. ?u?l?l?l?l?d?d?s
    RuleFile  string     `json:"rule_file,omitempty"`
    ChunkSize int        `json:"chunk_size"`          // candidate count per task
    Priority  int        `json:"priority"`
}

type Plan struct {
    JobID    string  `json:"job_id"`
    Strategy string  `json:"strategy"`
    Phases   []Phase `json:"phases"`
}

type Target struct {
    ESSID    string `json:"essid"`
    BSSID    string `json:"bssid"`
    HashFile string `json:"hash_file"`   // path to .hc22000 file
    HashMode int    `json:"hash_mode"`   // 22000 (WPA-PBKDF2-PMKID+EAPOL) or 22001
}

type Task struct {
    ID      string     `json:"id"`
    JobID   string     `json:"job_id"`
    Phase   int        `json:"phase"`   // index into Plan.Phases
    Mode    AttackMode `json:"mode"`
    Payload string     `json:"payload"` // wordlist chunk ref, mask, or rules spec
    Skip    uint64     `json:"skip"`
    Limit   uint64     `json:"limit"`
}

type Result struct {
    TaskID    string `json:"task_id"`
    JobID     string `json:"job_id"`
    Found     bool   `json:"found"`
    Plaintext string `json:"plaintext,omitempty"`
    Method    string `json:"method"`           // e.g. "dictionary:rockyou", "mask:?u?l..."
    Attempts  uint64 `json:"attempts"`
    WorkerID  string `json:"worker_id"`
}

type Event struct {      // published on the bus
    Topic   string `json:"topic"`    // cracking.discovery|breakthrough|dead_end|mutate
    JobID   string `json:"job_id"`
    Payload string `json:"payload"`  // JSON-encoded topic-specific body
}

type JobState struct {
    JobID         string `json:"job_id"`
    Status        string `json:"status"` // queued|running|succeeded|exhausted|halted
    TotalAttempts uint64 `json:"total_attempts"`
    DeadEnds      int    `json:"dead_ends"`
    StartedUnix   int64  `json:"started_unix"`
    Plaintext     string `json:"plaintext,omitempty"`
}
```

## Module contracts

### bus (internal/bus)
```go
type Bus interface {
    Publish(ev types.Event) error
    Subscribe(topic string, handler func(types.Event)) (unsubscribe func(), err error)
    Close() error
}
func NewInMemory() *InMemoryBus                 // goroutine-safe, fan-out to subscribers
func NewNATS(url string) (Bus, error)           // nats.go client
```

### store (internal/store)
```go
type Store interface {
    Enqueue(t types.Task) error
    Dequeue(jobID string) (types.Task, bool, error)   // blocking w/ 1s poll; false when empty
    RecordResult(r types.Result) error
    AddDeadEnd(jobID, candidate string) error
    DeadEndCount(jobID string) (int, error)
    GetState(jobID string) (types.JobState, error)
    PutState(s types.JobState) error
    Close() error
}
func NewInMemory() *InMemoryStore
func NewRedis(addr string) (Store, error)       // go-redis; keys job:{id}:queue etc.
```

### strategist (internal/strategist)
Pure functions, no I/O:
```go
// DefaultPlan builds the standard multi-phase plan for a target.
func DefaultPlan(jobID string, t types.Target, wordlist string) types.Plan
// Mutate generates a new plan avoiding observed dead-end patterns:
// 8-char alnum failures -> pivot to longer masks with symbols; month+year
// dead-ends -> year-increment mutations, etc.
func Mutate(jobID string, t types.Target, deadEnds []string) types.Plan
// ClassifyPattern returns a short tag like "word+yyyy+sym" for a candidate.
func ClassifyPattern(candidate string) string
```

### critic (internal/critic)
```go
type Config struct { PivotAfterSeconds int; DeadEndThreshold int; PollMillis int }
type Critic struct { /* store, bus, strategist hooks */ }
func New(st store.Store, b bus.Bus, cfg Config) *Critic
// Run loops until ctx cancel, success (publishes halt), pivot (publishes
// cracking.mutate with new plan), or exhaustion.
func (c *Critic) Run(ctx context.Context, jobID string, t types.Target,
                     onPivot func(types.Plan)) error
```

### runner (internal/runner)
```go
type Runner interface {
    // Run executes one task against hash file; returns Result.
    Run(ctx context.Context, t types.Task, hashFile string, hashMode int) (types.Result, error)
}
func NewHashcat(binPath, wordlistDir string) *HashcatRunner  // exec hashcat, parse --potfile
func NewSimulated(secret string, attemptsPerTask uint64) *SimRunner // deterministic; finds
    // secret when task's candidate space contains it (for tests / demos)
```

### spawner (internal/spawner)
```go
type Spawner interface {
    Spawn(workerID string, env map[string]string) (handle string, err error)
    Kill(handle string) error
}
func NewLocal(workerBin string) *LocalSpawner  // os/exec worker process
```

### api (internal/api)
Stdlib net/http, `NewHandler(st store.Store, b bus.Bus, mgr *JobManager) http.Handler`:
- POST /jobs        {target, wordlist} -> 202 {job_id, plan}
- GET  /jobs/{id}   -> JobState + results
- POST /jobs/{id}/halt
- GET  /healthz
JobManager (in api package) owns: plan creation via strategist, task chunking &
enqueue, critic goroutine, respawn-on-pivot. Concurrency-safe.

### worker (cmd/worker)
Flags: --id --store (memory|redis addr) --bus (memory|nats url) --runner
(hashcat|sim) --job --hash-file --hash-mode --secret (sim). Loop: Dequeue ->
runner.Run -> RecordResult -> publish breakthrough/dead_end events. Exits when
queue empty or halt event for its job.

## Integration test (internal/integration/integration_test.go)
Full in-memory stack: orchestrator JobManager + 4 simulated workers; target hash
is a synthetic WPA fixture with known password "Summer2026!" placed in a
generated wordlist chunk. Assert: job reaches status=succeeded, plaintext
returned, breakthrough event observed, dead-end sharing occurred, workers halt.
Must complete in <10s. Also unit tests per package (strategist pattern
classification & mutation, store dedup, chunking, critic pivot on timeout via
short config).

## Build/test gates
- `go vet ./...` clean, `go test ./...` green, no data races (`-race` on integration test).
- Makefile: `make build test vet run-demo` (run-demo = full in-memory demo binary path via `go test -run Integration -v`).
