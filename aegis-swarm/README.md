# aegis-swarm

> ⚠️ **AUTHORIZED SECURITY RESEARCH ONLY** ⚠️
>
> aegis-swarm is a distributed WPA/WPA2 handshake-cracking orchestrator
> intended **exclusively** for authorized penetration tests, red-team
> engagements, and lab research on networks you own or have **explicit written
> permission** to assess. Unauthorized interception or cracking of wireless
> networks is illegal in most jurisdictions (e.g. CFAA, Computer Misuse Act,
> StGB §202). By using this software you accept full responsibility for
> complying with all applicable laws and rules of engagement.

Functionally analogous to [Hashtopolis](https://github.com/hashtopolis/server):
a Go orchestrator distributes hashcat jobs to worker agents, with a strategy
engine and a critic feedback loop. All inter-agent messaging is abstracted so
the system runs either against real **Redis + NATS** or fully **in-memory**
(air-gapped test mode).

## Architecture

```
                        ┌────────────────────────────────────────────┐
                        │              ORCHESTRATOR                  │
                        │                                            │
   curl / clients       │   cmd/orchestrator                         │
   ───────────────►     │   ┌─────────────┐   ┌───────────────────┐  │
   POST /jobs           │   │ REST API    │   │ JobManager        │  │
   GET  /jobs/{id}      │   │ (net/http)  │──►│ • strategist plan │  │
   POST /jobs/{id}/halt │   └─────────────┘   │ • chunk & enqueue │  │
   GET  /healthz        │                     │ • critic loop ◄───┼──┼───┐
                        │                     │ • spawner         │  │   │
                        │                     └────────┬──────────┘  │   │
                        └──────────────────────────────┼─────────────┘   │
                                                       │                 │
              ┌────────────────────────────────────────┴───────────┐     │
              │  store.Store (queue/state)    bus.Bus (events)     │     │
              │  memory | redis://…           memory | nats://…    │     │
              └───────┬───────────────────────────────┬────────────┘     │
                      │ Dequeue / RecordResult        │ breakthrough     │
                      │                               │ dead_end / halt  │
        ┌─────────────┼───────────────────────────────┼──────────┐       │
        ▼             ▼                               ▼          ▼       │
   ┌─────────┐   ┌─────────┐                     ┌─────────┐  ┌────────┐ │
   │ worker  │   │ worker  │      ...            │ worker  │  │ critic │─┘
   │ hashcat │   │ hashcat │                     │  sim    │  │ (pivot │
   │ runner  │   │ runner  │                     │ runner  │  │  loop) │
   └─────────┘   └─────────┘                     └─────────┘  └────────┘
```

* **strategist** — pure-function attack planner. `DefaultPlan` builds a
  multi-phase plan (dictionary → rules → mask); `Mutate` generates a new plan
  that avoids observed dead-end patterns.
* **critic** — per-job feedback loop. Polls job state and dead ends; on
  timeout/threshold it publishes `cracking.mutate` and triggers a pivot, on
  success it publishes `cracking.halt`.
* **spawner** — provisions workers. Default `LocalSpawner` execs the worker
  binary; container spawning can be added behind the same interface.
* **runner** — executes one task: real `hashcat` exec wrapper or a
  deterministic simulated runner for tests/demos.

## Quickstart — air-gapped (fully in-memory)

No Redis, no NATS, no network services needed:

```sh
make build
./bin/orchestrator --addr :8080 --store memory --bus memory
```

For a complete single-box demo with workers included (no external services),
add in-process simulated workers that are spawned per submitted job:

```sh
./bin/orchestrator --addr :8080 --store memory --bus memory \
    --sim-workers 4 --sim-secret 'Summer2026!'
```

NOTE: `--workers N` (process-spawned workers via `--worker-bin`) requires
shared network backends (`--store redis://... --bus nats://...`) because
separate processes cannot share in-memory state. The orchestrator refuses
that combination with a clear error; use `--sim-workers` with memory backends.

Submit a job (dictionary phase reads the wordlist file on the orchestrator
host):

```sh
curl -s -X POST localhost:8080/jobs -d '{
  "target": {"essid":"lab-net","bssid":"00:11:22:33:44:55",
             "hash_file":"/data/lab.hc22000","hash_mode":22000},
  "wordlist": "/data/wordlists/rockyou.txt"
}'
# -> 202 {"job_id":"…","plan":{…}}

curl -s localhost:8080/jobs/<job_id>          # JobState JSON
curl -s -X POST localhost:8080/jobs/<job_id>/halt
curl -s localhost:8080/healthz
```

Attach workers (separate processes / machines sharing Redis+NATS):

```sh
# real GPU box:
./bin/worker --id gpu-1 --job <job_id> --runner hashcat \
    --hashcat-bin /usr/bin/hashcat --wordlist-dir /data/wordlists \
    --hash-file /data/lab.hc22000 --hash-mode 22000 \
    --store redis://127.0.0.1:6379 --bus nats://127.0.0.1:4222
```

The fastest demo is the end-to-end integration test (in-memory orchestrator +
4 simulated workers, synthetic WPA fixture, known password):

```sh
make run-demo
```

## Real Redis + NATS mode

```sh
docker compose up -d redis nats
./bin/orchestrator --store redis://127.0.0.1:6379 --bus nats://127.0.0.1:4222
# or the full containerized stack (orchestrator + worker):
docker compose up --build
```

Workers and the orchestrator must point at the same Redis/NATS endpoints.
The worker also honors `AEGIS_*` environment variables (`AEGIS_ID`,
`AEGIS_STORE`, `AEGIS_BUS`, `AEGIS_RUNNER`, `AEGIS_JOB`, …) as flag defaults,
which is how `LocalSpawner` configures spawned workers.

## API

| Route               | Method | Body / Response                                   |
|---------------------|--------|---------------------------------------------------|
| `/healthz`          | GET    | `200 {"status":"ok"}`                             |
| `/jobs`             | POST   | `{target, wordlist}` → `202 {job_id, plan}`       |
| `/jobs/{id}`        | GET    | `200` JobState JSON, `404` unknown job            |
| `/jobs/{id}/halt`   | POST   | `202`; publishes halt, marks halted, kills workers|

Validation: `target.hash_file` required; `hash_mode` must be `22000` or
`22001`; malformed JSON → `400`; un-buildable plan → `422`.

## How chunking, strategy, and the critic work

* **Chunking** — for dictionary phases the orchestrator reads the wordlist
  file and splits it into tasks of `ChunkSize` lines; each task payload is the
  actual comma-separated candidate lines (capped at 200 k chars per task,
  split further if needed) with `Skip`/`Limit` recording the line window.
  Mask and rules phases produce one task per phase (payload = mask / rule
  file). Only phase-0 tasks are enqueued at submit time; a background
  advancer enqueues the next phase when the queue drains.
* **Strategy** — `strategist.DefaultPlan` emits the standard multi-phase plan
  (fast dictionary first, then rules, then masks). On pivot,
  `strategist.Mutate` builds a new plan that avoids dead-end patterns (e.g.
  8-char alnum failures → longer masks with symbols).
* **Critic** — one goroutine per job. It polls progress; if no breakthrough
  arrives within `PivotAfterSeconds` or dead ends exceed `DeadEndThreshold`,
  it mutates the plan and the orchestrator enqueues the new tasks. On success
  it halts the job. Workers share dead-end candidates (up to 100 per task) on
  the bus so the whole swarm benefits.

## Testing

```sh
make vet        # go vet ./...
make test       # go test ./...
make run-demo   # race-enabled in-memory integration test (<10s)
```

## Layout

```
cmd/orchestrator    REST API server + critic loop + spawner wiring
cmd/worker          worker agent CLI (hashcat | sim runner)
internal/types      shared domain types (contract, no deps)
internal/bus        Bus iface + in-memory + NATS impls
internal/store      Store iface + in-memory + Redis impls
internal/strategist attack-plan generation engine
internal/critic     progress evaluation & pivot loop
internal/spawner    worker provisioning (local exec)
internal/runner     hashcat exec wrapper + simulated runner
internal/api        REST API + JobManager (stdlib net/http only)
```
