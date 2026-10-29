# Plan — Distributed WPA Cracking Swarm (Authorized Lab Use)

## Context
Security researcher, air-gapped lab, authorized testing. Build a production-ready
hybrid system per the blueprint: Go orchestrator + strategy engine + Redis/NATS
collaboration fabric + containerized hashcat workers + feedback/critic loop.
Functionally analogous to Hashtopolis (open-source distributed hashcat).

## Stage 1 — Architecture & Scaffolding (skill: vibecoding-general-swarm)
- Repo layout: cmd/orchestrator, cmd/worker, internal/{queue,bus,strategist,store,runner,api}
- Go module, Makefile, docker-compose (redis, nats, orchestrator, worker image)

## Stage 2 — Core Implementation (parallel coder subagents)
- A: Orchestrator REST API + job lifecycle + container spawner (Docker SDK, with local-exec fallback)
- B: Queue/state layer — Redis data structures (job queue, results, dead_ends, state) + NATS pub/sub topics; in-memory implementations for air-gapped/test mode
- C: Worker agent — pulls tasks, runs hashcat (22000/22001 modes) or simulated runner, publishes breakthrough/dead_end events
- D: Strategist + Critic — rule-based strategy engine (dictionary → mask → rules → genetic pivot), evaluates progress, publishes mutations. Implemented in Go with an embedded rule engine (Clojure service optional stub) so it runs air-gapped without JVM

## Stage 3 — Testing
- Unit tests: strategist plan generation, dead-end dedup, task chunking, critic pivot logic
- Integration test: full pipeline with simulated hashcat runner (known-password .hc22000 fixture), verify crack found, events published, workers halted
- `go vet`, `go test ./...` must pass

## Stage 4 — Delivery
- README (setup, air-gapped notes, usage), docker-compose, Makefile
- Archive source tree to /mnt/agents/output/
