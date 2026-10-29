// Command orchestrator runs the aegis-swarm REST API, critic loop, and
// (optionally) locally spawned workers.
// Authorized security-research use only.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"aegis-swarm/internal/api"
	"aegis-swarm/internal/bus"
	"aegis-swarm/internal/runner"
	"aegis-swarm/internal/spawner"
	"aegis-swarm/internal/store"
	"aegis-swarm/internal/types"
)

func main() {
	var (
		addr       = flag.String("addr", ":8080", "listen address for the REST API")
		storeSpec  = flag.String("store", "memory", `state backend: "memory" or "redis://host:port"`)
		busSpec    = flag.String("bus", "memory", `messaging backend: "memory" or "nats://host:port"`)
		workers    = flag.Int("workers", 0, "spawn N local worker processes per submitted job (requires --worker-bin)")
		workerBin  = flag.String("worker-bin", "", "path to the worker binary for local spawning")
		wRunner    = flag.String("worker-runner", "sim", `runner kind for spawned workers: "hashcat" or "sim"`)
		wSecret    = flag.String("worker-secret", "", "known plaintext for spawned sim workers (demo/testing only)")
		simWorkers = flag.Int("sim-workers", 0, "run N in-process simulated workers per job (air-gapped demo; works with memory backends)")
		simSecret  = flag.String("sim-secret", "", "known plaintext for in-process sim workers")
	)
	flag.Parse()

	ctx0 := context.Background()

	st, err := buildStore(*storeSpec)
	if err != nil {
		log.Fatalf("store: %v", err)
	}
	defer st.Close()

	b, err := buildBus(*busSpec)
	if err != nil {
		log.Fatalf("bus: %v", err)
	}
	defer b.Close()

	var sp *spawner.LocalSpawner
	if *workers > 0 {
		if *workerBin == "" {
			log.Fatalf("--workers > 0 requires --worker-bin")
		}
		sp = spawner.NewLocal(*workerBin)
	}
	var spi spawner.Spawner // nil interface when no local workers are managed
	if sp != nil {
		spi = sp
	}
	mgr := api.NewJobManager(st, b, spi)

	// Spawned worker PROCESSES communicate with the orchestrator via the
	// store and bus — that only works across processes with shared network
	// backends (Redis/NATS). With in-memory backends, use --sim-workers
	// (in-process) instead.
	if sp != nil && (*storeSpec == "memory" || *busSpec == "memory") {
		log.Fatalf("--workers (process-spawned) requires shared backends " +
			"(--store redis://... --bus nats://...); for memory backends use --sim-workers")
	}

	// In-process simulated workers: full swarm loop without any external
	// infrastructure — ideal for air-gapped demos and CI.
	if *simWorkers > 0 {
		prev := mgr.OnJobSubmit
		mgr.OnJobSubmit = func(jobID string, target types.Target) {
			if prev != nil {
				prev(jobID, target)
			}
			for i := 0; i < *simWorkers; i++ {
				go runSimWorker(ctx0, st, b, jobID,
					fmt.Sprintf("sim-%s-%d", jobID[:8], i), *simSecret)
			}
		}
	}

	// Spawn local workers per submitted job, passing the job ID and backend
	// settings via AEGIS_* env vars (the worker binary maps those onto its
	// flags). Handles are tracked against the job so Halt kills them.
	if sp != nil {
		mgr.OnJobSubmit = func(jobID string, target types.Target) {
			for i := 0; i < *workers; i++ {
				id := fmt.Sprintf("worker-%s-%d", jobID[:8], i)
				handle, err := sp.Spawn(id, map[string]string{
					"AEGIS_ID":        id,
					"AEGIS_STORE":     *storeSpec,
					"AEGIS_BUS":       *busSpec,
					"AEGIS_JOB":       jobID,
					"AEGIS_RUNNER":    *wRunner,
					"AEGIS_SECRET":    *wSecret,
					"AEGIS_HASH_FILE": target.HashFile,
					"AEGIS_HASH_MODE": fmt.Sprintf("%d", target.HashMode),
				})
				if err != nil {
					log.Printf("spawn worker %s: %v", id, err)
					continue
				}
				mgr.TrackHandle(jobID, handle)
				log.Printf("spawned %s (handle %s)", id, handle)
			}
		}
	}

	srv := &http.Server{Addr: *addr, Handler: api.NewHandler(mgr)}
	go func() {
		log.Printf("aegis-swarm orchestrator listening on %s (store=%s bus=%s)", *addr, *storeSpec, *busSpec)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http: %v", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	log.Println("shutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	_ = st.Close()
	_ = b.Close()
}

func buildStore(spec string) (store.Store, error) {
	if spec == "memory" || spec == "" {
		return store.NewInMemory(), nil
	}
	if s, ok := strings.CutPrefix(spec, "redis://"); ok {
		return store.NewRedis(s)
	}
	// Bare host:port is accepted as Redis too.
	if strings.Contains(spec, ":") && !strings.Contains(spec, "://") {
		return store.NewRedis(spec)
	}
	return nil, fmt.Errorf("unknown store spec %q", spec)
}

func buildBus(spec string) (bus.Bus, error) {
	if spec == "memory" || spec == "" {
		return bus.NewInMemory(), nil
	}
	if strings.HasPrefix(spec, "nats://") {
		return bus.NewNATS(spec)
	}
	return nil, fmt.Errorf("unknown bus spec %q", spec)
}

// runSimWorker is the in-process equivalent of cmd/worker with the simulated
// runner: dequeue -> run -> record -> publish breakthrough/dead-end events,
// until the queue drains, the job halts, or the context is cancelled.
func runSimWorker(ctx context.Context, st store.Store, b bus.Bus, jobID, id, secret string) {
	r := runner.NewSimulated(secret, 1000)
	halted := make(chan struct{})
	var haltOnce sync.Once // duplicate halts are routine (breakthrough + critic + manual)
	unsub, err := b.Subscribe(types.TopicHalt, func(ev types.Event) {
		if ev.JobID == jobID {
			haltOnce.Do(func() { close(halted) })
		}
	})
	if err != nil {
		log.Printf("sim worker %s: subscribe: %v", id, err)
		return
	}
	defer unsub()

	for {
		select {
		case <-ctx.Done():
			return
		case <-halted:
			return
		default:
		}
		task, ok, err := st.Dequeue(jobID)
		if err != nil {
			log.Printf("sim worker %s: dequeue: %v", id, err)
			return
		}
		if !ok {
			s, serr := st.GetState(jobID)
			if serr == nil && (s.Status == types.StatusSucceeded ||
				s.Status == types.StatusHalted || s.Status == types.StatusExhausted) {
				return
			}
			// Queue momentarily empty; manager may advance phases. Keep polling.
			select {
			case <-ctx.Done():
				return
			case <-halted:
				return
			case <-time.After(50 * time.Millisecond):
			}
			continue
		}
		res, err := r.Run(ctx, task, "", 22000)
		if err != nil {
			log.Printf("sim worker %s: run task %s: %v", id, task.ID, err)
			continue
		}
		res.WorkerID = id
		_ = st.RecordResult(res)
		if res.Found {
			payload, _ := json.Marshal(res)
			_ = b.Publish(types.Event{Topic: types.TopicBreakthrough, JobID: jobID, Payload: string(payload)})
			log.Printf("sim worker %s: BREAKTHROUGH on job %s", id, jobID)
			return
		}
		sample := task.Payload
		if len(sample) > 100 {
			sample = sample[:100]
		}
		payload, _ := json.Marshal([]string{sample})
		_ = b.Publish(types.Event{Topic: types.TopicDeadEnd, JobID: jobID, Payload: string(payload)})
	}
}
