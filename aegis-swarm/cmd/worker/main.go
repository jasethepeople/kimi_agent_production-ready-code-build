// Command worker is the aegis-swarm worker agent: it dequeues tasks for one
// job, runs them through a runner (hashcat or simulated), records results,
// and publishes breakthrough / dead-end events.
// Authorized security-research use only.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"

	"aegis-swarm/internal/bus"
	"aegis-swarm/internal/runner"
	"aegis-swarm/internal/store"
	"aegis-swarm/internal/types"
)

// envOr returns the value of the AEGIS_<NAME> environment variable, or def.
func envOr(name, def string) string {
	if v := os.Getenv("AEGIS_" + name); v != "" {
		return v
	}
	return def
}

// envOrInt is envOr for integer flags; invalid values fall back to def.
func envOrInt(name string, def int) int {
	if v := os.Getenv("AEGIS_" + name); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func main() {
	var (
		id          = flag.String("id", envOr("ID", "worker-0"), "worker ID")
		storeSpec   = flag.String("store", envOr("STORE", "memory"), `"memory" or "redis://host:port"`)
		busSpec     = flag.String("bus", envOr("BUS", "memory"), `"memory" or "nats://host:port"`)
		runnerKind  = flag.String("runner", envOr("RUNNER", "sim"), `"hashcat" or "sim"`)
		jobID       = flag.String("job", envOr("JOB", ""), "job ID to work on (required)")
		hashFile    = flag.String("hash-file", envOr("HASH_FILE", ""), "path to .hc22000 hash file")
		hashMode    = flag.Int("hash-mode", envOrInt("HASH_MODE", 22000), "hashcat hash mode (22000 or 22001)")
		secret      = flag.String("secret", envOr("SECRET", ""), "known plaintext for the simulated runner")
		hashcatBin  = flag.String("hashcat-bin", envOr("HASHCAT_BIN", "hashcat"), "path to hashcat binary")
		wordlistDir = flag.String("wordlist-dir", envOr("WORDLIST_DIR", ""), "wordlist directory for hashcat runner")
	)
	flag.Parse()
	if *jobID == "" {
		log.Fatal("--job is required")
	}

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

	var r runner.Runner
	switch *runnerKind {
	case "hashcat":
		r = runner.NewHashcat(*hashcatBin, *wordlistDir)
	case "sim":
		r = runner.NewSimulated(*secret, 0)
	default:
		log.Fatalf("unknown runner %q", *runnerKind)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() { <-sig; cancel() }()

	// Halt event for our job -> stop promptly and exit 0.
	var halted atomic.Bool
	unsub, err := b.Subscribe(types.TopicHalt, func(ev types.Event) {
		if ev.JobID == *jobID {
			log.Printf("worker %s: halt received for job %s", *id, *jobID)
			halted.Store(true)
			cancel()
		}
	})
	if err != nil {
		log.Fatalf("subscribe halt: %v", err)
	}
	defer unsub()

	log.Printf("worker %s: runner=%s job=%s", *id, *runnerKind, *jobID)
	for {
		if halted.Load() || ctx.Err() != nil {
			return
		}
		task, ok, err := st.Dequeue(*jobID)
		if err != nil {
			log.Printf("dequeue: %v", err)
			return
		}
		if !ok {
			log.Printf("worker %s: queue empty, exiting", *id)
			return
		}
		if halted.Load() || ctx.Err() != nil {
			return
		}
		res, err := r.Run(ctx, task, *hashFile, *hashMode)
		if err != nil {
			if ctx.Err() != nil || halted.Load() {
				return
			}
			log.Printf("run task %s: %v", task.ID, err)
			continue
		}
		res.WorkerID = *id
		if err := st.RecordResult(res); err != nil {
			log.Printf("record result: %v", err)
		}
		if res.Found {
			payload, _ := json.Marshal(res)
			if err := b.Publish(types.Event{Topic: types.TopicBreakthrough, JobID: *jobID, Payload: string(payload)}); err != nil {
				log.Printf("publish breakthrough: %v", err)
			}
			log.Printf("worker %s: BREAKTHROUGH job=%s plaintext=%q", *id, *jobID, res.Plaintext)
			return // job done; critic/orchestrator will halt others
		}
		// Dead end: share tried candidates (dictionary payload) with the swarm.
		var tried []string
		if task.Mode == types.ModeDictionary && task.Payload != "" {
			tried = strings.Split(task.Payload, ",")
			if len(tried) > 100 {
				tried = tried[:100]
			}
		}
		payload, _ := json.Marshal(tried)
		if err := b.Publish(types.Event{Topic: types.TopicDeadEnd, JobID: *jobID, Payload: string(payload)}); err != nil {
			log.Printf("publish dead end: %v", err)
		}
		for _, c := range tried {
			_ = st.AddDeadEnd(*jobID, c)
		}
	}
}

func buildStore(spec string) (store.Store, error) {
	if spec == "memory" || spec == "" {
		return store.NewInMemory(), nil
	}
	if s, ok := strings.CutPrefix(spec, "redis://"); ok {
		return store.NewRedis(s)
	}
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
