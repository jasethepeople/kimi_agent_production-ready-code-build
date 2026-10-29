// Package critic evaluates job progress and triggers strategy pivots.
// Authorized security-research use only.
package critic

import (
	"context"
	"encoding/json"
	"time"

	"aegis-swarm/internal/bus"
	"aegis-swarm/internal/store"
	"aegis-swarm/internal/strategist"
	"aegis-swarm/internal/types"
)

// Config tunes the critic feedback loop.
type Config struct {
	PivotAfterSeconds int // pivot when a job runs this long without success
	DeadEndThreshold  int // pivot when this many dead-ends accumulate
	PollMillis        int // poll interval for job state
}

// Critic watches a job and pivots the attack plan when progress stalls.
type Critic struct {
	st     store.Store
	b      bus.Bus
	cfg    Config
	lister func(jobID string) ([]string, error)
}

// New builds a Critic. lister retrieves recent dead-end candidates for a job
// (store.Store has no such accessor, so it is injected).
func New(st store.Store, b bus.Bus, cfg Config, lister func(string) ([]string, error)) *Critic {
	if cfg.PollMillis <= 0 {
		cfg.PollMillis = 500
	}
	return &Critic{st: st, b: b, cfg: cfg, lister: lister}
}

// Run loops until ctx cancel, success (publishes halt), pivot (publishes
// cracking.mutate with new plan), or exhaustion.
func (c *Critic) Run(ctx context.Context, jobID string, t types.Target,
	onPivot func(types.Plan)) error {
	timerStart := time.Now()
	deadEndBaseline := 0 // dead-end count at last pivot; reset on pivot
	tick := time.NewTicker(time.Duration(c.cfg.PollMillis) * time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}

		state, err := c.st.GetState(jobID)
		if err != nil {
			continue // transient store errors: retry next tick
		}
		switch state.Status {
		case types.StatusSucceeded:
			return c.b.Publish(types.Event{Topic: types.TopicHalt, JobID: jobID})
		case types.StatusExhausted, types.StatusHalted:
			return nil
		}

		timedOut := c.cfg.PivotAfterSeconds > 0 &&
			time.Since(timerStart) > time.Duration(c.cfg.PivotAfterSeconds)*time.Second
		// Use the dead-end SET cardinality, not JobState.DeadEnds — workers
		// record dead-ends via AddDeadEnd and nothing aggregates them into
		// the state hash.
		deCount, deErr := c.st.DeadEndCount(jobID)
		tooManyDeadEnds := c.cfg.DeadEndThreshold > 0 && deErr == nil &&
			deCount > c.cfg.DeadEndThreshold && deCount > deadEndBaseline
		if !timedOut && !tooManyDeadEnds {
			continue
		}

		var deadEnds []string
		if c.lister != nil {
			if de, lerr := c.lister(jobID); lerr == nil {
				deadEnds = de
			}
		}
		plan := strategist.Mutate(jobID, t, deadEnds)
		body, err := json.Marshal(plan)
		if err != nil {
			return err
		}
		if err := c.b.Publish(types.Event{
			Topic:   types.TopicMutate,
			JobID:   jobID,
			Payload: string(body),
		}); err != nil {
			return err
		}
		if onPivot != nil {
			onPivot(plan)
		}
		timerStart = time.Now() // reset pivot timer after a mutation
		if deErr == nil {
			deadEndBaseline = deCount
		}
	}
}
