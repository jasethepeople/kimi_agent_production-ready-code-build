// Package spawner provisions worker processes for aegis-swarm.
// Authorized security-research use only.
//
// The default implementation (LocalSpawner) starts the worker binary as a
// local OS process via os/exec. Containerized spawning (docker) is intended
// to be added behind the same Spawner interface; no docker SDK dependency is
// required for the default path.
package spawner

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

// Spawner starts and stops worker agents.
type Spawner interface {
	// Spawn starts a worker with the given ID. The env map is passed to the
	// worker process as environment variables (the worker binary maps
	// AEGIS_* variables onto its flags). Returns an opaque handle.
	Spawn(workerID string, env map[string]string) (handle string, err error)
	// Kill terminates the worker identified by handle: SIGTERM first, then
	// SIGKILL if the process has not exited within 2 seconds.
	Kill(handle string) error
}

// LocalSpawner starts workers as local OS processes.
type LocalSpawner struct {
	workerBin string

	mu    sync.Mutex
	procs map[string]*exec.Cmd
}

// NewLocal returns a LocalSpawner that execs workerBin for each Spawn call.
func NewLocal(workerBin string) *LocalSpawner {
	return &LocalSpawner{workerBin: workerBin, procs: make(map[string]*exec.Cmd)}
}

// Spawn implements Spawner.
func (l *LocalSpawner) Spawn(workerID string, env map[string]string) (string, error) {
	if l.workerBin == "" {
		return "", fmt.Errorf("spawner: no worker binary configured")
	}
	cmd := exec.Command(l.workerBin)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = os.Environ()
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	if err := cmd.Start(); err != nil {
		return "", fmt.Errorf("spawner: start worker %q: %w", workerID, err)
	}
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		_ = cmd.Process.Kill()
		return "", fmt.Errorf("spawner: handle id: %w", err)
	}
	handle := fmt.Sprintf("%s-%d-%s", workerID, cmd.Process.Pid, hex.EncodeToString(b[:]))
	l.mu.Lock()
	l.procs[handle] = cmd
	l.mu.Unlock()
	// Reap the process when it exits on its own so we don't leak zombies.
	go func() {
		_ = cmd.Wait()
		l.mu.Lock()
		delete(l.procs, handle)
		l.mu.Unlock()
	}()
	return handle, nil
}

// Kill implements Spawner: SIGTERM, wait up to 2s, then SIGKILL.
func (l *LocalSpawner) Kill(handle string) error {
	l.mu.Lock()
	cmd, ok := l.procs[handle]
	l.mu.Unlock()
	if !ok {
		return fmt.Errorf("spawner: unknown handle %q", handle)
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		// Process may already be gone; still attempt SIGKILL path cleanup.
		_ = cmd.Process.Kill()
		l.mu.Lock()
		delete(l.procs, handle)
		l.mu.Unlock()
		return nil
	}
	done := make(chan struct{})
	go func() {
		for {
			l.mu.Lock()
			_, stillTracked := l.procs[handle]
			l.mu.Unlock()
			if !stillTracked {
				close(done)
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()
	select {
	case <-done:
		return nil // exited cleanly after SIGTERM (reaped by Spawn goroutine)
	case <-time.After(2 * time.Second):
	}
	if err := cmd.Process.Kill(); err != nil {
		return fmt.Errorf("spawner: SIGKILL %q: %w", handle, err)
	}
	return nil
}
