package spawner

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"
)

// fakeWorker writes a shell script that prints its env marker and sleeps.
func fakeWorker(t *testing.T, script string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("shell-script worker requires a POSIX shell")
	}
	path := filepath.Join(t.TempDir(), "fake-worker.sh")
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

// waitFile waits until path exists or the deadline passes.
func waitFile(t *testing.T, path string, d time.Duration, msg string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", msg)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestLocalSpawnAndKill(t *testing.T) {
	bin := fakeWorker(t, "#!/bin/sh\ntrap 'echo term >> \"$AEGIS_MARKER\"; exit 0' TERM\ntouch \"$AEGIS_READY\"\nwhile true; do sleep 0.1; done\n")
	dir := t.TempDir()
	marker := filepath.Join(dir, "marker")
	ready := filepath.Join(dir, "ready")

	l := NewLocal(bin)
	handle, err := l.Spawn("w1", map[string]string{
		"AEGIS_MARKER": marker, "AEGIS_READY": ready, "AEGIS_ID": "w1",
	})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	// Wait until the worker has installed its signal trap before killing.
	waitFile(t, ready, 2*time.Second, "worker readiness file")
	if !strings.HasPrefix(handle, "w1-") {
		t.Fatalf("handle %q should be prefixed with worker ID", handle)
	}
	if err := l.Kill(handle); err != nil {
		t.Fatalf("Kill: %v", err)
	}
	// SIGTERM trap should have fired; process reaped.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(marker); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("worker did not receive SIGTERM (no marker file)")
		}
		time.Sleep(20 * time.Millisecond)
	}
	l.mu.Lock()
	_, still := l.procs[handle]
	l.mu.Unlock()
	if still {
		t.Fatal("process still tracked after Kill")
	}
}

func TestKillEscalatesToSIGKILL(t *testing.T) {
	// Ignores SIGTERM; Kill must escalate to SIGKILL after ~2s.
	bin := fakeWorker(t, "#!/bin/sh\ntrap '' TERM\ntouch \"$AEGIS_READY\"\nwhile true; do sleep 0.1; done\n")
	ready := filepath.Join(t.TempDir(), "ready")
	l := NewLocal(bin)
	handle, err := l.Spawn("w2", map[string]string{"AEGIS_READY": ready})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	waitFile(t, ready, 2*time.Second, "worker readiness file")
	l.mu.Lock()
	cmd := l.procs[handle]
	l.mu.Unlock()
	start := time.Now()
	if err := l.Kill(handle); err != nil {
		t.Fatalf("Kill: %v", err)
	}
	if d := time.Since(start); d < 1800*time.Millisecond || d > 5*time.Second {
		t.Fatalf("Kill took %v, expected ~2s escalation window", d)
	}
	// The reaper goroutine removes the process from tracking once Wait returns.
	deadline := time.Now().Add(2 * time.Second)
	for {
		l.mu.Lock()
		_, still := l.procs[handle]
		l.mu.Unlock()
		if !still {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("process still tracked after SIGKILL")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := cmd.Process.Signal(syscall.Signal(0)); err == nil {
		t.Fatal("process still alive after SIGKILL")
	}
}

func TestKillUnknownHandle(t *testing.T) {
	l := NewLocal("/bin/true")
	if err := l.Kill("nope"); err == nil {
		t.Fatal("expected error for unknown handle")
	}
}

func TestSpawnMissingBinary(t *testing.T) {
	l := NewLocal("")
	if _, err := l.Spawn("w3", nil); err == nil {
		t.Fatal("expected error with no worker binary")
	}
}
