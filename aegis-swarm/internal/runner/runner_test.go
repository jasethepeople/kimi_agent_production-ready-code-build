package runner

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"aegis-swarm/internal/types"
)

func TestSimRunnerDictionary(t *testing.T) {
	s := NewSimulated("Summer2026!", 1000)
	task := types.Task{ID: "t1", JobID: "j1", Mode: types.ModeDictionary,
		Payload: "hello,letmein,Summer2026!,dragon", Limit: 4}
	res, err := s.Run(context.Background(), task, "h.hc22000", 22000)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Found || res.Plaintext != "Summer2026!" {
		t.Fatalf("expected find, got %+v", res)
	}
	if res.Attempts != 1000 {
		t.Fatalf("attempts = %d", res.Attempts)
	}
	if !strings.HasPrefix(res.Method, "dictionary:") {
		t.Fatalf("method = %q", res.Method)
	}

	miss := types.Task{ID: "t2", JobID: "j1", Mode: types.ModeDictionary, Payload: "aaa,bbb"}
	res2, _ := s.Run(context.Background(), miss, "h", 22000)
	if res2.Found {
		t.Fatal("unexpected find")
	}
}

func TestSimRunnerMask(t *testing.T) {
	s := NewSimulated("Abcde12", 42)
	found, _ := s.Run(context.Background(), types.Task{
		ID: "t3", JobID: "j1", Mode: types.ModeMask, Payload: "?u?l?l?l?l?d?d"}, "h", 22000)
	if !found.Found {
		t.Fatal("mask should match secret")
	}
	if found.Method != "mask:?u?l?l?l?l?d?d" {
		t.Fatalf("method = %q", found.Method)
	}
	miss, _ := s.Run(context.Background(), types.Task{
		ID: "t4", JobID: "j1", Mode: types.ModeMask, Payload: "?d?d?d?d"}, "h", 22000)
	if miss.Found {
		t.Fatal("mask should not match")
	}
}

func TestMaskMatch(t *testing.T) {
	cases := []struct {
		plain, mask string
		want        bool
	}{
		{"Pass123!", "?u?l?l?l?d?d?d?s", true},
		{"pass123!", "?u?l?l?l?d?d?d?s", false},
		{"Pass123!", "?u?l?l?l?d?d?d", false},
		{"ab12", "?l?l?d?d", true},
		{"ab12", "?l?l?d?d?d", false},
		{"a?l", "?l??l", true},
	}
	for _, c := range cases {
		if got := maskMatch(c.plain, c.mask); got != c.want {
			t.Errorf("maskMatch(%q,%q) = %v, want %v", c.plain, c.mask, got, c.want)
		}
	}
}

// fakeExec captures argv and writes a potfile line if "cracked".
type fakeExec struct {
	name    string
	args    []string
	cracked string // plaintext to plant in potfile; empty = no crack
}

func (f *fakeExec) run(ctx context.Context, name string, args ...string) error {
	f.name = name
	f.args = append([]string(nil), args...)
	if f.cracked == "" {
		return nil
	}
	for i, a := range args {
		if a == "--potfile-path" && i+1 < len(args) {
			return os.WriteFile(args[i+1], []byte("deadbeef:"+f.cracked+"\n"), 0o600)
		}
	}
	return nil
}

func TestHashcatRunnerArgvDictionary(t *testing.T) {
	fx := &fakeExec{cracked: "hunter2"}
	h := NewHashcat("/usr/bin/hashcat", "/wordlists")
	h.exec = fx.run
	res, err := h.Run(context.Background(), types.Task{
		ID: "t1", JobID: "j1", Mode: types.ModeDictionary,
		Payload: "rockyou.txt", Skip: 100, Limit: 50000,
	}, "cap.hc22000", 22000)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(fx.args, " ")
	for _, want := range []string{"-m 22000", "-a 0", "cap.hc22000",
		filepath.Join("/wordlists", "rockyou.txt"), "--skip 100", "--limit 50000",
		"--potfile-path", "--quiet"} {
		if !strings.Contains(joined, want) {
			t.Errorf("argv %q missing %q", joined, want)
		}
	}
	if !res.Found || res.Plaintext != "hunter2" {
		t.Fatalf("expected crack from potfile, got %+v", res)
	}
	if res.Method != "dictionary:rockyou" {
		t.Fatalf("method = %q", res.Method)
	}
	if fx.name != "/usr/bin/hashcat" {
		t.Fatalf("bin = %q", fx.name)
	}
}

func TestHashcatRunnerArgvMaskAndRules(t *testing.T) {
	fx := &fakeExec{}
	h := NewHashcat("hashcat", "")
	h.exec = fx.run
	_, err := h.Run(context.Background(), types.Task{
		ID: "t2", JobID: "j1", Mode: types.ModeMask, Payload: "?u?l?l?l?l?d?d?d",
	}, "h", 22000)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(fx.args, " ")
	if !strings.Contains(joined, "-a 3 h ?u?l?l?l?l?d?d?d") {
		t.Fatalf("mask argv: %q", joined)
	}

	_, err = h.Run(context.Background(), types.Task{
		ID: "t3", JobID: "j1", Mode: types.ModeRules, Payload: "base.txt|rules/best64.rule",
	}, "h", 22000)
	if err != nil {
		t.Fatal(err)
	}
	joined = strings.Join(fx.args, " ")
	if !strings.Contains(joined, "-a 0 h base.txt -r rules/best64.rule") {
		t.Fatalf("rules argv: %q", joined)
	}
}
