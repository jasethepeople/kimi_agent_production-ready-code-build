// Package runner wraps hashcat execution and provides a deterministic
// simulated runner for tests and demos.
// Authorized security-research use only.
package runner

import (
	"bufio"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"aegis-swarm/internal/types"
)

// Runner executes one task against a hash file and returns the result.
type Runner interface {
	Run(ctx context.Context, t types.Task, hashFile string, hashMode int) (types.Result, error)
}

// execFn is the injectable command runner used by HashcatRunner (tests stub it).
type execFn func(ctx context.Context, name string, args ...string) error

func defaultExec(ctx context.Context, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout = nil
	cmd.Stderr = nil
	return cmd.Run()
}

// HashcatRunner shells out to the hashcat binary and parses its potfile.
type HashcatRunner struct {
	binPath     string
	wordlistDir string
	exec        execFn
}

// NewHashcat builds a runner around the hashcat binary at binPath.
func NewHashcat(binPath, wordlistDir string) *HashcatRunner {
	return &HashcatRunner{binPath: binPath, wordlistDir: wordlistDir, exec: defaultExec}
}

// resolveWordlist joins relative wordlist refs with the configured dir.
func (h *HashcatRunner) resolveWordlist(wl string) string {
	if wl == "" || filepath.IsAbs(wl) || h.wordlistDir == "" {
		return wl
	}
	return filepath.Join(h.wordlistDir, wl)
}

// buildArgs constructs the hashcat argv for a task.
//
//	hashcat -m <mode> -a 0|3 <hashfile> <wordlist|mask>
//	        --potfile-path <tmp> --quiet [--skip N --limit N] [-r rulefile]
func (h *HashcatRunner) buildArgs(t types.Task, hashFile string, hashMode int, potfile string) []string {
	args := []string{"-m", strconv.Itoa(hashMode)}
	switch t.Mode {
	case types.ModeMask:
		args = append(args, "-a", "3", hashFile, t.Payload)
	case types.ModeRules:
		wl, rule, _ := strings.Cut(t.Payload, "|")
		args = append(args, "-a", "0", hashFile, h.resolveWordlist(wl))
		if rule != "" {
			args = append(args, "-r", rule)
		}
	default: // dictionary
		args = append(args, "-a", "0", hashFile, h.resolveWordlist(t.Payload))
	}
	// --skip/--limit are supported for -a 0 and -a 3 straight attacks.
	if t.Skip > 0 {
		args = append(args, "--skip", strconv.FormatUint(t.Skip, 10))
	}
	if t.Limit > 0 {
		args = append(args, "--limit", strconv.FormatUint(t.Limit, 10))
	}
	args = append(args, "--potfile-path", potfile, "--quiet")
	return args
}

// methodTag builds the Result.Method string for a task.
func methodTag(t types.Task) string {
	switch t.Mode {
	case types.ModeMask:
		return "mask:" + t.Payload
	case types.ModeRules:
		_, rule, _ := strings.Cut(t.Payload, "|")
		return "rules:" + rule
	default:
		base := filepath.Base(t.Payload)
		return "dictionary:" + strings.TrimSuffix(base, filepath.Ext(base))
	}
}

// parsePotfile reads hash:plain lines; returns the first plaintext.
func parsePotfile(path string) (string, bool, error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if i := strings.Index(line, ":"); i >= 0 && i+1 < len(line) {
			return line[i+1:], true, nil
		}
	}
	return "", false, sc.Err()
}

// Run executes one task via hashcat and parses the potfile for cracks.
func (h *HashcatRunner) Run(ctx context.Context, t types.Task, hashFile string, hashMode int) (types.Result, error) {
	res := types.Result{TaskID: t.ID, JobID: t.JobID, Method: methodTag(t)}

	tmp, err := os.MkdirTemp("", "aegis-pot-*")
	if err != nil {
		return res, err
	}
	defer os.RemoveAll(tmp)
	potfile := filepath.Join(tmp, "hashcat.potfile")

	args := h.buildArgs(t, hashFile, hashMode, potfile)
	if err := h.exec(ctx, h.binPath, args...); err != nil {
		// hashcat exits non-zero on exhaustion in some versions; still parse potfile.
		if ctx.Err() != nil {
			return res, ctx.Err()
		}
	}

	plain, found, perr := parsePotfile(potfile)
	if perr != nil {
		return res, perr
	}
	res.Found = found
	res.Plaintext = plain
	res.Attempts = t.Limit
	return res, nil
}

// SimRunner is a deterministic in-memory runner for tests and demos.
type SimRunner struct {
	secret          string
	attemptsPerTask uint64
}

// NewSimulated builds a SimRunner that "cracks" secret when the task's
// candidate space contains it.
func NewSimulated(secret string, attemptsPerTask uint64) *SimRunner {
	return &SimRunner{secret: secret, attemptsPerTask: attemptsPerTask}
}

// Run checks whether the task's candidate space contains the secret.
func (s *SimRunner) Run(ctx context.Context, t types.Task, hashFile string, hashMode int) (types.Result, error) {
	res := types.Result{
		TaskID:   t.ID,
		JobID:    t.JobID,
		Method:   methodTag(t),
		Attempts: s.attemptsPerTask,
	}
	if err := ctx.Err(); err != nil {
		return res, err
	}
	switch t.Mode {
	case types.ModeMask:
		if maskMatch(s.secret, t.Payload) {
			res.Found = true
			res.Plaintext = s.secret
		}
	default: // dictionary & rules: payload is a comma-separated candidate list
		list := t.Payload
		if t.Mode == types.ModeRules {
			list, _, _ = strings.Cut(t.Payload, "|")
		}
		for _, cand := range strings.Split(list, ",") {
			if strings.TrimSpace(cand) == s.secret {
				res.Found = true
				res.Plaintext = s.secret
				break
			}
		}
	}
	return res, nil
}

// maskMatch reports whether plain matches a hashcat mask of ?u/?l/?d/?s tokens
// and literal characters.
func maskMatch(plain, mask string) bool {
	var classes []func(rune) bool
	for i := 0; i < len(mask); i++ {
		c := mask[i]
		if c == '?' && i+1 < len(mask) {
			i++
			switch mask[i] {
			case 'u':
				classes = append(classes, func(r rune) bool { return 'A' <= r && r <= 'Z' })
			case 'l':
				classes = append(classes, func(r rune) bool { return 'a' <= r && r <= 'z' })
			case 'd':
				classes = append(classes, func(r rune) bool { return '0' <= r && r <= '9' })
			case 's':
				classes = append(classes, func(r rune) bool {
					return !('a' <= r && r <= 'z' || 'A' <= r && r <= 'Z' || '0' <= r && r <= '9')
				})
			case '?':
				classes = append(classes, func(r rune) bool { return r == '?' })
			default:
				return false // unsupported token
			}
			continue
		}
		lit := rune(c)
		classes = append(classes, func(r rune) bool { return r == lit })
	}
	runes := []rune(plain)
	if len(runes) != len(classes) {
		return false
	}
	for i, r := range runes {
		if !classes[i](r) {
			return false
		}
	}
	return true
}
