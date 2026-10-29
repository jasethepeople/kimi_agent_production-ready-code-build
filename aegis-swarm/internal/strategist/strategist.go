// Package strategist generates attack plans for aegis-swarm.
// Authorized security-research use only. Pure functions, no I/O.
package strategist

import (
	"regexp"
	"sort"
	"strconv"
	"strings"

	"aegis-swarm/internal/types"
)

const defaultWordlist = "wordlists/default.txt"

// DefaultPlan builds the standard multi-phase plan for a target.
func DefaultPlan(jobID string, t types.Target, wordlist string) types.Plan {
	if wordlist == "" {
		wordlist = defaultWordlist
	}
	return types.Plan{
		JobID:    jobID,
		Strategy: "default-hybrid",
		Phases: []types.Phase{
			{Type: types.ModeDictionary, Wordlist: wordlist, ChunkSize: 50000, Priority: 1},
			{Type: types.ModeMask, Mask: "?u?l?l?l?l?d?d?d", ChunkSize: 50000, Priority: 2},
			{Type: types.ModeRules, Wordlist: wordlist, RuleFile: "rules/best64.rule", ChunkSize: 50000, Priority: 3},
			{Type: types.ModeMask, Mask: "?u?l?l?l?l?d?d?s", ChunkSize: 50000, Priority: 4},
		},
	}
}

var yearSuffixRe = regexp.MustCompile(`^[A-Za-z]+(19|20)[0-9]{2}[^A-Za-z0-9]+$`)

func isLetters(s string) bool {
	for _, r := range s {
		if !('a' <= r && r <= 'z' || 'A' <= r && r <= 'Z') {
			return false
		}
	}
	return len(s) > 0
}

func isDigits(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return len(s) > 0
}

func isAlnum(s string) bool {
	for _, r := range s {
		if !('a' <= r && r <= 'z' || 'A' <= r && r <= 'Z' || '0' <= r && r <= '9') {
			return false
		}
	}
	return len(s) > 0
}

func hasSymbol(s string) bool {
	for _, r := range s {
		if !('a' <= r && r <= 'z' || 'A' <= r && r <= 'Z' || '0' <= r && r <= '9') {
			return true
		}
	}
	return false
}

// ClassifyPattern returns a short tag describing the shape of a candidate,
// e.g. "word+yyyy+sym", "8char-alnum", "all-lower", "all-digit", "mixed+sym".
func ClassifyPattern(candidate string) string {
	if candidate == "" {
		return "empty"
	}
	if isDigits(candidate) {
		return "all-digit"
	}
	if isLetters(candidate) {
		if candidate == strings.ToLower(candidate) {
			return "all-lower"
		}
		if candidate == strings.ToUpper(candidate) {
			return "all-upper"
		}
		return "all-alpha"
	}
	if yearSuffixRe.MatchString(candidate) {
		return "word+yyyy+sym"
	}
	// letters followed by digits only, e.g. "hunter2"
	if i := strings.IndexFunc(candidate, func(r rune) bool { return r >= '0' && r <= '9' }); i > 0 {
		if isLetters(candidate[:i]) && isDigits(candidate[i:]) {
			return "word+digits"
		}
	}
	if isAlnum(candidate) {
		if len(candidate) == 8 {
			return "8char-alnum"
		}
		return "alnum"
	}
	if hasSymbol(candidate) {
		return "mixed+sym"
	}
	return "other"
}

// dominantClass returns the most frequent class tag. Ties broken
// lexicographically for determinism.
func dominantClass(counts map[string]int) string {
	type kv struct {
		k string
		v int
	}
	var items []kv
	for k, v := range counts {
		items = append(items, kv{k, v})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].v != items[j].v {
			return items[i].v > items[j].v
		}
		return items[i].k < items[j].k
	})
	if len(items) == 0 {
		return ""
	}
	return items[0].k
}

var jobRoundRe = regexp.MustCompile(`-([0-9]+)$`)

// mutationRound infers the pivot round from a jobID numeric suffix, else 1.
func mutationRound(jobID string) int {
	m := jobRoundRe.FindStringSubmatch(jobID)
	if m == nil {
		return 1
	}
	n, err := strconv.Atoi(m[1])
	if err != nil || n < 1 {
		return 1
	}
	return n
}

// Mutate generates a new plan that avoids the most-frequent failed
// candidate classes observed in deadEnds. Deterministic for identical input.
func Mutate(jobID string, t types.Target, deadEnds []string) types.Plan {
	counts := map[string]int{}
	for _, d := range deadEnds {
		counts[ClassifyPattern(d)]++
	}
	dom := dominantClass(counts)
	round := mutationRound(jobID)

	var phases []types.Phase
	switch dom {
	case "8char-alnum", "alnum":
		// 8-char alnum space exhausted: pivot to longer masks with symbols.
		phases = []types.Phase{
			{Type: types.ModeMask, Mask: "?u?l?l?l?l?l?l?d?d?s", ChunkSize: 50000, Priority: 1},
			{Type: types.ModeMask, Mask: "?l?l?l?l?l?l?l?l?d?s", ChunkSize: 50000, Priority: 2},
			{Type: types.ModeMask, Mask: "?u?l?l?l?l?l?d?d?d?s", ChunkSize: 50000, Priority: 3},
		}
	case "word+yyyy+sym":
		// word+year dead-ends: year-increment 2020-2030 plus leetspeak.
		prio := 1
		for y := 2020; y <= 2030; y++ {
			phases = append(phases, types.Phase{
				Type:      types.ModeRules,
				Wordlist:  defaultWordlist,
				RuleFile:  "rules/year-" + strconv.Itoa(y) + ".rule",
				ChunkSize: 50000, Priority: prio,
			})
			prio++
		}
		phases = append(phases, types.Phase{
			Type:      types.ModeRules,
			Wordlist:  defaultWordlist,
			RuleFile:  "rules/leetspeak.rule",
			ChunkSize: 50000, Priority: prio,
		})
	case "all-lower", "all-alpha", "all-upper", "word+digits":
		// pure-word failures: force upper+digit+symbol structure.
		phases = []types.Phase{
			{Type: types.ModeMask, Mask: "?u?l?l?l?l?d?d?d?s", ChunkSize: 50000, Priority: 1},
			{Type: types.ModeMask, Mask: "?u?l?l?l?l?l?d?s", ChunkSize: 50000, Priority: 2},
			{Type: types.ModeRules, Wordlist: defaultWordlist, RuleFile: "rules/leetspeak.rule", ChunkSize: 50000, Priority: 3},
		}
	case "all-digit":
		// PIN-style failures: mixed alnum masks.
		phases = []types.Phase{
			{Type: types.ModeMask, Mask: "?u?l?l?l?l?d?d?d", ChunkSize: 50000, Priority: 1},
			{Type: types.ModeMask, Mask: "?l?l?l?l?d?d?d?d", ChunkSize: 50000, Priority: 2},
		}
	default:
		// unknown / no dead-ends: broaden with symbol-bearing masks.
		phases = []types.Phase{
			{Type: types.ModeMask, Mask: "?u?l?l?l?l?d?d?s", ChunkSize: 50000, Priority: 1},
			{Type: types.ModeMask, Mask: "?u?l?l?l?l?l?d?d?s", ChunkSize: 50000, Priority: 2},
		}
	}
	return types.Plan{
		JobID:    jobID,
		Strategy: "genetic-pivot-" + strconv.Itoa(round),
		Phases:   phases,
	}
}
