package strategist

import (
	"testing"

	"aegis-swarm/internal/types"
)

var target = types.Target{ESSID: "lab", BSSID: "aa:bb", HashFile: "t.hc22000", HashMode: 22000}

func TestDefaultPlan(t *testing.T) {
	p := DefaultPlan("job-1", target, "rockyou.txt")
	if p.Strategy != "default-hybrid" {
		t.Fatalf("strategy = %q", p.Strategy)
	}
	if len(p.Phases) != 4 {
		t.Fatalf("phases = %d", len(p.Phases))
	}
	if p.Phases[0].Type != types.ModeDictionary || p.Phases[0].Wordlist != "rockyou.txt" ||
		p.Phases[0].ChunkSize != 50000 || p.Phases[0].Priority != 1 {
		t.Fatalf("phase 0 wrong: %+v", p.Phases[0])
	}
	if p.Phases[1].Type != types.ModeMask || p.Phases[1].Mask != "?u?l?l?l?l?d?d?d" || p.Phases[1].Priority != 2 {
		t.Fatalf("phase 1 wrong: %+v", p.Phases[1])
	}
	if p.Phases[2].Type != types.ModeRules || p.Phases[2].RuleFile != "rules/best64.rule" ||
		p.Phases[2].Wordlist != "rockyou.txt" || p.Phases[2].Priority != 3 {
		t.Fatalf("phase 2 wrong: %+v", p.Phases[2])
	}
	if p.Phases[3].Type != types.ModeMask || p.Phases[3].Mask != "?u?l?l?l?l?d?d?s" || p.Phases[3].Priority != 4 {
		t.Fatalf("phase 3 wrong: %+v", p.Phases[3])
	}
}

func TestClassifyPattern(t *testing.T) {
	cases := map[string]string{
		"Summer2024!": "word+yyyy+sym",
		"winter1999?": "word+yyyy+sym",
		"a1b2c3d4":    "8char-alnum",
		"Ab3xY9zq":    "8char-alnum",
		"password":    "all-lower",
		"PASSWORD":    "all-upper",
		"Password":    "all-alpha",
		"12345678":    "all-digit",
		"hunter2":     "word+digits",
		"P@ss w0rd!":  "mixed+sym",
		"a1b2c3d4e5":  "alnum",
		"!@#$%":       "mixed+sym",
		"":            "empty",
	}
	for in, want := range cases {
		if got := ClassifyPattern(in); got != want {
			t.Errorf("ClassifyPattern(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestMutateAvoidsAlnumDominant(t *testing.T) {
	dead := []string{"a1b2c3d4", "x9y8z7w6", "Ab3xY9zq", "q1w2e3r4", "mnbv1234"}
	p := Mutate("job-1", target, dead)
	if p.Strategy != "genetic-pivot-1" {
		t.Fatalf("strategy = %q", p.Strategy)
	}
	for _, ph := range p.Phases {
		if ph.Type != types.ModeMask {
			t.Fatalf("expected mask phases, got %+v", ph)
		}
		// must avoid bare 8-char alnum space: longer and/or symbol-bearing
		if len(ph.Mask)/2 < 9 {
			t.Fatalf("mask too short: %q", ph.Mask)
		}
		if !contains(ph.Mask, "?s") {
			t.Fatalf("mask lacks symbol class: %q", ph.Mask)
		}
	}
}

func TestMutateYearDominant(t *testing.T) {
	dead := []string{"Summer2024!", "Winter2023!", "Spring2020?", "monday2021!"}
	p := Mutate("job", target, dead)
	if p.Strategy != "genetic-pivot-1" {
		t.Fatalf("strategy = %q", p.Strategy)
	}
	var yearRules, leet bool
	for _, ph := range p.Phases {
		if ph.Type != types.ModeRules {
			t.Fatalf("expected rules phases, got %+v", ph)
		}
		if len(ph.RuleFile) > 11 && ph.RuleFile[:11] == "rules/year-" {
			yearRules = true
		}
		if ph.RuleFile == "rules/leetspeak.rule" {
			leet = true
		}
	}
	if !yearRules || !leet {
		t.Fatalf("expected year-increment and leetspeak rules: %+v", p.Phases)
	}
}

func TestMutateDeterministicAndRound(t *testing.T) {
	dead := []string{"123456", "000000", "999999"}
	a := Mutate("job-7", target, dead)
	b := Mutate("job-7", target, dead)
	if a.Strategy != "genetic-pivot-7" {
		t.Fatalf("round inference: %q", a.Strategy)
	}
	if len(a.Phases) != len(b.Phases) {
		t.Fatal("non-deterministic phase count")
	}
	for i := range a.Phases {
		if a.Phases[i] != b.Phases[i] {
			t.Fatalf("non-deterministic phase %d", i)
		}
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
