// Package types defines the shared domain contracts for aegis-swarm.
// Authorized security-research use only.
package types

type AttackMode string

const (
	ModeDictionary AttackMode = "dictionary"
	ModeMask       AttackMode = "mask"
	ModeRules      AttackMode = "rules"
)

// Event topics on the collaboration bus.
const (
	TopicDiscovery    = "cracking.discovery"
	TopicBreakthrough = "cracking.breakthrough"
	TopicDeadEnd      = "cracking.dead_end"
	TopicMutate       = "cracking.mutate"
	TopicHalt         = "cracking.halt"
)

// Job statuses.
const (
	StatusQueued    = "queued"
	StatusRunning   = "running"
	StatusSucceeded = "succeeded"
	StatusExhausted = "exhausted"
	StatusHalted    = "halted"
)

type Phase struct {
	Type      AttackMode `json:"type"`
	Wordlist  string     `json:"wordlist,omitempty"`
	Mask      string     `json:"mask,omitempty"`
	RuleFile  string     `json:"rule_file,omitempty"`
	ChunkSize int        `json:"chunk_size"`
	Priority  int        `json:"priority"`
}

type Plan struct {
	JobID    string  `json:"job_id"`
	Strategy string  `json:"strategy"`
	Phases   []Phase `json:"phases"`
}

type Target struct {
	ESSID    string `json:"essid"`
	BSSID    string `json:"bssid"`
	HashFile string `json:"hash_file"`
	HashMode int    `json:"hash_mode"`
}

type Task struct {
	ID      string     `json:"id"`
	JobID   string     `json:"job_id"`
	Phase   int        `json:"phase"`
	Mode    AttackMode `json:"mode"`
	Payload string     `json:"payload"`
	Skip    uint64     `json:"skip"`
	Limit   uint64     `json:"limit"`
}

type Result struct {
	TaskID    string `json:"task_id"`
	JobID     string `json:"job_id"`
	Found     bool   `json:"found"`
	Plaintext string `json:"plaintext,omitempty"`
	Method    string `json:"method"`
	Attempts  uint64 `json:"attempts"`
	WorkerID  string `json:"worker_id"`
}

type Event struct {
	Topic   string `json:"topic"`
	JobID   string `json:"job_id"`
	Payload string `json:"payload"`
}

type JobState struct {
	JobID         string `json:"job_id"`
	Status        string `json:"status"`
	TotalAttempts uint64 `json:"total_attempts"`
	DeadEnds      int    `json:"dead_ends"`
	StartedUnix   int64  `json:"started_unix"`
	Plaintext     string `json:"plaintext,omitempty"`
}
