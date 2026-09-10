// Package control owns durable job state and execution. Connectors never write
// job state; a cursor advances only after the sink acknowledges the whole page.
package control

import (
	"bufio"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"
)

var ErrNotFound = errors.New("job not found")
var ErrConflict = errors.New("operation conflicts with current job state")

type Spec struct {
	WorkerID    string `json:"worker_id,omitempty"`
	Name        string `json:"name"`
	Source      string `json:"source"`
	Sink        string `json:"sink"`
	Dimension   int    `json:"dimension"`
	BatchSize   int    `json:"batch_size"`
	MaxAttempts int    `json:"max_attempts"`
	MaxRestarts int    `json:"max_restarts,omitempty"`
}

type Owner struct {
	ID      string `json:"id"`
	Subject string `json:"subject"`
	Realm   string `json:"realm"`
}

type Job struct {
	LeaseID           string     `json:"lease_id,omitempty"`
	LeaseUntil        *time.Time `json:"lease_until,omitempty"`
	Generation        int        `json:"generation,omitempty"`
	ReportSequence    int64      `json:"report_sequence,omitempty"`
	Owner             *Owner     `json:"owner,omitempty"`
	Phase             string     `json:"phase,omitempty"`
	Restarts          int        `json:"restarts,omitempty"`
	NextRunAt         *time.Time `json:"next_run_at,omitempty"`
	LastCheckpointAt  *time.Time `json:"last_checkpoint_at,omitempty"`
	ID                string     `json:"id"`
	Spec              Spec       `json:"spec"`
	State             string     `json:"state"`
	CreatedAt         time.Time  `json:"created_at"`
	UpdatedAt         time.Time  `json:"updated_at"`
	CreatedBy         string     `json:"created_by"`
	Records           int64      `json:"records"`
	Batches           int64      `json:"batches"`
	Retries           int        `json:"retries"`
	Runs              int        `json:"runs"`
	Cursor            string     `json:"cursor,omitempty"`
	SourceExhausted   bool       `json:"source_exhausted"`
	Error             string     `json:"error,omitempty"`
	SourceFingerprint string     `json:"source_fingerprint"`
	SinkFingerprint   string     `json:"sink_fingerprint"`
	SinkResource      string     `json:"sink_resource"`
	IdempotencyKey    string     `json:"idempotency_key,omitempty"`
}

type Event struct {
	Sequence     int64     `json:"sequence"`
	At           time.Time `json:"at"`
	JobID        string    `json:"job_id"`
	Kind         string    `json:"kind"`
	Actor        string    `json:"actor"`
	Message      string    `json:"message"`
	Records      int64     `json:"records"`
	BatchSHA256  string    `json:"batch_sha256,omitempty"`
	PreviousHash string    `json:"previous_hash"`
	Hash         string    `json:"hash"`
}

type entry struct {
	Event Event `json:"event"`
	Job   Job   `json:"job"`
}

// Store is a single-process append journal. Each state transition and its audit
// event occupy one fsynced line. A file lock prevents two servers sharing it.
type Store struct {
	mu       sync.Mutex
	file     *os.File
	jobs     map[string]Job
	sequence int64
	hash     string
	poison   error
}

func OpenStore(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, "journal.jsonl"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, errors.New("data directory is already in use")
	}
	s := &Store{file: f, jobs: map[string]Job{}}
	if err := s.replay(); err != nil {
		f.Close()
		return nil, err
	}
	// fsync the containing directory so initial journal creation is durable.
	d, err := os.Open(dir)
	if err != nil {
		f.Close()
		return nil, err
	}
	err = d.Sync()
	d.Close()
	if err != nil {
		f.Close()
		return nil, err
	}
	return s, nil
}

func entryHash(e entry) string {
	e.Event.Hash = ""
	b, _ := json.Marshal(e)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func (s *Store) replay() error {
	r := bufio.NewReader(s.file)
	var offset int64
	for {
		line, err := r.ReadBytes('\n')
		if err == io.EOF {
			if len(line) > 0 {
				if err := s.file.Truncate(offset); err != nil {
					return err
				}
				if err := s.file.Sync(); err != nil {
					return err
				}
			}
			break
		}
		if err != nil {
			return err
		}
		var e entry
		if json.Unmarshal(line, &e) != nil || e.Event.Sequence != s.sequence+1 || e.Event.PreviousHash != s.hash || e.Event.Hash != entryHash(e) || e.Event.JobID != e.Job.ID {
			return fmt.Errorf("journal integrity check failed at byte %d", offset)
		}
		s.jobs[e.Job.ID] = e.Job
		s.sequence = e.Event.Sequence
		s.hash = e.Event.Hash
		offset += int64(len(line))
	}
	_, err := s.file.Seek(0, io.SeekEnd)
	return err
}

func (s *Store) append(j Job, kind, actor, message, digest string) error {
	if s.poison != nil {
		return s.poison
	}
	j.UpdatedAt = time.Now().UTC()
	if j.State == "canceled" {
		kind = "canceled"
	}
	e := entry{Job: j, Event: Event{Sequence: s.sequence + 1, At: j.UpdatedAt, JobID: j.ID, Kind: kind, Actor: actor, Message: message, Records: j.Records, BatchSHA256: digest, PreviousHash: s.hash}}
	e.Event.Hash = entryHash(e)
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	n, err := s.file.Write(b)
	if err == nil && n != len(b) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = s.file.Sync()
	}
	if err != nil {
		s.poison = fmt.Errorf("job journal write failed: %w", err)
		return s.poison
	}
	s.jobs[j.ID] = j
	s.sequence = e.Event.Sequence
	s.hash = e.Event.Hash
	return nil
}

func (s *Store) Close() error  { s.mu.Lock(); defer s.mu.Unlock(); return s.file.Close() }
func (s *Store) Health() error { s.mu.Lock(); defer s.mu.Unlock(); return s.poison }

func (s *Store) Create(spec Spec, sourceFP, sinkFP, sinkResource, key, actor string, owners ...Owner) (Job, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.poison != nil {
		return Job{}, false, s.poison
	}
	var owner *Owner
	if len(owners) > 0 {
		o := owners[0]
		owner = &o
	}
	if key != "" {
		for _, j := range s.jobs {
			if j.IdempotencyKey == key && sameOwner(j.Owner, owner) {
				if j.Spec != spec || j.SourceFingerprint != sourceFP || j.SinkFingerprint != sinkFP {
					return Job{}, false, ErrConflict
				}
				return j, false, nil
			}
		}
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return Job{}, false, err
	}
	j := Job{Owner: owner, ID: hex.EncodeToString(id[:]), Spec: spec, State: "queued", CreatedAt: time.Now().UTC(), CreatedBy: actor, SourceFingerprint: sourceFP, SinkFingerprint: sinkFP, SinkResource: sinkResource, IdempotencyKey: key}
	if err := s.append(j, "submitted", actor, "Job submitted", ""); err != nil {
		return Job{}, false, err
	}
	return s.jobs[j.ID], true, nil
}

func (s *Store) Get(id string) (Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok {
		return Job{}, ErrNotFound
	}
	return j, nil
}

func (s *Store) List() []Job {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := make([]Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		list = append(list, j)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].CreatedAt.After(list[j].CreatedAt) })
	return list
}

func (s *Store) Update(id, kind, actor, message, digest string, fn func(*Job) error) (Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok {
		return Job{}, ErrNotFound
	}
	if err := fn(&j); err != nil {
		return Job{}, err
	}
	if err := s.append(j, kind, actor, message, digest); err != nil {
		return Job{}, err
	}
	return s.jobs[id], nil
}

func (s *Store) Recover() error {
	for _, j := range s.List() {
		if j.Spec.WorkerID != "" || (j.State != "running" && j.State != "cancel_requested") {
			continue
		}
		_, err := s.Update(j.ID, "recovered", "worker", "Recovered interrupted job from its last acknowledged checkpoint", "", func(j *Job) error {
			if j.State == "cancel_requested" {
				j.State = "canceled"
			} else {
				j.State = "queued"
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) Claim() (Job, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.poison != nil {
		return Job{}, false, s.poison
	}
	busy := map[string]bool{}
	for _, j := range s.jobs {
		if j.State == "running" || j.State == "cancel_requested" {
			busy[j.SinkResource] = true
		}
	}
	var picked Job
	for _, j := range s.jobs {
		if j.Spec.WorkerID == "" && (j.State == "queued" || (j.State == "retry_wait" && j.NextRunAt != nil && !j.NextRunAt.After(time.Now()))) && !busy[j.SinkResource] && (picked.ID == "" || j.CreatedAt.Before(picked.CreatedAt)) {
			picked = j
		}
	}
	if picked.ID == "" {
		return Job{}, false, nil
	}
	picked.State = "running"
	picked.Phase = "starting"
	picked.NextRunAt = nil
	picked.Runs++
	picked.Error = ""
	if err := s.append(picked, "started", "worker", "Transfer execution started", ""); err != nil {
		return Job{}, false, err
	}
	return s.jobs[picked.ID], true, nil
}

// Events is bounded on output and uses a fixed snapshot of journal length.
func (s *Store) Events(id string, after int64, limit int) ([]Event, error) {
	s.mu.Lock()
	if _, ok := s.jobs[id]; !ok {
		s.mu.Unlock()
		return nil, ErrNotFound
	}
	info, err := s.file.Stat()
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	r := bufio.NewReader(io.NewSectionReader(s.file, 0, info.Size()))
	s.mu.Unlock()
	events := make([]Event, 0)
	for len(events) < limit {
		line, err := r.ReadBytes('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		var e entry
		if err := json.Unmarshal(line, &e); err != nil {
			return nil, err
		}
		if e.Job.ID == id && e.Event.Sequence > after {
			events = append(events, e.Event)
		}
	}
	return events, nil
}

func sameOwner(a, b *Owner) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}
