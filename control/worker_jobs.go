package control

import (
	"context"
	"errors"
	"time"
)

const WorkerLeaseDuration = 60 * time.Second

// WorkerReport accepts only progress summaries. All error messages are generated
// server-side; no raw provider errors, payload digests or cursors cross this API.
type WorkerReport struct {
	LeaseID    string `json:"lease_id"`
	Sequence   int64  `json:"sequence"`
	State      string `json:"state"`
	Phase      string `json:"phase"`
	Records    int64  `json:"records"`
	Batches    int64  `json:"batches"`
	Retries    int    `json:"retries"`
	Restarts   int    `json:"restarts"`
	Checkpoint bool   `json:"checkpoint"`
	RetryAfter int    `json:"retry_after,omitempty"`
	Failure    string `json:"failure,omitempty"`
}
type WorkerAssignment struct {
	ID                string    `json:"id"`
	Spec              Spec      `json:"spec"`
	LeaseID           string    `json:"lease_id"`
	LeaseUntil        time.Time `json:"lease_until"`
	Generation        int       `json:"generation"`
	Records           int64     `json:"records"`
	Batches           int64     `json:"batches"`
	Checkpoint        bool      `json:"checkpoint"`
	SourceFingerprint string    `json:"source_fingerprint"`
	SinkFingerprint   string    `json:"sink_fingerprint"`
	CancelRequested   bool      `json:"cancel_requested"`
}

func assignment(j Job) WorkerAssignment {
	return WorkerAssignment{ID: j.ID, Spec: j.Spec, LeaseID: j.LeaseID, LeaseUntil: *j.LeaseUntil, Generation: j.Generation, Records: j.Records, Batches: j.Batches, Checkpoint: j.LastCheckpointAt != nil, SourceFingerprint: j.SourceFingerprint, SinkFingerprint: j.SinkFingerprint, CancelRequested: j.State == "cancel_requested"}
}

func (s *Store) ClaimWorker(w Worker) (*WorkerAssignment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.poison != nil {
		return nil, s.poison
	}
	now := time.Now().UTC()
	var pick Job
	for _, j := range s.jobs {
		if j.Spec.WorkerID != w.ID || !sameOwner(j.Owner, w.Owner) {
			continue
		}
		if j.LeaseUntil != nil && j.LeaseUntil.After(now) && (j.State == "running" || j.State == "cancel_requested") {
			return nil, nil
		}
		eligible := j.State == "queued" || ((j.State == "running" || j.State == "cancel_requested") && (j.LeaseUntil == nil || !j.LeaseUntil.After(now))) || (j.State == "retry_wait" && j.NextRunAt != nil && !j.NextRunAt.After(now))
		if eligible && (pick.ID == "" || j.CreatedAt.Before(pick.CreatedAt)) {
			pick = j
		}
	}
	if pick.ID == "" {
		return nil, nil
	}
	src, dst, err := w.Connections(pick.Spec)
	if err != nil || src.Fingerprint != pick.SourceFingerprint || dst.Fingerprint != pick.SinkFingerprint {
		pick.State = "failed"
		pick.Error = "Worker connection policy changed; restore it or submit a new job"
		pick.LeaseID = ""
		pick.LeaseUntil = nil
		return nil, s.append(pick, "failed", "worker", pick.Error, "")
	}
	if pick.State != "cancel_requested" {
		pick.State = "running"
	}
	pick.Phase = "starting"
	pick.Runs++
	pick.ReportSequence = 0
	pick.NextRunAt = nil
	pick.Error = ""
	pick.LeaseID = randomID(32)
	until := now.Add(WorkerLeaseDuration)
	pick.LeaseUntil = &until
	if err := s.append(pick, "worker_leased", "worker:"+w.ID, "Customer worker acquired execution lease", ""); err != nil {
		return nil, err
	}
	a := assignment(pick)
	return &a, nil
}
func (s *Store) ReportWorker(w Worker, id string, r WorkerReport) (Job, error) {
	switch r.State {
	case "queued", "running", "retry_wait", "succeeded", "failed", "canceled", "cancel_requested":
	default:
		return Job{}, errors.New("invalid worker state")
	}
	switch r.Phase {
	case "", "starting", "read", "write", "backoff", "checkpointed", "waiting":
	default:
		return Job{}, errors.New("invalid worker phase")
	}
	if r.Sequence < 1 || r.Records < 0 || r.Batches < 0 || r.Retries < 0 || r.Restarts < 0 || r.Restarts > 10 || r.RetryAfter < 0 || r.RetryAfter > 3600 {
		return Job{}, errors.New("invalid worker progress")
	}
	failure := ""
	switch r.Failure {
	case "":
	case "policy":
		failure = "Worker rejected the job: local connection policy or configuration changed"
	case "checkpoint_missing":
		failure = "Worker checkpoint is missing or behind reported progress; restore the original worker data directory"
	case "transfer":
		failure = "Transfer failed; inspect the customer worker and database permissions before resuming"
	default:
		return Job{}, errors.New("invalid worker failure code")
	}
	if failure != "" && r.State != "failed" {
		return Job{}, errors.New("failure requires failed state")
	}
	return s.Update(id, "worker_progress", "worker:"+w.ID, "Customer worker reported transfer progress", "", func(j *Job) error {
		if j.Spec.WorkerID != w.ID || !sameOwner(j.Owner, w.Owner) {
			return ErrNotFound
		}
		if j.LeaseID != r.LeaseID || j.LeaseUntil == nil || !j.LeaseUntil.After(time.Now()) || r.Sequence <= j.ReportSequence {
			return ErrConflict
		}
		if j.State != "running" && j.State != "cancel_requested" {
			return ErrConflict
		}
		if r.Records < j.Records || r.Batches < j.Batches {
			return ErrConflict
		}
		if r.Records > j.Records && !r.Checkpoint {
			return errors.New("progress requires a durable local checkpoint")
		}
		cancelRequested := j.State == "cancel_requested"
		if r.Checkpoint && (j.LastCheckpointAt == nil || r.Records > j.Records || r.Batches > j.Batches) {
			now := time.Now().UTC()
			j.LastCheckpointAt = &now
		}
		j.Records = r.Records
		j.Batches = r.Batches
		j.Retries = r.Retries
		j.Restarts = r.Restarts
		j.ReportSequence = r.Sequence
		j.Phase = r.Phase
		j.Error = failure
		until := time.Now().UTC().Add(WorkerLeaseDuration)
		j.LeaseUntil = &until
		j.State = r.State
		if cancelRequested {
			if r.State == "running" || r.State == "cancel_requested" {
				j.State = "cancel_requested"
			} else {
				j.State = "canceled"
			}
		}
		if j.State == "retry_wait" {
			next := time.Now().UTC().Add(time.Duration(max(1, r.RetryAfter)) * time.Second)
			j.NextRunAt = &next
		} else {
			j.NextRunAt = nil
		}
		if j.State != "running" && j.State != "cancel_requested" {
			j.LeaseID = ""
			j.LeaseUntil = nil
		}
		return nil
	})
}

// PrepareWorkerJob imports only an assignment validated against local policy.
// Checkpoints and local configuration fingerprints are never supplied by the server.
func (s *Store) PrepareWorkerJob(a WorkerAssignment, sourceFP, sinkFP, resource string) (Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	localSpec := a.Spec
	localSpec.WorkerID = ""
	if j, ok := s.jobs[a.ID]; ok {
		if j.Spec != localSpec || j.SourceFingerprint != sourceFP || j.SinkFingerprint != sinkFP || a.Generation < j.Generation {
			return Job{}, ErrConflict
		}
		if j.Records < a.Records || j.Batches < a.Batches || (a.Checkpoint && j.LastCheckpointAt == nil) {
			return Job{}, errors.New("checkpoint missing")
		}
		if a.Generation > j.Generation {
			j.Generation = a.Generation
			j.State = "queued"
			j.Restarts = 0
			j.Error = ""
			j.NextRunAt = nil
			if err := s.append(j, "resumed", "worker", "Control plane requested resume", ""); err != nil {
				return Job{}, err
			}
		}
		return s.jobs[a.ID], nil
	}
	if a.Checkpoint || a.Records > 0 || a.Batches > 0 {
		return Job{}, errors.New("checkpoint missing")
	}
	j := Job{ID: a.ID, Spec: localSpec, Generation: a.Generation, State: "queued", CreatedAt: time.Now().UTC(), CreatedBy: "control-plane", SourceFingerprint: sourceFP, SinkFingerprint: sinkFP, SinkResource: resource}
	if err := s.append(j, "submitted", "worker", "Accepted locally approved transfer", ""); err != nil {
		return Job{}, err
	}
	return s.jobs[a.ID], nil
}

// RunJob executes exactly one locally stored job. Unlike Run it never scans for
// work: a customer worker must hold a remote lease before invoking it.
func (e *Engine) RunJob(ctx context.Context, id string) error {
	j, err := e.Store.Update(id, "started", "worker", "Transfer execution started", "", func(j *Job) error {
		if j.Spec.WorkerID != "" || (j.State != "queued" && j.State != "retry_wait") {
			return ErrConflict
		}
		j.State = "running"
		j.Phase = "starting"
		j.Runs++
		j.NextRunAt = nil
		return nil
	})
	if err != nil {
		return err
	}
	return e.execute(ctx, j)
}
