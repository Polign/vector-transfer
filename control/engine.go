package control

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	"github.com/anuptalwalkar/vector-transfer/connector"
)

type Engine struct {
	WorkerDownloads string
	HostedDisabled  bool
	Store           *Store
	Registry        connector.Registry
	AccountRealm    string
	Connections     *Connections
	Workers         *Workers
}

func (e *Engine) Submit(spec Spec, key, actor string, owners ...Owner) (Job, bool, error) {
	if e.AccountRealm != "" {
		if len(owners) != 1 || owners[0].Realm != e.AccountRealm || owners[0].ID == "" || owners[0].Subject == "" {
			return Job{}, false, ErrForbidden
		}
		src, _ := e.connection(spec.Source)
		dst, _ := e.connection(spec.Sink)
		if spec.WorkerID == "" && (!src.AllowsRead(owners[0].Subject) || !dst.AllowsWrite(owners[0].Subject)) {
			return Job{}, false, ErrForbidden
		}
	} else if len(owners) != 0 {
		return Job{}, false, ErrForbidden
	}
	if e.HostedDisabled && spec.WorkerID == "" {
		return Job{}, false, errors.New("hosted execution is disabled; select a customer worker")
	}
	spec.Name = strings.TrimSpace(spec.Name)
	if spec.Name == "" || len(spec.Name) > 160 {
		return Job{}, false, errors.New("name must contain 1–160 bytes")
	}
	if spec.Dimension < 1 || spec.Dimension > 65536 {
		return Job{}, false, errors.New("dimension must be between 1 and 65536")
	}
	if spec.BatchSize == 0 {
		spec.BatchSize = 100
	}
	if spec.BatchSize < 1 || spec.BatchSize > 500 {
		return Job{}, false, errors.New("batch_size must be between 1 and 500")
	}
	if spec.MaxAttempts == 0 {
		spec.MaxAttempts = 5
	}
	if spec.MaxAttempts < 1 || spec.MaxAttempts > 10 {
		return Job{}, false, errors.New("max_attempts must be between 1 and 10")
	}
	if spec.MaxRestarts == 0 {
		spec.MaxRestarts = 3
	}
	if spec.MaxRestarts < -1 || spec.MaxRestarts > 10 {
		return Job{}, false, errors.New("max_restarts must be -1 (disabled) or 0–10 (0 defaults to 3)")
	}
	if len(key) > 128 {
		return Job{}, false, errors.New("idempotency key must not exceed 128 bytes")
	}
	if spec.WorkerID != "" {
		if e.Workers == nil {
			return Job{}, false, ErrForbidden
		}
		var owner *Owner
		if len(owners) > 0 {
			owner = &owners[0]
		}
		worker, err := e.Workers.Get(spec.WorkerID, owner)
		if err != nil {
			return Job{}, false, err
		}
		src, dst, err := worker.Connections(spec)
		if err != nil {
			return Job{}, false, err
		}
		return e.Store.Create(spec, src.Fingerprint, dst.Fingerprint, "worker:"+worker.ID+":"+dst.Resource, key, actor, owners...)
	}
	src, ok := e.connection(spec.Source)
	if !ok || src.Source == nil {
		return Job{}, false, errors.New("unknown or unreadable source connection")
	}
	dst, ok := e.connection(spec.Sink)
	if !ok || dst.Sink == nil {
		return Job{}, false, errors.New("unknown or unwritable sink connection")
	}
	if spec.Source == spec.Sink || src.Fingerprint == dst.Fingerprint || (src.Resource != "" && src.Resource == dst.Resource) {
		return Job{}, false, errors.New("source and sink must be different connections")
	}
	resource := dst.Resource
	if resource == "" {
		resource = dst.Fingerprint
	}
	return e.Store.Create(spec, src.Fingerprint, dst.Fingerprint, resource, key, actor, owners...)
}

func (e *Engine) Action(id, action, actor string) (Job, error) {
	return e.Store.Update(id, action, actor, "Operator requested "+action, "", func(j *Job) error {
		switch action {
		case "cancel":
			switch j.State {
			case "queued", "retry_wait":
				j.State = "canceled"
				j.Phase = ""
				j.NextRunAt = nil
				j.Error = ""
			case "running", "cancel_requested":
				j.State = "cancel_requested"
			default:
				return ErrConflict
			}
		case "resume":
			if j.State != "failed" && j.State != "canceled" && j.State != "retry_wait" {
				return ErrConflict
			}
			j.State = "queued"
			if j.Spec.WorkerID != "" {
				j.Generation++
				j.LeaseID = ""
				j.LeaseUntil = nil
				j.ReportSequence = 0
			}
			j.Phase = ""
			j.NextRunAt = nil
			j.Restarts = 0
			j.Error = ""
		default:
			return errors.New("unknown action")
		}
		return nil
	})
}

func (e *Engine) Run(ctx context.Context, workers int) error {
	if workers < 1 || workers > 32 {
		return errors.New("workers must be between 1 and 32")
	}
	if err := e.Store.Recover(); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				j, ok, err := e.Store.Claim()
				if err != nil {
					errs <- err
					cancel()
					return
				}
				if !ok {
					select {
					case <-ctx.Done():
						return
					case <-time.After(150 * time.Millisecond):
					}
					continue
				}
				if err := e.execute(ctx, j); err != nil {
					errs <- err
					cancel()
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) execute(ctx context.Context, j Job) error {
	if !e.authorized(j) {
		return e.finish(j.ID, "failed", "Connection access was revoked or the account realm changed")
	}
	src, sok := e.connection(j.Spec.Source)
	dst, dok := e.connection(j.Spec.Sink)
	if !sok || !dok || src.Source == nil || dst.Sink == nil || src.Fingerprint != j.SourceFingerprint || dst.Fingerprint != j.SinkFingerprint {
		return e.finish(j.ID, "failed", "Connection configuration changed; restore it or submit a new job")
	}
	for {
		current, err := e.Store.Get(j.ID)
		if err != nil {
			return err
		}
		if current.State == "cancel_requested" {
			return e.finish(j.ID, "canceled", "")
		}
		if ctx.Err() != nil {
			return e.finish(j.ID, "queued", "")
		}
		if current.SourceExhausted {
			return e.finish(j.ID, "succeeded", "Source scan was already fully acknowledged")
		}
		if !e.authorized(current) {
			return e.finish(j.ID, "failed", "Connection access was revoked")
		}
		src, sok = e.connection(current.Spec.Source)
		dst, dok = e.connection(current.Spec.Sink)
		if !sok || !dok || src.Source == nil || dst.Sink == nil || src.Fingerprint != current.SourceFingerprint || dst.Fingerprint != current.SinkFingerprint {
			return e.finish(j.ID, "failed", "Connection configuration changed; restore it or submit a new job")
		}
		var page connector.Page
		err = e.retry(ctx, current, "read", func() error {
			var err error
			page, err = src.Source.Read(ctx, current.Cursor, current.Spec.BatchSize)
			return err
		})
		if err == nil && len(page.Records) > current.Spec.BatchSize {
			err = errors.New("source exceeded the requested page size")
		}
		if err == nil && !page.Done && (page.Next == "" || page.Next == current.Cursor) {
			err = errors.New("source pagination did not advance")
		}
		if err == nil {
			err = connector.Validate(page.Records, j.Spec.Dimension)
		}
		if err == nil && len(page.Records) > 0 {
			err = e.retry(ctx, current, "write", func() error { return dst.Sink.Upsert(ctx, page.Records) })
		}
		if err != nil {
			if health := e.Store.Health(); health != nil {
				return health
			}
			if ctx.Err() != nil {
				return e.finish(j.ID, "queued", "")
			}
			state, getErr := e.Store.Get(j.ID)
			if getErr != nil {
				return getErr
			}
			if state.State == "cancel_requested" {
				return e.finish(j.ID, "canceled", "")
			}
			if connector.Retryable(err) && state.Restarts < state.Spec.MaxRestarts {
				next := time.Now().UTC().Add(30 * time.Second * time.Duration(1<<min(state.Restarts, 5)))
				_, updateErr := e.Store.Update(j.ID, "restart_scheduled", "worker", "Temporary provider failure; restarting from the saved checkpoint", "", func(j *Job) error {
					if j.State == "cancel_requested" {
						j.State = "canceled"
						return nil
					}
					j.State = "retry_wait"
					j.Phase = "waiting"
					j.Restarts++
					j.NextRunAt = &next
					j.Error = "Temporary provider failure"
					return nil
				})
				return updateErr
			}
			return e.finish(j.ID, "failed", "Transfer failed; check connection permissions, availability, and data compatibility, then resume")
		}
		b, err := json.Marshal(page.Records)
		if err != nil {
			return e.finish(j.ID, "failed", "Could not encode batch receipt")
		}
		hash := sha256.Sum256(b)
		kind := "checkpoint"
		if page.Done {
			kind = "succeeded"
		}
		_, err = e.Store.Update(j.ID, kind, "worker", fmt.Sprintf("Acknowledged %d records", len(page.Records)), hex.EncodeToString(hash[:]), func(j *Job) error {
			now := time.Now().UTC()
			j.LastCheckpointAt = &now
			j.Phase = "checkpointed"
			j.Cursor = page.Next
			j.SourceExhausted = page.Done
			j.Records += int64(len(page.Records))
			if len(page.Records) > 0 {
				j.Batches++
			}
			if j.State == "cancel_requested" {
				j.State = "canceled"
			} else if page.Done {
				j.State = "succeeded"
			}
			return nil
		})
		if err != nil {
			return err
		}
		if page.Done {
			return nil
		}
		state, err := e.Store.Get(j.ID)
		if err != nil {
			return err
		}
		if state.State == "canceled" {
			return nil
		}
	}
}

func (e *Engine) retry(ctx context.Context, j Job, operation string, fn func() error) error {
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		state, err := e.Store.Get(j.ID)
		if err != nil {
			return err
		}
		if state.State == "cancel_requested" {
			return errors.New("cancel requested")
		}
		_, phaseErr := e.Store.Update(j.ID, "phase", "worker", "Starting "+operation+" attempt", "", func(j *Job) error { j.Phase = operation; return nil })
		if phaseErr != nil {
			return phaseErr
		}
		err = fn()
		if err == nil || !connector.Retryable(err) || attempt >= j.Spec.MaxAttempts {
			return err
		}
		_, persistErr := e.Store.Update(j.ID, "retry", "worker", fmt.Sprintf("Retrying %s after temporary failure on attempt %d", operation, attempt), "", func(j *Job) error { j.Retries++; j.Phase = "backoff"; return nil })
		if persistErr != nil {
			return persistErr
		}
		delay := min(5*time.Second, 200*time.Millisecond*time.Duration(1<<min(attempt-1, 5))) + time.Duration(rand.IntN(100))*time.Millisecond
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}

func (e *Engine) finish(id, state, message string) error {
	_, err := e.Store.Update(id, state, "worker", message, "", func(j *Job) error {
		j.Phase = ""
		j.NextRunAt = nil
		if j.State == "cancel_requested" {
			j.State = "canceled"
			j.Error = ""
		} else {
			j.State = state
			j.Error = message
		}
		return nil
	})
	return err
}

var ErrForbidden = errors.New("connection access denied")

func (e *Engine) authorized(j Job) bool {
	if j.Spec.WorkerID != "" {
		if e.Workers == nil || (e.AccountRealm != "" && (j.Owner == nil || j.Owner.Realm != e.AccountRealm)) {
			return false
		}
		worker, err := e.Workers.Get(j.Spec.WorkerID, j.Owner)
		if err != nil {
			return false
		}
		src, dst, err := worker.Connections(j.Spec)
		return err == nil && src.Fingerprint == j.SourceFingerprint && dst.Fingerprint == j.SinkFingerprint
	}

	if e.AccountRealm == "" {
		return j.Owner == nil
	}
	src, _ := e.connection(j.Spec.Source)
	dst, _ := e.connection(j.Spec.Sink)
	return j.Owner != nil && j.Owner.Realm == e.AccountRealm && src.AllowsRead(j.Owner.Subject) && dst.AllowsWrite(j.Owner.Subject)
}
