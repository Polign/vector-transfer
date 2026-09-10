package control

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/anuptalwalkar/vector-transfer/internal/durable"
)

// WorkerConnection contains deliberately limited, non-secret discovery data.
// Endpoints, secret references, cursors and vector payloads are not accepted.
type WorkerConnection struct {
	Name        string `json:"name"`
	Kind        string `json:"kind"`
	CanRead     bool   `json:"can_read"`
	CanWrite    bool   `json:"can_write"`
	Fingerprint string `json:"fingerprint"`
	Resource    string `json:"resource"`
}
type WorkerPair struct {
	Source    string `json:"source"`
	Sink      string `json:"sink"`
	Dimension int    `json:"dimension"`
}
type WorkerManifest struct {
	Connections []WorkerConnection `json:"connections"`
	Pairs       []WorkerPair       `json:"pairs"`
}
type Worker struct {
	ID              string         `json:"id"`
	Name            string         `json:"name"`
	Owner           *Owner         `json:"owner,omitempty"`
	PublicKey       string         `json:"public_key,omitempty"`
	EnrollmentHash  string         `json:"enrollment_hash,omitempty"`
	EnrollmentUntil time.Time      `json:"enrollment_until"`
	CreatedAt       time.Time      `json:"created_at"`
	LastSeen        *time.Time     `json:"last_seen,omitempty"`
	Revoked         bool           `json:"revoked"`
	Manifest        WorkerManifest `json:"manifest"`
}
type WorkerView struct {
	ID       string         `json:"id"`
	Name     string         `json:"name"`
	Status   string         `json:"status"`
	LastSeen *time.Time     `json:"last_seen,omitempty"`
	Manifest WorkerManifest `json:"manifest"`
}
type workerSession struct {
	WorkerID string
	Until    time.Time
}
type workerChallenge struct {
	WorkerID string
	Until    time.Time
}
type Workers struct {
	mu         sync.Mutex
	path       string
	lock       *os.File
	entries    map[string]Worker
	challenges map[string]workerChallenge
	sessions   map[string]workerSession
	poison     error
}

var workerName = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,128}$`)
var digestPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)
var workerIDPattern = regexp.MustCompile(`^[a-f0-9]{32}$`)
var ErrWorkersUnavailable = errors.New("worker registry unavailable")

func randomID(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}
func tokenDigest(s string) string { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }
func OpenWorkers(dir string) (*Workers, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, "lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, errors.New("worker registry already in use")
	}
	w := &Workers{path: filepath.Join(dir, "workers.json"), lock: f, entries: map[string]Worker{}, challenges: map[string]workerChallenge{}, sessions: map[string]workerSession{}}
	b, err := os.ReadFile(w.path)
	if err == nil {
		err = json.Unmarshal(b, &w.entries)
	} else if os.IsNotExist(err) {
		err = nil
	}
	if err != nil || w.entries == nil {
		f.Close()
		return nil, ErrWorkersUnavailable
	}
	return w, nil
}
func (w *Workers) Close() error { return w.lock.Close() }
func (w *Workers) Health() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.poison
}
func (w *Workers) save() error {
	if w.poison != nil {
		return ErrWorkersUnavailable
	}
	if err := durable.Write(w.path, w.entries); err != nil {
		w.poison = err
		return ErrWorkersUnavailable
	}
	return nil
}
func (w *Workers) Create(owner *Owner, name string) (WorkerView, string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !workerName.MatchString(name) {
		return WorkerView{}, "", errors.New("worker name must use 1–128 letters, digits, dots, underscores or hyphens")
	}
	count := 0
	for _, e := range w.entries {
		if sameOwner(e.Owner, owner) && !e.Revoked {
			count++
		}
	}
	if count >= 20 || len(w.entries) >= 1000 {
		return WorkerView{}, "", errors.New("worker registration limit reached")
	}
	now := time.Now().UTC()
	token := randomID(32)
	e := Worker{ID: randomID(16), Name: name, CreatedAt: now, EnrollmentUntil: now.Add(10 * time.Minute), EnrollmentHash: tokenDigest(token)}
	if owner != nil {
		o := *owner
		e.Owner = &o
	}
	w.entries[e.ID] = e
	if err := w.save(); err != nil {
		return WorkerView{}, "", err
	}
	return workerView(e), e.ID + "." + token, nil
}
func workerView(e Worker) WorkerView {
	status := "offline"
	if e.PublicKey == "" {
		status = "awaiting enrollment"
		if time.Now().After(e.EnrollmentUntil) {
			status = "enrollment expired"
		}
	} else if e.LastSeen != nil && time.Since(*e.LastSeen) < 45*time.Second {
		status = "online"
	}
	if e.Revoked {
		status = "revoked"
	}
	return WorkerView{ID: e.ID, Name: e.Name, Status: status, LastSeen: e.LastSeen, Manifest: e.Manifest}
}
func (w *Workers) List(owner *Owner) []WorkerView {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := []WorkerView{}
	for _, e := range w.entries {
		if sameOwner(e.Owner, owner) {
			out = append(out, workerView(e))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
func (w *Workers) Get(id string, owner *Owner) (Worker, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.poison != nil {
		return Worker{}, ErrWorkersUnavailable
	}
	e, ok := w.entries[id]
	if !ok || !sameOwner(e.Owner, owner) {
		return Worker{}, ErrNotFound
	}
	if e.Revoked {
		return Worker{}, ErrForbidden
	}
	return e, nil
}
func (w *Workers) Revoke(id string, owner *Owner) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	e, ok := w.entries[id]
	if !ok || !sameOwner(e.Owner, owner) {
		return ErrNotFound
	}
	e.Revoked = true
	e.EnrollmentHash = ""
	w.entries[id] = e
	return w.save()
}
func (w *Workers) Enroll(id, token, key string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.poison != nil {
		return ErrWorkersUnavailable
	}
	e, ok := w.entries[id]
	pub, err := base64.StdEncoding.DecodeString(key)
	if !ok || e.Revoked || err != nil || len(pub) != ed25519.PublicKeySize {
		return ErrForbidden
	}
	// A response lost after the durable binding can be retried with the same key.
	if e.PublicKey == key {
		return nil
	}
	if e.PublicKey != "" || time.Now().After(e.EnrollmentUntil) || tokenDigest(token) != e.EnrollmentHash {
		return ErrForbidden
	}
	e.PublicKey = key
	e.EnrollmentHash = ""
	w.entries[id] = e
	return w.save()
}
func (w *Workers) Challenge(id string) (string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.poison != nil {
		return "", ErrWorkersUnavailable
	}
	e, ok := w.entries[id]
	if !ok || e.Revoked || e.PublicKey == "" {
		return "", ErrForbidden
	}
	now := time.Now()
	for k, c := range w.challenges {
		if now.After(c.Until) || c.WorkerID == id {
			delete(w.challenges, k)
		}
	}
	if len(w.challenges) >= 1000 {
		return "", errors.New("worker authentication busy; retry shortly")
	}
	c := randomID(32)
	w.challenges[c] = workerChallenge{id, now.Add(time.Minute)}
	return c, nil
}

// SessionProof is domain-separated; signatures cannot enroll or authorize jobs.
func SessionProof(id, challenge string) []byte {
	return []byte("vector-transfer-worker-session-v1\n" + id + "\n" + challenge)
}
func (w *Workers) Session(id, challenge, signature string) (string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.poison != nil {
		return "", ErrWorkersUnavailable
	}
	c, ok := w.challenges[challenge]
	delete(w.challenges, challenge)
	e := w.entries[id]
	sig, err := base64.StdEncoding.DecodeString(signature)
	pub, _ := base64.StdEncoding.DecodeString(e.PublicKey)
	if !ok || c.WorkerID != id || time.Now().After(c.Until) || e.Revoked || err != nil || len(pub) != ed25519.PublicKeySize || !ed25519.Verify(pub, SessionProof(id, challenge), sig) {
		return "", ErrForbidden
	}
	now := time.Now()
	for k, s := range w.sessions {
		if now.After(s.Until) || s.WorkerID == id {
			delete(w.sessions, k)
		}
	}
	token := randomID(32)
	w.sessions[tokenDigest(token)] = workerSession{id, now.Add(10 * time.Minute)}
	return token, nil
}
func (w *Workers) Authenticate(token string) (Worker, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.poison != nil {
		return Worker{}, ErrWorkersUnavailable
	}
	s, ok := w.sessions[tokenDigest(token)]
	e := w.entries[s.WorkerID]
	if !ok || time.Now().After(s.Until) || e.Revoked {
		return Worker{}, ErrForbidden
	}
	return e, nil
}
func ValidateManifest(m WorkerManifest) error {
	if len(m.Connections) == 0 || len(m.Connections) > 50 || len(m.Pairs) == 0 || len(m.Pairs) > 100 {
		return errors.New("worker requires 1–50 connections and 1–100 approved pairs")
	}
	names := map[string]WorkerConnection{}
	for _, c := range m.Connections {
		if !workerName.MatchString(c.Name) || !workerName.MatchString(c.Kind) || !digestPattern.MatchString(c.Fingerprint) || !digestPattern.MatchString(c.Resource) || names[c.Name].Name != "" {
			return errors.New("invalid worker connection")
		}
		names[c.Name] = c
	}
	for _, p := range m.Pairs {
		s, d := names[p.Source], names[p.Sink]
		if !s.CanRead || !d.CanWrite || p.Source == p.Sink || s.Resource == d.Resource || p.Dimension < 1 || p.Dimension > 65536 {
			return errors.New("invalid approved connection pair")
		}
	}
	return nil
}
func (w *Workers) Publish(id string, m WorkerManifest) error {
	if err := ValidateManifest(m); err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	e, ok := w.entries[id]
	if !ok || e.Revoked {
		return ErrForbidden
	}
	e.Manifest = m
	now := time.Now().UTC()
	e.LastSeen = &now
	w.entries[id] = e
	return w.save()
}
func (w *Workers) Touch(id string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	e, ok := w.entries[id]
	if !ok || e.Revoked {
		return ErrForbidden
	}
	now := time.Now().UTC()
	e.LastSeen = &now
	w.entries[id] = e
	return w.save()
}
func (e Worker) Connections(spec Spec) (WorkerConnection, WorkerConnection, error) {
	var src, dst WorkerConnection
	for _, c := range e.Manifest.Connections {
		if c.Name == spec.Source {
			src = c
		}
		if c.Name == spec.Sink {
			dst = c
		}
	}
	for _, p := range e.Manifest.Pairs {
		if p.Source == spec.Source && p.Sink == spec.Sink && p.Dimension == spec.Dimension && src.CanRead && dst.CanWrite {
			return src, dst, nil
		}
	}
	return src, dst, errors.New("source, destination and dimension must match a locally approved worker pair")
}
