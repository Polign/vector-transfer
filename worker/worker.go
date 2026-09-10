// Package worker runs customer-owned transfers over outbound HTTPS. Only the
// local configuration may choose endpoints, credentials and approved pairs.
package worker

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/anuptalwalkar/vector-transfer/connector"
	"github.com/anuptalwalkar/vector-transfer/control"
	"github.com/anuptalwalkar/vector-transfer/internal/durable"
)

type Identity struct {
	ID       string `json:"id"`
	URL      string `json:"url"`
	Seed     []byte `json:"seed"`
	Enrolled bool   `json:"enrolled"`
}
type Config struct {
	ConnectionsFile string               `json:"connections_file"`
	Pairs           []control.WorkerPair `json:"pairs"`
}
type Client struct {
	data       string
	lock       *os.File
	Identity   Identity
	HTTP       *http.Client
	token      string
	tokenUntil time.Time
}

func Open(data string) (*Client, error) {
	if err := os.MkdirAll(data, 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(data, "worker.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, errors.New("worker data directory already in use")
	}
	c := &Client{data: data, lock: f, HTTP: &http.Client{Timeout: 20 * time.Second, Transport: &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: 10 * time.Second}).DialContext, TLSHandshakeTimeout: 10 * time.Second, ResponseHeaderTimeout: 18 * time.Second}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	b, err := os.ReadFile(filepath.Join(data, "identity.json"))
	if os.IsNotExist(err) {
		return c, nil
	}
	if err != nil || json.Unmarshal(b, &c.Identity) != nil || len(c.Identity.Seed) != ed25519.SeedSize || len(c.Identity.ID) != 32 {
		f.Close()
		return nil, errors.New("invalid worker identity; restore its original data directory")
	}
	info, err := os.Stat(filepath.Join(data, "identity.json"))
	if err != nil || info.Mode().Perm()&0077 != 0 {
		f.Close()
		return nil, errors.New("worker identity must have mode 0600")
	}
	return c, nil
}
func (c *Client) Close() error { c.HTTP.CloseIdleConnections(); return c.lock.Close() }
func ValidateURL(raw string, allowLocalHTTP bool) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return "", errors.New("control plane must be an HTTPS origin")
	}
	ip := net.ParseIP(u.Hostname())
	local := u.Hostname() == "localhost" || (ip != nil && ip.IsLoopback())
	if u.Scheme != "https" && !(allowLocalHTTP && u.Scheme == "http" && local) {
		return "", errors.New("control plane requires HTTPS; local development requires explicit loopback opt-in")
	}
	return strings.TrimRight(raw, "/"), nil
}
func (c *Client) call(ctx context.Context, path string, body, out any, authenticated bool) error {
	if authenticated {
		if err := c.session(ctx); err != nil {
			return err
		}
	}
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", c.Identity.URL+"/worker/v1"+path, bytes.NewReader(b))
	if err != nil {
		return errors.New("invalid worker request")
	}
	req.Header.Set("Content-Type", "application/json")
	if authenticated {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return errors.New("control plane connection unavailable")
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		if resp.StatusCode == 401 {
			c.token = ""
		}
		return fmt.Errorf("control plane rejected worker request (HTTP %d)", resp.StatusCode)
	}
	dec := json.NewDecoder(io.LimitReader(resp.Body, 256<<10))
	dec.DisallowUnknownFields()
	if err = dec.Decode(out); err != nil {
		return errors.New("invalid control plane response")
	}
	if dec.Decode(new(any)) != io.EOF {
		return errors.New("oversized or invalid control plane response")
	}
	return nil
}
func (c *Client) Enroll(ctx context.Context, rawURL, enrollment string, allowLocalHTTP bool) error {
	base, err := ValidateURL(rawURL, allowLocalHTTP)
	if err != nil {
		return err
	}
	id, token, ok := strings.Cut(strings.TrimSpace(enrollment), ".")
	if !ok || len(id) != 32 || len(token) != 64 {
		return errors.New("invalid enrollment token")
	}
	if _, err = hex.DecodeString(id + token); err != nil {
		return errors.New("invalid enrollment token")
	}
	if c.Identity.ID != "" && (c.Identity.ID != id || c.Identity.URL != base) {
		return errors.New("data directory belongs to another worker or control plane")
	}
	if c.Identity.ID == "" {
		_, key, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			return err
		}
		c.Identity = Identity{ID: id, URL: base, Seed: key.Seed()}
		// Persist the key before enrollment so a lost response is safely retryable.
		if err = durable.Write(filepath.Join(c.data, "identity.json"), c.Identity); err != nil {
			return err
		}
	}
	public := ed25519.NewKeyFromSeed(c.Identity.Seed).Public().(ed25519.PublicKey)
	var result struct {
		Enrolled bool `json:"enrolled"`
	}
	if err = c.call(ctx, "/enroll", map[string]string{"id": id, "token": token, "public_key": base64.StdEncoding.EncodeToString(public)}, &result, false); err != nil {
		return err
	}
	if !result.Enrolled {
		return errors.New("worker enrollment rejected")
	}
	c.Identity.Enrolled = true
	return durable.Write(filepath.Join(c.data, "identity.json"), c.Identity)
}
func (c *Client) session(ctx context.Context) error {
	if c.token != "" && time.Now().Before(c.tokenUntil) {
		return nil
	}
	var challenge struct {
		Challenge string `json:"challenge"`
	}
	if err := c.call(ctx, "/challenge", map[string]string{"id": c.Identity.ID}, &challenge, false); err != nil {
		return err
	}
	if len(challenge.Challenge) != 64 {
		return errors.New("invalid authentication challenge")
	}
	signature := ed25519.Sign(ed25519.NewKeyFromSeed(c.Identity.Seed), control.SessionProof(c.Identity.ID, challenge.Challenge))
	var session struct {
		Token     string `json:"token"`
		ExpiresIn int    `json:"expires_in"`
	}
	if err := c.call(ctx, "/session", map[string]string{"id": c.Identity.ID, "challenge": challenge.Challenge, "signature": base64.StdEncoding.EncodeToString(signature)}, &session, false); err != nil {
		return err
	}
	if len(session.Token) != 64 || session.ExpiresIn < 60 || session.ExpiresIn > 600 {
		return errors.New("invalid worker session")
	}
	c.token = session.Token
	c.tokenUntil = time.Now().Add(time.Duration(session.ExpiresIn-30) * time.Second)
	return nil
}
func LoadConfig(path string) (Config, error) {
	var cfg Config
	f, err := os.Open(path)
	if err != nil {
		return cfg, err
	}
	defer f.Close()
	d := json.NewDecoder(io.LimitReader(f, 1<<20))
	d.DisallowUnknownFields()
	if err = d.Decode(&cfg); err != nil {
		return cfg, err
	}
	if d.Decode(new(any)) != io.EOF {
		return cfg, errors.New("invalid worker config")
	}
	if cfg.ConnectionsFile == "" {
		return cfg, errors.New("connections_file is required")
	}
	if !filepath.IsAbs(cfg.ConnectionsFile) {
		cfg.ConnectionsFile = filepath.Join(filepath.Dir(path), cfg.ConnectionsFile)
	}
	return cfg, nil
}
func (c *Client) opaque(s string) string {
	h := hmac.New(sha256.New, c.Identity.Seed)
	h.Write([]byte(s))
	return hex.EncodeToString(h.Sum(nil))
}
func (c *Client) Manifest(registry connector.Registry, pairs []control.WorkerPair) (control.WorkerManifest, error) {
	m := control.WorkerManifest{Pairs: pairs}
	allowed := map[string]bool{}
	for _, p := range pairs {
		allowed[p.Source] = true
		allowed[p.Sink] = true
	}
	for _, b := range registry.List() {
		if !allowed[b.Name] {
			continue
		}
		resource := b.Resource
		if resource == "" {
			resource = b.Fingerprint
		}
		m.Connections = append(m.Connections, control.WorkerConnection{Name: b.Name, Kind: b.Kind, CanRead: b.Source != nil, CanWrite: b.Sink != nil, Fingerprint: c.opaque(b.Fingerprint), Resource: c.opaque(resource)})
	}
	return m, control.ValidateManifest(m)
}
func (c *Client) Run(ctx context.Context, registry connector.Registry, pairs []control.WorkerPair, allowLocalHTTP bool) error {
	if !c.Identity.Enrolled {
		return errors.New("enroll this worker first")
	}
	if _, err := ValidateURL(c.Identity.URL, allowLocalHTTP); err != nil {
		return err
	}
	manifest, err := c.Manifest(registry, pairs)
	if err != nil {
		return err
	}
	store, err := control.OpenStore(filepath.Join(c.data, "jobs"))
	if err != nil {
		return err
	}
	defer store.Close()
	if err = store.Recover(); err != nil {
		return err
	}
	engine := &control.Engine{Store: store, Registry: registry}
	published := false
	for ctx.Err() == nil {
		if !published {
			var result struct {
				Accepted bool `json:"accepted"`
			}
			err = c.call(ctx, "/manifest", manifest, &result, true)
			published = err == nil && result.Accepted
			if err == nil && !result.Accepted {
				err = errors.New("worker manifest rejected")
			}
		}
		if published {
			var response struct {
				Job *control.WorkerAssignment `json:"job"`
			}
			err = c.call(ctx, "/claim", struct{}{}, &response, true)
			if err == nil && response.Job != nil {
				err = c.execute(ctx, engine, manifest, *response.Job)
			}
		}
		if store.Health() != nil {
			return errors.New("worker checkpoint store unavailable; execution stopped")
		}
		if err != nil {
			log.Print("Worker paused: control plane unavailable or assignment rejected; retrying in 5 seconds")
			published = false
			select {
			case <-ctx.Done():
			case <-time.After(5 * time.Second):
			}
		}
	}
	return nil
}
func validAssignment(a control.WorkerAssignment, id string) bool {
	if len(a.ID) != 32 || len(a.LeaseID) != 64 || a.Spec.WorkerID != id || a.Generation < 0 || a.Records < 0 || a.Batches < 0 {
		return false
	}
	if _, err := hex.DecodeString(a.ID + a.LeaseID); err != nil {
		return false
	}
	s := a.Spec
	return len(s.Name) > 0 && len(s.Name) <= 160 && s.Dimension >= 1 && s.Dimension <= 65536 && s.BatchSize >= 1 && s.BatchSize <= 500 && s.MaxAttempts >= 1 && s.MaxAttempts <= 10 && s.MaxRestarts >= -1 && s.MaxRestarts <= 10
}
func (c *Client) report(ctx context.Context, a control.WorkerAssignment, r control.WorkerReport) (bool, error) {
	var response struct {
		CancelRequested bool       `json:"cancel_requested"`
		LeaseUntil      *time.Time `json:"lease_until"`
	}
	err := c.call(ctx, "/jobs/"+a.ID+"/progress", r, &response, true)
	return response.CancelRequested, err
}
func (c *Client) execute(ctx context.Context, e *control.Engine, m control.WorkerManifest, a control.WorkerAssignment) error {
	if !validAssignment(a, c.Identity.ID) {
		return errors.New("invalid job assignment")
	}
	rejected := func(code string) error {
		_, err := c.report(ctx, a, control.WorkerReport{LeaseID: a.LeaseID, Sequence: 1, State: "failed", Records: a.Records, Batches: a.Batches, Checkpoint: a.Checkpoint, Failure: code})
		return err
	}
	src, dst, err := (control.Worker{Manifest: m}).Connections(a.Spec)
	if err != nil || src.Fingerprint != a.SourceFingerprint || dst.Fingerprint != a.SinkFingerprint {
		return rejected("policy")
	}
	localSource, localSink := e.Registry[a.Spec.Source], e.Registry[a.Spec.Sink]
	j, err := e.Store.PrepareWorkerJob(a, localSource.Fingerprint, localSink.Fingerprint, localSink.Resource)
	if err != nil {
		if e.Store.Health() != nil {
			return err
		}
		return rejected("checkpoint_missing")
	}
	if a.CancelRequested && (j.State == "queued" || j.State == "retry_wait") {
		j, err = e.Action(j.ID, "cancel", "control-plane")
		if err != nil {
			return err
		}
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	// This conservative monotonic timer stops execution even if a progress HTTP
	// request hangs. Leases last 60 seconds server-side; requests time out at 20.
	leaseTimer := time.AfterFunc(35*time.Second, cancel)
	defer leaseTimer.Stop()
	done := make(chan error, 1)
	running := j.State == "queued" || j.State == "retry_wait"
	if running {
		go func() { done <- e.RunJob(runCtx, j.ID) }()
	} else {
		done <- nil
	}
	joined := false
	defer func() {
		cancel()
		if !joined {
			<-done
		}
	}()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	sequence := int64(0)
	for {
		finished := false
		select {
		case err = <-done:
			joined = true
			finished = true
			if err != nil {
				return err
			}
		case <-ticker.C:
		case <-runCtx.Done():
			return runCtx.Err()
		}
		current, err := e.Store.Get(j.ID)
		if err != nil {
			return err
		}
		sequence++
		r := control.WorkerReport{LeaseID: a.LeaseID, Sequence: sequence, State: current.State, Phase: current.Phase, Records: current.Records, Batches: current.Batches, Retries: current.Retries, Restarts: current.Restarts, Checkpoint: current.LastCheckpointAt != nil}
		if current.NextRunAt != nil {
			r.RetryAfter = max(1, min(3600, int(time.Until(*current.NextRunAt).Seconds())+1))
		}
		if current.State == "failed" {
			r.Failure = "transfer"
		}
		start := time.Now()
		cancelRequested, err := c.report(runCtx, a, r)
		if err != nil {
			return err
		}
		// A terminal snapshot may race the executor returning. Join before claiming
		// anything else, including when the final response was lost.
		if current.State != "running" && current.State != "cancel_requested" {
			return nil
		}
		if cancelRequested {
			if _, err = e.Action(j.ID, "cancel", "control-plane"); err != nil {
				return err
			}
		}
		if !leaseTimer.Stop() || runCtx.Err() != nil {
			return errors.New("execution lease expired")
		}
		leaseTimer.Reset(max(time.Millisecond, 35*time.Second-time.Since(start)))
		if finished {
			return nil
		}
	}
}
