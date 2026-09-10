package control

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Polign/vector-transfer/connector"
)

func personalFixture(t *testing.T) (*Connections, string, string) {
	t.Helper()
	root := t.TempDir()
	key := filepath.Join(root, "key")
	if err := os.WriteFile(key, []byte(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{42}, 32))), 0600); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, "connections")
	v, err := OpenConnections(dir, key, "polign-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { v.Close() })
	return v, dir, key
}
func personalDefinition(direction string) ConnectionDefinition {
	return ConnectionDefinition{Label: "My " + direction, Direction: direction, Config: connector.PersonalConfig{Kind: "polign", Endpoint: "https://vectors.example", Collection: direction}, Credentials: connector.PersonalCredentials{APIKey: "highly-sensitive-user-provider-key"}}
}
func personalOwner(subject string) Owner {
	return Owner{ID: "owner-" + subject, Subject: subject, Realm: "polign-test"}
}

func TestPrivateConnectionsAreEncryptedDurableAndBoundToOwner(t *testing.T) {
	v, dir, key := personalFixture(t)
	alice := personalOwner("alice")
	bob := personalOwner("bob")
	a, created, err := v.Create(alice, personalDefinition("source"), "request")
	if err != nil || !created {
		t.Fatal(err)
	}
	b, _, err := v.Create(bob, personalDefinition("source"), "request")
	if err != nil {
		t.Fatal(err)
	}
	if a.Name == b.Name {
		t.Fatal("request key crossed owners")
	}
	if _, created, err := v.Create(alice, personalDefinition("source"), "request"); err != nil || created {
		t.Fatal("idempotent retry failed")
	}
	for _, binding := range []connector.Binding{a, b} {
		data, err := os.ReadFile(filepath.Join(dir, binding.Name))
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(data, []byte("highly-sensitive")) || bytes.Contains(data, []byte("vectors.example")) || json.Valid(data) {
			t.Fatal("connection entry was not fully encrypted")
		}
		info, _ := os.Stat(filepath.Join(dir, binding.Name))
		if info.Mode().Perm() != 0600 {
			t.Fatal("unsafe credential file permissions")
		}
	}
	reopened, err := OpenConnections(dir, key, "polign-test")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, ok := reopened.Get(a.Name)
	if !ok || !got.AllowsRead("alice") || got.AllowsRead("bob") || got.CanWrite {
		t.Fatal("ownership or direction lost across restart")
	}
	if _, err := reopened.Rotate(bob, a.Name, connector.PersonalCredentials{APIKey: "attacker"}); err != ErrNotFound {
		t.Fatal("cross-owner rotation accepted")
	}
	if err := reopened.Delete(bob, a.Name); err != ErrNotFound {
		t.Fatal("cross-owner deletion accepted")
	}
	rotated, err := reopened.Rotate(alice, a.Name, connector.PersonalCredentials{APIKey: "replacement-secret"})
	if err != nil {
		t.Fatal(err)
	}
	if rotated.Fingerprint != a.Fingerprint || rotated.Resource != a.Resource {
		t.Fatal("credential rotation invalidated saved checkpoints")
	}
	otherRealm, err := OpenConnections(dir, key, "other-realm")
	if err != nil {
		t.Fatal(err)
	}
	defer otherRealm.Close()
	if _, ok := otherRealm.Get(a.Name); ok || len(otherRealm.List()) != 0 {
		t.Fatal("connections crossed account realms")
	}
	data, _ := os.ReadFile(filepath.Join(dir, a.Name))
	data[len(data)-1] ^= 1
	if err := os.WriteFile(filepath.Join(dir, a.Name), data, 0600); err != nil {
		t.Fatal(err)
	}
	if broken, err := OpenConnections(dir, key, "polign-test"); err == nil {
		broken.Close()
		t.Fatal("tampered ciphertext accepted")
	}
}

func TestConnectionCiphertextCannotBeMovedAndConcurrentRetriesDeduplicate(t *testing.T) {
	v, dir, key := personalFixture(t)
	owner := personalOwner("alice")
	var wg sync.WaitGroup
	var created atomic.Int32
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, fresh, err := v.Create(owner, personalDefinition("source"), "same-request")
			if err != nil {
				t.Error(err)
			}
			if fresh {
				created.Add(1)
			}
		}()
	}
	wg.Wait()
	if created.Load() != 1 || len(v.List()) != 1 {
		t.Fatal("concurrent requests duplicated credentials")
	}
	a := v.List()[0]
	b, _, err := v.Create(owner, personalDefinition("sink"), "other-request")
	if err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(filepath.Join(dir, a.Name))
	if err := os.WriteFile(filepath.Join(dir, b.Name), data, 0600); err != nil {
		t.Fatal(err)
	}
	if broken, err := OpenConnections(dir, key, "polign-test"); err == nil {
		broken.Close()
		t.Fatal("ciphertext accepted under another connection ID")
	}
}

func TestSelfServiceAPIConfiguresOwnJobsWithoutAdministratorGrants(t *testing.T) {
	v, _, _ := personalFixture(t)
	e := testEngine(t, nil, nil)
	e.Registry = connector.Registry{}
	e.Connections = v
	h := AccountHandler(e, testAccounts{})
	request := func(method, path, user, key string, body any) *httptest.ResponseRecorder {
		payload, _ := json.Marshal(body)
		r := httptest.NewRequest(method, "https://transfer.test"+path, bytes.NewReader(payload))
		r.Header.Set("Authorization", "Bearer "+user)
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Idempotency-Key", key)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	ids := map[string]string{}
	for _, direction := range []string{"source", "sink"} {
		w := request("POST", "/v1/connections", "alice", direction, personalDefinition(direction))
		if w.Code != 201 {
			t.Fatalf("create: %d %s", w.Code, w.Body)
		}
		if strings.Contains(w.Body.String(), "highly-sensitive") {
			t.Fatal("credential leaked in creation response")
		}
		var binding connector.Binding
		if err := json.Unmarshal(w.Body.Bytes(), &binding); err != nil {
			t.Fatal(err)
		}
		ids[direction] = binding.Name
	}
	if w := request("GET", "/v1/connections", "bob", "", nil); strings.Contains(w.Body.String(), ids["source"]) {
		t.Fatal("private connection leaked to another account")
	}
	job := spec()
	job.Source = ids["source"]
	job.Sink = ids["sink"]
	if w := request("POST", "/v1/jobs", "bob", "job", job); w.Code != 403 {
		t.Fatal("another user used private credentials")
	}
	w := request("POST", "/v1/jobs", "alice", "job", job)
	if w.Code != 202 {
		t.Fatalf("own job: %d %s", w.Code, w.Body)
	}
	var submitted Job
	if err := json.Unmarshal(w.Body.Bytes(), &submitted); err != nil {
		t.Fatal(err)
	}
	if w := request("DELETE", "/v1/connections/"+ids["source"], "alice", "", nil); w.Code != 400 {
		t.Fatal("deleted connection while queued work depended on it")
	}
	if w := request("PUT", "/v1/connections/"+ids["source"]+"/credentials", "bob", "", connector.PersonalCredentials{APIKey: "attacker"}); w.Code != 404 {
		t.Fatal("cross-owner API rotation accepted")
	}
	if w := request("PUT", "/v1/connections/"+ids["source"]+"/credentials", "alice", "", connector.PersonalCredentials{APIKey: "replacement-key"}); w.Code != 200 || strings.Contains(w.Body.String(), "replacement-key") {
		t.Fatal("credential rotation failed or leaked")
	}
	for _, body := range []any{
		map[string]any{"label": "x", "direction": "source", "read_subjects": []string{"bob"}},
		map[string]any{"label": "x", "direction": "source", "config": map[string]any{"kind": "polign", "api_key_env": "SERVER_SECRET"}},
	} {
		if w := request("POST", "/v1/connections", "alice", "bad", body); w.Code != 400 {
			t.Fatal("server configuration field accepted from a user")
		}
	}
	// Replace only fixture clients; ownership/fingerprints and the real engine remain intact.
	b := v.bindings[ids["source"]]
	b.Source = sourceFunc(func(context.Context, string, int) (connector.Page, error) {
		return connector.Page{Records: []connector.Record{record("one")}, Done: true}, nil
	})
	v.bindings[ids["source"]] = b
	b = v.bindings[ids["sink"]]
	b.Sink = sinkFunc(func(context.Context, []connector.Record) error { return nil })
	v.bindings[ids["sink"]] = b
	claimed, ok, err := e.Store.Claim()
	if err != nil || !ok {
		t.Fatal("claim failed")
	}
	if err := e.execute(context.Background(), claimed); err != nil {
		t.Fatal(err)
	}
	got, _ := e.Store.Get(submitted.ID)
	if got.State != "succeeded" || got.Records != 1 {
		t.Fatalf("private transfer did not execute: %+v", got)
	}
	journal, err := os.ReadFile(e.Store.file.Name())
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(journal, []byte("highly-sensitive")) || bytes.Contains(journal, []byte("replacement-key")) {
		t.Fatal("credentials leaked into the job journal")
	}
	if w := request("DELETE", "/v1/connections/"+ids["source"], "alice", "", nil); w.Code != 200 {
		t.Fatal("owner could not remove unused credentials")
	}
}
