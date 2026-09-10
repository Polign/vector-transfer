package control

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Polign/vector-transfer/connector"
)

var ErrVaultUnavailable = errors.New("connection credential store unavailable")

type ConnectionDefinition struct {
	Label       string                        `json:"label"`
	Direction   string                        `json:"direction"`
	Config      connector.PersonalConfig      `json:"config"`
	Credentials connector.PersonalCredentials `json:"credentials"`
}

type personalEntry struct {
	ID            string               `json:"id"`
	Owner         Owner                `json:"owner"`
	Definition    ConnectionDefinition `json:"definition"`
	RequestDigest string               `json:"request_digest"`
	CreatedAt     time.Time            `json:"created_at"`
}

// Connections stores authenticated users' private connections independently of
// the job journal. Entire entries are AES-256-GCM encrypted with filename-bound
// associated data. The key is injected by the host, never supplied by API callers.
type Connections struct {
	mu         sync.RWMutex
	dir, realm string
	key        []byte
	aead       cipher.AEAD
	entries    map[string]personalEntry
	bindings   connector.Registry
	poison     error
}

var personalID = regexp.MustCompile(`^personal-[a-f0-9]{32}$`)

func OpenConnections(dir, keyFile, realm string) (*Connections, error) {
	encoded, err := os.ReadFile(keyFile)
	if err != nil {
		return nil, ErrVaultUnavailable
	}
	key, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(encoded)))
	if err != nil || len(key) != 32 || realm == "" {
		return nil, errors.New("credential key must be base64-encoded 32 random bytes and requires account mode")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, ErrVaultUnavailable
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, ErrVaultUnavailable
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, ErrVaultUnavailable
	}
	v := &Connections{dir: dir, realm: realm, key: key, aead: aead, entries: map[string]personalEntry{}, bindings: connector.Registry{}}
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, ErrVaultUnavailable
	}
	for _, file := range files {
		if strings.HasPrefix(file.Name(), ".connection-") {
			continue
		} // Uncommitted temporary file from an interrupted write.
		if file.IsDir() || !personalID.MatchString(file.Name()) {
			v.Close()
			return nil, ErrVaultUnavailable
		}
		info, err := file.Info()
		if err != nil || !info.Mode().IsRegular() || info.Size() > 128<<10 {
			v.Close()
			return nil, ErrVaultUnavailable
		}
		data, err := os.ReadFile(filepath.Join(dir, file.Name()))
		if err != nil {
			v.Close()
			return nil, ErrVaultUnavailable
		}
		entry, err := v.open(file.Name(), data)
		if err != nil {
			v.Close()
			return nil, err
		}
		binding, err := personalBinding(entry)
		if err != nil {
			v.Close()
			return nil, ErrVaultUnavailable
		}
		v.entries[entry.ID] = entry
		v.bindings[entry.ID] = binding
		if len(v.entries) > 2000 {
			v.Close()
			return nil, ErrVaultUnavailable
		}
	}
	return v, nil
}

func (v *Connections) mac(value []byte) string {
	m := hmac.New(sha256.New, v.key)
	_, _ = m.Write(value)
	return hex.EncodeToString(m.Sum(nil))
}
func (v *Connections) open(id string, data []byte) (personalEntry, error) {
	var entry personalEntry
	if len(data) < 1+v.aead.NonceSize() || data[0] != 1 {
		return entry, ErrVaultUnavailable
	}
	plaintext, err := v.aead.Open(nil, data[1:1+v.aead.NonceSize()], data[1+v.aead.NonceSize():], []byte("vector-transfer/connection/"+id))
	if err != nil || json.Unmarshal(plaintext, &entry) != nil || entry.ID != id || entry.Owner.ID == "" || entry.Owner.Subject == "" || entry.Owner.Realm == "" {
		return personalEntry{}, ErrVaultUnavailable
	}
	return entry, nil
}

func personalBinding(entry personalEntry) (connector.Binding, error) {
	d := entry.Definition
	if strings.TrimSpace(d.Label) == "" || len(d.Label) > 128 || (d.Direction != "source" && d.Direction != "sink") {
		return connector.Binding{}, errors.New("provide a connection name and choose source or destination")
	}
	b, err := connector.OpenPersonal(d.Config, d.Credentials)
	if err != nil {
		return connector.Binding{}, err
	}
	b.Name = entry.ID
	b.Label = d.Label
	b.Personal = true
	if d.Direction == "source" {
		b.Sink = nil
		b.CanWrite = false
		b.ReadSubjects = []string{entry.Owner.Subject}
	} else {
		b.Source = nil
		b.CanRead = false
		b.WriteSubjects = []string{entry.Owner.Subject}
	}
	b.Fingerprint = connector.Fingerprint([]string{b.Fingerprint, d.Direction})
	return b, nil
}

func (v *Connections) persist(entry personalEntry) error {
	plaintext, err := json.Marshal(entry)
	if err != nil {
		return ErrVaultUnavailable
	}
	nonce := make([]byte, v.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return ErrVaultUnavailable
	}
	data := append([]byte{1}, nonce...)
	data = v.aead.Seal(data, nonce, plaintext, []byte("vector-transfer/connection/"+entry.ID))
	f, err := os.CreateTemp(v.dir, ".connection-")
	if err != nil {
		return ErrVaultUnavailable
	}
	name := f.Name()
	defer os.Remove(name)
	if _, err = f.Write(data); err != nil {
		f.Close()
		return ErrVaultUnavailable
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return ErrVaultUnavailable
	}
	if err = f.Close(); err != nil {
		return ErrVaultUnavailable
	}
	if err = os.Rename(name, filepath.Join(v.dir, entry.ID)); err != nil {
		return ErrVaultUnavailable
	}
	dir, err := os.Open(v.dir)
	if err != nil {
		v.poison = ErrVaultUnavailable
		return v.poison
	}
	defer dir.Close()
	if err = dir.Sync(); err != nil {
		v.poison = ErrVaultUnavailable
		return v.poison
	}
	return nil
}

func (v *Connections) Create(owner Owner, definition ConnectionDefinition, key string) (connector.Binding, bool, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.poison != nil {
		return connector.Binding{}, false, v.poison
	}
	if owner.ID == "" || owner.Subject == "" || owner.Realm != v.realm {
		return connector.Binding{}, false, ErrForbidden
	}
	if key == "" || len(key) > 128 {
		return connector.Binding{}, false, errors.New("a connection idempotency key of 1–128 bytes is required")
	}
	definition.Label = strings.TrimSpace(definition.Label)
	encoded, _ := json.Marshal(definition)
	id := "personal-" + v.mac([]byte(owner.ID + "\x00" + key))[:32]
	requestDigest := v.mac(encoded)
	if entry, ok := v.entries[id]; ok {
		if !sameOwner(&entry.Owner, &owner) || entry.RequestDigest != requestDigest {
			return connector.Binding{}, false, ErrConflict
		}
		return v.bindings[id], false, nil
	}
	count := 0
	for _, entry := range v.entries {
		if sameOwner(&entry.Owner, &owner) {
			count++
		}
	}
	if count >= 50 || len(v.entries) >= 2000 {
		return connector.Binding{}, false, errors.New("saved connection limit reached; remove unused connections")
	}
	entry := personalEntry{ID: id, Owner: owner, Definition: definition, RequestDigest: requestDigest, CreatedAt: time.Now().UTC()}
	binding, err := personalBinding(entry)
	if err != nil {
		return connector.Binding{}, false, err
	}
	if err := v.persist(entry); err != nil {
		connector.Registry{id: binding}.Close()
		return connector.Binding{}, false, err
	}
	v.entries[id] = entry
	v.bindings[id] = binding
	return binding, true, nil
}

func (v *Connections) Get(id string) (connector.Binding, bool) {
	if v == nil {
		return connector.Binding{}, false
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	entry, ok := v.entries[id]
	if !ok || entry.Owner.Realm != v.realm || v.poison != nil {
		return connector.Binding{}, false
	}
	return v.bindings[id], true
}
func (v *Connections) List() []connector.Binding {
	list := []connector.Binding{}
	if v == nil {
		return list
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.poison != nil {
		return list
	}
	for id, entry := range v.entries {
		if entry.Owner.Realm == v.realm {
			list = append(list, v.bindings[id])
		}
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Label < list[j].Label })
	return list
}
func (v *Connections) Rotate(owner Owner, id string, credentials connector.PersonalCredentials) (connector.Binding, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.poison != nil {
		return connector.Binding{}, v.poison
	}
	entry, ok := v.entries[id]
	if !ok || !sameOwner(&entry.Owner, &owner) || owner.Realm != v.realm {
		return connector.Binding{}, ErrNotFound
	}
	entry.Definition.Credentials = credentials
	binding, err := personalBinding(entry)
	if err != nil {
		return connector.Binding{}, err
	}
	if err := v.persist(entry); err != nil {
		connector.Registry{id: binding}.Close()
		return connector.Binding{}, err
	}
	old := v.bindings[id]
	v.entries[id] = entry
	v.bindings[id] = binding
	_ = connector.Registry{id: old}.Close()
	return binding, nil
}
func (v *Connections) Delete(owner Owner, id string) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.poison != nil {
		return v.poison
	}
	entry, ok := v.entries[id]
	if !ok || !sameOwner(&entry.Owner, &owner) || owner.Realm != v.realm {
		return ErrNotFound
	}
	if err := os.Remove(filepath.Join(v.dir, id)); err != nil {
		return ErrVaultUnavailable
	}
	dir, err := os.Open(v.dir)
	if err != nil {
		v.poison = ErrVaultUnavailable
		return v.poison
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		v.poison = ErrVaultUnavailable
		return v.poison
	}
	_ = connector.Registry{id: v.bindings[id]}.Close()
	delete(v.bindings, id)
	delete(v.entries, id)
	return nil
}
func (v *Connections) Health() error {
	if v == nil {
		return nil
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.poison
}
func (v *Connections) Close() error { v.mu.Lock(); defer v.mu.Unlock(); return v.bindings.Close() }

// Lookup combines immutable operator connections with the separate synchronized
// personal store. Dynamic configuration never mutates the shared registry map.
func (e *Engine) connection(name string) (connector.Binding, bool) {
	if b, ok := e.Registry[name]; ok {
		return b, true
	}
	return e.Connections.Get(name)
}
func (e *Engine) connectionList() []connector.Binding {
	return append(e.Registry.List(), e.Connections.List()...)
}

var _ io.Closer = (*Connections)(nil)
