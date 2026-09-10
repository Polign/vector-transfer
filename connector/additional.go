package connector

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/netip"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func isAdditional(kind string) bool {
	switch kind {
	case "weaviate", "milvus", "elasticsearch", "opensearch", "pgvector", "chroma", "redis", "mongodb", "faiss", "solr", "qdrant":
		return true
	}
	return false
}

var fieldName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`)
var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func additionalDefaults(c PersonalConfig) PersonalConfig {
	c.Endpoint = strings.TrimRight(c.Endpoint, "/")
	if c.Kind == "mongodb" && c.AuthDatabase == "" {
		c.AuthDatabase = "admin"
	}
	if c.VectorField == "" {
		c.VectorField = "vector"
	}
	if c.IDField == "" {
		c.IDField = "id"
	}
	if c.MetadataField == "" {
		c.MetadataField = "metadata"
	}
	if c.IDType == "" {
		c.IDType = "string"
		if c.Kind == "milvus" {
			c.IDType = "int64"
		}
		if c.Kind == "mongodb" {
			c.IDType = "objectid"
		}
	}
	if c.Kind == "chroma" {
		if c.Tenant == "" {
			c.Tenant = "default_tenant"
		}
		if c.Database == "" {
			c.Database = "default_database"
		}
	}
	if c.Kind == "milvus" && c.Database == "" {
		c.Database = "default"
	}
	if c.Kind == "redis" && c.Database == "" {
		c.Database = "0"
	}
	return c
}
func validateAdditional(c PersonalConfig, s PersonalCredentials, public bool) error {
	for _, value := range []string{c.Endpoint, c.Collection, c.Namespace, c.Index, c.Database, c.AuthDatabase, c.Tenant, c.VectorField, c.IDField, c.MetadataField, c.KeyPrefix, c.IDType} {
		if len(value) > 2048 || strings.ContainsAny(value, "\r\n\x00") {
			return errors.New("invalid connection setting")
		}
	}
	for _, value := range []string{s.APIKey, s.Username, s.Password} {
		if len(value) > 16384 || strings.ContainsAny(value, "\r\n\x00") {
			return errors.New("invalid credential format")
		}
	}
	if s.AccessKeyID != "" || s.SecretAccessKey != "" || s.SessionToken != "" || c.Region != "" || c.Bucket != "" || c.DistanceMetric != "" {
		return errors.New("unsupported provider settings")
	}
	if s.APIKey != "" && (s.Username != "" || s.Password != "") {
		return errors.New("choose API key or username/password authentication")
	}
	if (s.Username != "" && s.Password == "") || (s.Password != "" && s.Username == "" && c.Kind != "redis") {
		return errors.New("username and password are required together")
	}
	if public && s.APIKey == "" && s.Password == "" {
		return errors.New("database credentials are required")
	}
	u, err := url.Parse(c.Endpoint)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return errors.New("endpoint must be an origin without credentials, query, or path")
	}
	switch c.Kind {
	case "pgvector":
		if u.Scheme != "postgresql" {
			return errors.New("pgvector requires a postgresql://host:port endpoint")
		}
	case "redis":
		if u.Scheme != "rediss" && !(u.Scheme == "redis" && !public && s.Password == "") {
			return errors.New("Redis requires a rediss://host:port endpoint")
		}
	case "mongodb":
		if u.Scheme != "mongodb" && u.Scheme != "mongodb+srv" {
			return errors.New("MongoDB requires a mongodb:// or mongodb+srv:// endpoint")
		}
	default:
		if u.Scheme != "https" && !(u.Scheme == "http" && !public && s.APIKey == "" && s.Password == "") {
			return errors.New("authenticated HTTP connections require HTTPS")
		}
	}
	if public {
		if strings.EqualFold(u.Hostname(), "localhost") || strings.HasSuffix(strings.ToLower(u.Hostname()), ".localhost") {
			return errors.New("private network endpoints are not supported")
		}
		if ip, err := netip.ParseAddr(u.Hostname()); err == nil && !publicAddress(ip) {
			return errors.New("private network endpoints are not supported")
		}
	}
	c = additionalDefaults(c)
	if !fieldName.MatchString(c.VectorField) || !fieldName.MatchString(c.IDField) || !fieldName.MatchString(c.MetadataField) || c.VectorField == c.IDField || c.VectorField == c.MetadataField || c.MetadataField == c.IDField {
		return errors.New("field names must be distinct simple identifiers")
	}
	switch c.Kind {
	case "elasticsearch", "opensearch":
		if !namePattern.MatchString(c.Index) {
			return errors.New("one concrete index name is required")
		}
	case "redis":
		if c.KeyPrefix == "" || strings.ContainsAny(c.KeyPrefix, "*?[]\\") {
			return errors.New("a literal Redis key prefix is required")
		}
	default:
		if !namePattern.MatchString(c.Collection) || c.Collection == "." || c.Collection == ".." {
			return errors.New("a valid collection or table name is required")
		}
	}
	if (c.Kind == "pgvector" || c.Kind == "mongodb") && !namePattern.MatchString(c.Database) {
		return errors.New("database is required")
	}
	if c.Kind == "pgvector" && !fieldName.MatchString(c.Collection) {
		return errors.New("use a table name in the public schema")
	}
	if c.Kind == "milvus" && (c.IDType != "int64" || !namePattern.MatchString(c.Database)) {
		return errors.New("Milvus transfer requires an INT64 primary key and a database name")
	}
	if c.Kind == "mongodb" && !namePattern.MatchString(c.AuthDatabase) {
		return errors.New("invalid authentication database")
	}
	if c.Kind == "mongodb" && c.IDType != "string" && c.IDType != "objectid" && c.IDType != "int64" {
		return errors.New("MongoDB ID type must be objectid, string, or int64")
	}
	if c.Kind == "chroma" && (!namePattern.MatchString(c.Tenant) || !namePattern.MatchString(c.Database)) {
		return errors.New("valid Chroma tenant and database are required")
	}
	if (c.Kind == "pgvector" || c.Kind == "redis" || c.Kind == "mongodb") && s.APIKey != "" {
		return errors.New("this database uses username/password credentials")
	}
	return nil
}

func openAdditional(c PersonalConfig, s PersonalCredentials, public bool) (Binding, error) {
	if err := validateAdditional(c, s, public); err != nil {
		return Binding{}, err
	}
	c = additionalDefaults(c)
	b := Binding{Kind: c.Kind, CanRead: true, CanWrite: true, Fingerprint: Fingerprint(c)}
	resource := c.Collection
	if c.Index != "" {
		resource = c.Index
	}
	if c.Kind == "redis" {
		resource = c.KeyPrefix
	}
	b.Description = strings.Trim(c.Tenant+"/"+c.Database+"/"+resource, "/")
	b.Resource = Fingerprint([]string{c.Kind, c.Endpoint, b.Description})
	switch c.Kind {
	case "pgvector":
		p, err := newPGVector(c, s, public)
		if err != nil {
			return Binding{}, err
		}
		b.Source = p
		b.Sink = p
		return b, nil
	case "redis":
		p, err := newRedis(c, s, public)
		if err != nil {
			return Binding{}, err
		}
		b.Source = p
		b.Sink = p
		b.close = p.client.Close
		return b, nil
	case "mongodb":
		p, err := newMongo(c, s, public)
		if err != nil {
			return Binding{}, err
		}
		b.Source = p
		b.Sink = p
		b.close = p.Close
		return b, nil
	}
	h := http.Header{}
	if s.Password != "" {
		h.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(s.Username+":"+s.Password)))
	} else if s.APIKey != "" {
		switch c.Kind {
		case "qdrant":
			h.Set("api-key", s.APIKey)
		case "elasticsearch":
			h.Set("Authorization", "ApiKey "+s.APIKey)
		case "chroma":
			h.Set("x-chroma-token", s.APIKey)
		default:
			h.Set("Authorization", "Bearer "+s.APIKey)
		}
	}
	r := newRemote(c.Endpoint, h)
	if public {
		transport := &http.Transport{DialContext: publicDial, ForceAttemptHTTP2: true, MaxIdleConns: 8, MaxConnsPerHost: 8, IdleConnTimeout: 30 * time.Second, TLSHandshakeTimeout: 10 * time.Second, ResponseHeaderTimeout: 45 * time.Second}
		r.client.Transport = transport
		b.close = func() error { transport.CloseIdleConnections(); return nil }
	}
	p := &RESTDatabase{remote: r, config: c}
	b.Source = p
	b.Sink = p
	if c.Kind == "faiss" {
		p := &Polign{remote: r, collection: url.PathEscape(c.Collection)}
		b.Source = p
		b.Sink = p
	}
	return b, nil
}

func decodeID(raw json.RawMessage) (string, error) {
	var s string
	if json.Unmarshal(raw, &s) == nil && s != "" {
		return s, nil
	}
	var n json.Number
	if json.Unmarshal(raw, &n) == nil {
		if _, err := n.Int64(); err == nil {
			return n.String(), nil
		}
		if _, err := strconv.ParseUint(n.String(), 10, 64); err == nil {
			return n.String(), nil
		}
	}
	return "", errors.New("unsupported or missing record ID")
}
func documentRecord(doc map[string]json.RawMessage, c PersonalConfig) (Record, error) {
	id, err := decodeID(doc[c.IDField])
	if err != nil {
		return Record{}, err
	}
	var values []float32
	if json.Unmarshal(doc[c.VectorField], &values) != nil || len(values) == 0 {
		return Record{}, errors.New("missing or unsupported dense vector field")
	}
	metadata := map[string]json.RawMessage{}
	for k, v := range doc {
		if k != c.IDField && k != c.VectorField {
			metadata[k] = v
		}
	}
	return Record{ID: id, Values: values, Metadata: metadata}, nil
}
func recordDocument(r Record, c PersonalConfig, id any) (map[string]any, error) {
	doc := map[string]any{}
	for k, v := range r.Metadata {
		if k == c.IDField || k == c.VectorField {
			return nil, errors.New("metadata collides with configured ID or vector field")
		}
		doc[k] = v
	}
	doc[c.IDField] = id
	doc[c.VectorField] = r.Values
	return doc, nil
}
