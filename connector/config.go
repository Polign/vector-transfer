package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

type Config struct {
	Connections map[string]Connection `json:"connections"`
}
type Connection struct {
	ReadSubjects     []string        `json:"read_subjects,omitempty"`
	WriteSubjects    []string        `json:"write_subjects,omitempty"`
	WriteOnly        bool            `json:"write_only,omitempty"`
	AWSRoleARN       string          `json:"aws_role_arn,omitempty"`
	AWSExternalIDEnv string          `json:"aws_external_id_env,omitempty"`
	Kind             string          `json:"kind"`
	Endpoint         string          `json:"endpoint,omitempty"`
	Collection       string          `json:"collection,omitempty"`
	Namespace        string          `json:"namespace,omitempty"`
	Bucket           string          `json:"bucket,omitempty"`
	Index            string          `json:"index,omitempty"`
	Region           string          `json:"region,omitempty"`
	APIKeyEnv        string          `json:"api_key_env,omitempty"`
	DistanceMetric   string          `json:"distance_metric,omitempty"`
	IDType           string          `json:"id_type,omitempty"`
	ReadOnly         bool            `json:"read_only,omitempty"`
	Options          json.RawMessage `json:"options,omitempty"`
	Database         string          `json:"database,omitempty"`
	AuthDatabase     string          `json:"auth_database,omitempty"`
	Tenant           string          `json:"tenant,omitempty"`
	VectorField      string          `json:"vector_field,omitempty"`
	IDField          string          `json:"id_field,omitempty"`
	MetadataField    string          `json:"metadata_field,omitempty"`
	KeyPrefix        string          `json:"key_prefix,omitempty"`
	UsernameEnv      string          `json:"username_env,omitempty"`
	PasswordEnv      string          `json:"password_env,omitempty"`
}

var namePattern = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,128}$`)

// LoadConfig opens built-in and explicitly registered third-party connections.
// Existing flat built-in configurations remain supported. Custom connections
// use {"kind":"acme","options":{...}}. The caller owns Registry.Close.
func LoadConfig(ctx context.Context, path string, extensions ...Factories) (Registry, error) {
	factories := Factories{}
	for _, group := range extensions {
		for kind, factory := range group {
			if !namePattern.MatchString(kind) || factory == nil {
				return nil, errors.New("invalid connector factory registration")
			}
			if isBuiltin(kind) || factories[kind] != nil {
				return nil, fmt.Errorf("connector kind %q is already registered", kind)
			}
			factories[kind] = factory
		}
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	d := json.NewDecoder(io.LimitReader(f, 1<<20))
	d.DisallowUnknownFields()
	var cfg Config
	if err := d.Decode(&cfg); err != nil {
		return nil, err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return nil, errors.New("configuration must contain one JSON object")
	}
	if cfg.Connections == nil {
		return nil, errors.New("configuration must include a connections object")
	}
	r := Registry{}
	names := make([]string, 0, len(cfg.Connections))
	for name := range cfg.Connections {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		c := cfg.Connections[name]
		if !namePattern.MatchString(name) {
			return nil, errors.Join(errors.New("invalid connection name"), r.Close())
		}
		if c.ReadOnly && c.WriteOnly {
			return nil, errors.Join(errors.New("read_only and write_only cannot both be set"), r.Close())
		}
		var b Binding
		var err error
		if factory := factories[c.Kind]; factory != nil {
			b, err = openCustom(ctx, name, c, factory)
		} else {
			b, err = build(ctx, name, c)
		}
		if err != nil {
			return nil, errors.Join(fmt.Errorf("connection %s: %w", name, err), r.Close())
		}
		b.ReadSubjects, b.WriteSubjects = c.ReadSubjects, c.WriteSubjects
		if c.WriteOnly {
			b.Source = nil
			b.CanRead = false
		}
		r[name] = b
	}
	return r, nil
}

func build(ctx context.Context, name string, c Connection) (Binding, error) {
	if isAdditional(c.Kind) {
		if strings.HasPrefix(c.Endpoint, "http://") && len(c.ReadSubjects)+len(c.WriteSubjects) > 0 {
			return Binding{}, errors.New("account connections require TLS")
		}
		if c.Options != nil || c.AWSRoleARN != "" || c.AWSExternalIDEnv != "" {
			return Binding{}, errors.New("unsupported settings for this connector")
		}
		secret := PersonalCredentials{}
		for _, ref := range []struct {
			env    string
			target *string
		}{{c.APIKeyEnv, &secret.APIKey}, {c.UsernameEnv, &secret.Username}, {c.PasswordEnv, &secret.Password}} {
			if ref.env != "" {
				*ref.target = os.Getenv(ref.env)
				if *ref.target == "" {
					return Binding{}, errors.New("configured credential environment variable is empty")
				}
			}
		}
		pc := PersonalConfig{Kind: c.Kind, Endpoint: c.Endpoint, Collection: c.Collection, Namespace: c.Namespace, Index: c.Index, Database: c.Database, AuthDatabase: c.AuthDatabase, Tenant: c.Tenant, VectorField: c.VectorField, IDField: c.IDField, MetadataField: c.MetadataField, IDType: c.IDType, KeyPrefix: c.KeyPrefix}
		b, err := openAdditional(pc, secret, false)
		b.Name = name
		if c.ReadOnly {
			b.Sink = nil
			b.CanWrite = false
		}
		return b, err
	}
	if !isBuiltin(c.Kind) {
		return Binding{}, fmt.Errorf("unknown connector kind %q; register its factory in this executable", c.Kind)
	}
	if c.Options != nil {
		return Binding{}, errors.New("built-in connections use flat configuration fields, not options")
	}
	if c.Kind == "s3vectors" && c.Endpoint == "" {
		if !regexp.MustCompile(`^[a-z0-9-]+$`).MatchString(c.Region) {
			return Binding{}, errors.New("S3 Vectors requires an AWS region")
		}
		c.Endpoint = "https://s3vectors." + c.Region + ".api.aws"
	}
	c.Endpoint = strings.TrimRight(c.Endpoint, "/")
	u, err := url.Parse(c.Endpoint)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return Binding{}, errors.New("endpoint must be an HTTP(S) origin without credentials, query, or path")
	}
	if u.Scheme != "https" && (c.APIKeyEnv != "" || c.Kind == "s3vectors" || len(c.ReadSubjects)+len(c.WriteSubjects) > 0) {
		return Binding{}, errors.New("authenticated connections require an HTTPS endpoint")
	}
	if c.Kind != "s3vectors" && (c.AWSRoleARN != "" || c.AWSExternalIDEnv != "") {
		return Binding{}, errors.New("AWS role settings require s3vectors")
	}
	if c.AWSExternalIDEnv != "" && c.AWSRoleARN == "" {
		return Binding{}, errors.New("aws_external_id_env requires aws_role_arn")
	}
	if c.Kind == "turbopuffer" {
		if c.DistanceMetric == "" {
			c.DistanceMetric = "cosine_distance"
		}
		if c.IDType == "" {
			c.IDType = "string"
		}
		if c.DistanceMetric != "cosine_distance" && c.DistanceMetric != "euclidean_squared" {
			return Binding{}, errors.New("unsupported Turbopuffer distance metric")
		}
		if c.IDType != "string" && c.IDType != "uint" && c.IDType != "uuid" {
			return Binding{}, errors.New("Turbopuffer id_type must be string, uint, or uuid")
		}
	}
	key := ""
	if c.APIKeyEnv != "" {
		key = os.Getenv(c.APIKeyEnv)
		if key == "" {
			return Binding{}, errors.New("configured API key environment variable is empty")
		}
	}
	fingerprintConfig := c
	fingerprintConfig.ReadSubjects, fingerprintConfig.WriteSubjects = nil, nil
	b := Binding{Name: name, Kind: c.Kind, Fingerprint: Fingerprint(fingerprintConfig), CanRead: true, CanWrite: !c.ReadOnly}
	switch c.Kind {
	case "polign":
		if c.Collection == "" {
			return b, errors.New("collection is required")
		}
		p := NewPolign(c.Endpoint, c.Collection, key)
		b.Source = p
		b.Sink = p
		b.Description = c.Collection
	case "pinecone":
		if key == "" {
			return b, errors.New("api_key_env is required")
		}
		p := NewPinecone(c.Endpoint, c.Namespace, key)
		b.Source = p
		b.Sink = p
		b.Description = c.Namespace
	case "turbopuffer":
		if key == "" || !namePattern.MatchString(c.Namespace) {
			return b, errors.New("api_key_env and valid namespace are required")
		}
		p := NewTurbopuffer(c.Endpoint, c.Namespace, key, c.DistanceMetric, c.IDType)
		b.Source = p
		b.Sink = p
		b.Description = c.Namespace
	case "s3vectors":
		if c.Region == "" || c.Bucket == "" || c.Index == "" {
			return b, errors.New("region, bucket, and index are required")
		}
		cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(c.Region))
		if err != nil {
			return b, errors.New("AWS configuration could not be loaded")
		}
		if c.AWSRoleARN != "" {
			externalID := ""
			if c.AWSExternalIDEnv != "" {
				externalID = os.Getenv(c.AWSExternalIDEnv)
				if externalID == "" {
					return b, errors.New("AWS external ID environment variable is empty")
				}
			}
			cfg.Credentials = aws.NewCredentialsCache(stscreds.NewAssumeRoleProvider(sts.NewFromConfig(cfg), c.AWSRoleARN, func(o *stscreds.AssumeRoleOptions) {
				o.RoleSessionName = "vector-transfer"
				if externalID != "" {
					o.ExternalID = aws.String(externalID)
				}
			}))
		}
		p := NewS3Vectors(c.Endpoint, c.Bucket, c.Index, c.Region, cfg.Credentials)
		b.Source = p
		b.Sink = p
		b.Description = c.Bucket + "/" + c.Index
	default:
		return b, fmt.Errorf("unknown connector kind %q", c.Kind)
	}
	if c.ReadOnly {
		b.Sink = nil
	}
	// Aliases and credential settings do not distinguish physical resources.
	b.Resource = Fingerprint([]string{c.Kind, c.Endpoint, b.Description})
	return b, nil
}

func isBuiltin(kind string) bool {
	if isAdditional(kind) {
		return true
	}
	switch kind {
	case "polign", "pinecone", "turbopuffer", "s3vectors":
		return true
	}
	return false
}
