package connector

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/credentials"
)

// PersonalConfig is the public, user-configurable portion of a connection.
// It deliberately excludes server environment references, IAM role assumption,
// account grants, and arbitrary third-party factory options.
type PersonalConfig struct {
	Kind           string `json:"kind"`
	Endpoint       string `json:"endpoint,omitempty"`
	Collection     string `json:"collection,omitempty"`
	Namespace      string `json:"namespace,omitempty"`
	Region         string `json:"region,omitempty"`
	Bucket         string `json:"bucket,omitempty"`
	Index          string `json:"index,omitempty"`
	DistanceMetric string `json:"distance_metric,omitempty"`
	IDType         string `json:"id_type,omitempty"`
	Database       string `json:"database,omitempty"`
	AuthDatabase   string `json:"auth_database,omitempty"`
	Tenant         string `json:"tenant,omitempty"`
	VectorField    string `json:"vector_field,omitempty"`
	IDField        string `json:"id_field,omitempty"`
	MetadataField  string `json:"metadata_field,omitempty"`
	KeyPrefix      string `json:"key_prefix,omitempty"`
}

// PersonalCredentials must be encrypted in storage and never returned by an API.
// S3 uses only these explicit credentials, never the server's workload identity.
type PersonalCredentials struct {
	APIKey          string `json:"api_key,omitempty"`
	AccessKeyID     string `json:"access_key_id,omitempty"`
	SecretAccessKey string `json:"secret_access_key,omitempty"`
	SessionToken    string `json:"session_token,omitempty"`
	Username        string `json:"username,omitempty"`
	Password        string `json:"password,omitempty"`
}

func (c PersonalConfig) Validate(secret PersonalCredentials) error {
	if isAdditional(c.Kind) {
		return validateAdditional(c, secret, true)
	}
	if secret.Username != "" || secret.Password != "" || c.Database != "" || c.AuthDatabase != "" || c.Tenant != "" || c.VectorField != "" || c.IDField != "" || c.MetadataField != "" || c.KeyPrefix != "" {
		return errors.New("unsupported settings for this provider")
	}
	if !isBuiltin(c.Kind) {
		return errors.New("choose Polign, Pinecone, Turbopuffer, or S3 Vectors")
	}
	for _, value := range []string{c.Endpoint, c.Collection, c.Namespace, c.Region, c.Bucket, c.Index} {
		if len(value) > 2048 || strings.ContainsAny(value, "\r\n\x00") {
			return errors.New("invalid connection setting")
		}
	}
	for _, value := range []string{secret.APIKey, secret.AccessKeyID, secret.SecretAccessKey, secret.SessionToken} {
		if len(value) > 16384 || strings.ContainsAny(value, "\r\n\x00") {
			return errors.New("invalid credential format")
		}
	}
	if c.Kind == "s3vectors" {
		if c.Endpoint != "" || c.Collection != "" || c.Namespace != "" || c.DistanceMetric != "" || c.IDType != "" || secret.APIKey != "" {
			return errors.New("S3 Vectors accepts region, bucket, index, and AWS credentials")
		}
		if !regexp.MustCompile(`^[a-z]{2}-[a-z]+(?:-[a-z]+)?-[0-9]+$`).MatchString(c.Region) || !namePattern.MatchString(c.Bucket) || !namePattern.MatchString(c.Index) {
			return errors.New("S3 Vectors requires a valid region, vector bucket, and index")
		}
		if secret.AccessKeyID == "" || secret.SecretAccessKey == "" {
			return errors.New("AWS access key ID and secret access key are required")
		}
		return nil
	}
	if c.Region != "" || c.Bucket != "" || c.Index != "" || secret.AccessKeyID != "" || secret.SecretAccessKey != "" || secret.SessionToken != "" {
		return errors.New("this provider accepts an API key, not AWS credentials")
	}
	if secret.APIKey == "" {
		return errors.New("an API key is required")
	}
	u, err := url.Parse(c.Endpoint)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return errors.New("endpoint must be a public HTTPS origin without credentials, query, or path")
	}
	if strings.EqualFold(u.Hostname(), "localhost") || strings.HasSuffix(strings.ToLower(u.Hostname()), ".localhost") {
		return errors.New("private network endpoints are not supported for personal connections")
	}
	if ip, err := netip.ParseAddr(u.Hostname()); err == nil && !publicAddress(ip) {
		return errors.New("private network endpoints are not supported for personal connections")
	}
	switch c.Kind {
	case "polign":
		if c.Collection == "" || c.Namespace != "" || c.DistanceMetric != "" || c.IDType != "" {
			return errors.New("Polign requires a collection and no namespace settings")
		}
	case "pinecone":
		if c.Collection != "" || c.DistanceMetric != "" || c.IDType != "" {
			return errors.New("Pinecone accepts an index endpoint and optional namespace")
		}
	case "turbopuffer":
		if !namePattern.MatchString(c.Namespace) || c.Collection != "" {
			return errors.New("Turbopuffer requires a valid namespace")
		}
		if c.DistanceMetric != "" && c.DistanceMetric != "cosine_distance" && c.DistanceMetric != "euclidean_squared" {
			return errors.New("unsupported distance metric")
		}
		if c.IDType != "" && c.IDType != "string" && c.IDType != "uint" && c.IDType != "uuid" {
			return errors.New("unsupported ID type")
		}
	}
	return nil
}

// OpenPersonal builds a bounded public-network client using only user-supplied
// credentials. The caller adds verified ownership and the permitted direction.
func OpenPersonal(c PersonalConfig, secret PersonalCredentials) (Binding, error) {
	if isAdditional(c.Kind) {
		return openAdditional(c, secret, true)
	}
	if err := c.Validate(secret); err != nil {
		return Binding{}, err
	}
	c.Endpoint = strings.TrimRight(c.Endpoint, "/")
	if c.Kind == "turbopuffer" {
		if c.DistanceMetric == "" {
			c.DistanceMetric = "cosine_distance"
		}
		if c.IDType == "" {
			c.IDType = "string"
		}
	}
	var r *remote
	b := Binding{Kind: c.Kind, CanRead: true, CanWrite: true}
	switch c.Kind {
	case "polign":
		p := NewPolign(c.Endpoint, c.Collection, secret.APIKey)
		b.Source = p
		b.Sink = p
		r = p.remote
		b.Description = c.Collection
	case "pinecone":
		p := NewPinecone(c.Endpoint, c.Namespace, secret.APIKey)
		b.Source = p
		b.Sink = p
		r = p.remote
		b.Description = c.Namespace
	case "turbopuffer":
		p := NewTurbopuffer(c.Endpoint, c.Namespace, secret.APIKey, c.DistanceMetric, c.IDType)
		b.Source = p
		b.Sink = p
		r = p.remote
		b.Description = c.Namespace
	case "s3vectors":
		base := "https://s3vectors." + c.Region + ".api.aws"
		p := NewS3Vectors(base, c.Bucket, c.Index, c.Region, credentials.NewStaticCredentialsProvider(secret.AccessKeyID, secret.SecretAccessKey, secret.SessionToken))
		b.Source = p
		b.Sink = p
		r = p.remote
		b.Description = c.Bucket + "/" + c.Index
	}
	transport := &http.Transport{Proxy: nil, DialContext: publicDial, ForceAttemptHTTP2: true, MaxIdleConns: 8, MaxIdleConnsPerHost: 4, MaxConnsPerHost: 8, IdleConnTimeout: 30 * time.Second, TLSHandshakeTimeout: 10 * time.Second, ResponseHeaderTimeout: 45 * time.Second}
	r.client.Transport = transport
	b.close = func() error { transport.CloseIdleConnections(); return nil }
	b.Fingerprint = Fingerprint(c)
	endpoint := c.Endpoint
	if c.Kind == "s3vectors" {
		endpoint = "https://s3vectors." + c.Region + ".api.aws"
	}
	b.Resource = Fingerprint([]string{c.Kind, endpoint, b.Description})
	return b, nil
}

var excludedNetworks = []netip.Prefix{
	netip.MustParsePrefix("100.64.0.0/10"), netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"), netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"), netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("240.0.0.0/4"), netip.MustParsePrefix("168.63.129.16/32"),
	netip.MustParsePrefix("2001:db8::/32"), netip.MustParsePrefix("64:ff9b::/96"),
	netip.MustParsePrefix("64:ff9b:1::/48"), netip.MustParsePrefix("2002::/16"),
	netip.MustParsePrefix("::/96"), netip.MustParsePrefix("2001::/32"),
}

func publicAddress(ip netip.Addr) bool {
	ip = ip.Unmap()
	if !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
		return false
	}
	for _, prefix := range excludedNetworks {
		if prefix.Contains(ip) {
			return false
		}
	}
	return true
}

func publicDial(ctx context.Context, network, address string) (net.Conn, error) {
	return resolvedPublicDial(ctx, network, address, net.DefaultResolver.LookupNetIP, (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext)
}

// Resolve on every new connection, reject mixed private/public answers, then
// dial the checked IP directly. A second DNS lookup cannot rebind the target.
func resolvedPublicDial(ctx context.Context, network, address string, lookup func(context.Context, string, string) ([]netip.Addr, error), dial func(context.Context, string, string) (net.Conn, error)) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, errors.New("invalid provider address")
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ips, err := lookup(ctx, "ip", host)
	if err != nil || len(ips) == 0 {
		return nil, errors.New("provider endpoint could not be resolved")
	}
	for _, ip := range ips {
		if !publicAddress(ip) {
			return nil, errors.New("provider endpoint resolves to a restricted network")
		}
	}
	for _, ip := range ips {
		conn, err := dial(ctx, network, net.JoinHostPort(ip.String(), port))
		if err == nil {
			return conn, nil
		}
	}
	return nil, errors.New("provider endpoint could not be reached")
}
