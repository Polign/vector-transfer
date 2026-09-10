package connector

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
)

func TestPersonalConnectionsRejectServerCredentialsAndPrivateEndpoints(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "server-identity")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "server-secret")
	for _, endpoint := range []string{"http://public.example", "https://user:password@public.example", "https://public.example?api_key=secret", "https://127.0.0.1", "https://[::1]", "https://169.254.169.254", "https://10.1.2.3", "https://localhost", "https://100.100.100.200", "https://168.63.129.16"} {
		if _, err := OpenPersonal(PersonalConfig{Kind: "polign", Endpoint: endpoint, Collection: "docs"}, PersonalCredentials{APIKey: "own-key"}); err == nil {
			t.Fatalf("accepted unsafe endpoint %s", endpoint)
		}
	}
	if _, err := OpenPersonal(PersonalConfig{Kind: "s3vectors", Region: "us-east-1", Bucket: "bucket", Index: "index"}, PersonalCredentials{}); err == nil {
		t.Fatal("S3 fell back to the server's AWS identity")
	}
	if _, err := OpenPersonal(PersonalConfig{Kind: "s3vectors", Region: "us-east-1", Bucket: "bucket", Index: "index", Endpoint: "https://attacker.example"}, PersonalCredentials{AccessKeyID: "own", SecretAccessKey: "own-secret"}); err == nil {
		t.Fatal("S3 accepted an arbitrary signing destination")
	}
}

func TestPublicDialRejectsMixedAnswersAndPinsResolvedIP(t *testing.T) {
	for _, ip := range []string{"127.0.0.1", "169.254.169.254", "10.0.0.1", "172.16.0.1", "192.168.1.1", "::ffff:127.0.0.1", "::1", "fd00:ec2::254", "100.64.0.1", "0.0.0.0", "224.0.0.1", "198.18.0.1"} {
		called := false
		_, err := resolvedPublicDial(context.Background(), "tcp", "provider.example:443", func(context.Context, string, string) ([]netip.Addr, error) {
			return []netip.Addr{netip.MustParseAddr("8.8.8.8"), netip.MustParseAddr(ip)}, nil
		}, func(context.Context, string, string) (net.Conn, error) {
			called = true
			return nil, errors.New("should not dial")
		})
		if err == nil || called {
			t.Fatalf("dialed restricted answer %s", ip)
		}
	}
	lookups := 0
	var address string
	_, _ = resolvedPublicDial(context.Background(), "tcp", "provider.example:443", func(context.Context, string, string) ([]netip.Addr, error) {
		lookups++
		return []netip.Addr{netip.MustParseAddr("8.8.8.8")}, nil
	}, func(_ context.Context, _ string, target string) (net.Conn, error) {
		address = target
		return nil, errors.New("fixture")
	})
	if lookups != 1 || address != "8.8.8.8:443" {
		t.Fatal("target was resolved again after validation")
	}
}

func TestPersonalClientsKeepCredentialsSeparateAndRefuseRedirects(t *testing.T) {
	var received []string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received = append(received, r.Header.Get("Authorization"))
		w.Header().Set("Location", "https://attacker.example")
		w.WriteHeader(302)
	}))
	defer server.Close()
	for _, key := range []string{"source-only-secret", "sink-only-secret"} {
		binding, err := OpenPersonal(PersonalConfig{Kind: "polign", Endpoint: "https://provider.example", Collection: "docs"}, PersonalCredentials{APIKey: key})
		if err != nil {
			t.Fatal(err)
		}
		p := binding.Source.(*Polign)
		transport := p.remote.client.Transport.(*http.Transport)
		if transport.Proxy != nil || transport.DialContext == nil {
			t.Fatal("missing public-network transport policy")
		}
		// Only the fixture replaces transport; public clients never expose this override.
		p.remote.base = server.URL
		p.remote.client.Transport = server.Client().Transport
		_, err = p.Read(context.Background(), "", 1)
		if err == nil || Retryable(err) {
			t.Fatal("redirect was followed or retried")
		}
	}
	if len(received) != 2 || received[0] != "Bearer source-only-secret" || received[1] != "Bearer sink-only-secret" {
		t.Fatalf("credentials crossed clients: %d requests", len(received))
	}
	for _, id := range []string{"SOURCEKEY", "SINKKEY"} {
		b, err := OpenPersonal(PersonalConfig{Kind: "s3vectors", Region: "us-east-1", Bucket: "bucket", Index: "index"}, PersonalCredentials{AccessKeyID: id, SecretAccessKey: "personal-secret", SessionToken: "personal-session"})
		if err != nil {
			t.Fatal(err)
		}
		r, _ := http.NewRequest("POST", "https://s3vectors.us-east-1.api.aws/list-vectors", nil)
		if err := b.Source.(*S3Vectors).remote.sign(context.Background(), r, []byte("{}")); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(r.Header.Get("Authorization"), "Credential="+id+"/") || r.Header.Get("X-Amz-Security-Token") != "personal-session" {
			t.Fatal("S3 did not use its own explicit credentials")
		}
	}
}
