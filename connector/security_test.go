package connector

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAuthenticatedConnectionsRequireTLS(t *testing.T) {
	t.Setenv("TEST_PROVIDER_KEY", "fake-secret")
	for _, c := range []Connection{
		{Kind: "polign", Endpoint: "http://localhost:1234", Collection: "docs", APIKeyEnv: "TEST_PROVIDER_KEY"},
		{Kind: "polign", Endpoint: "http://localhost:1234", Collection: "docs", ReadSubjects: []string{"alice"}},
		{Kind: "s3vectors", Endpoint: "http://localhost:1234", Region: "us-west-2", Bucket: "bucket", Index: "index"},
	} {
		if _, err := build(context.Background(), "test", c); err == nil {
			t.Fatal("plaintext authenticated endpoint accepted")
		}
	}
}

func TestS3ConnectionsUseSeparateAssumedCredentials(t *testing.T) {
	sts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		role := r.Form.Get("RoleArn")
		name := "source"
		if strings.HasSuffix(role, ":role/sink") {
			name = "sink"
		}
		if role != "arn:aws:iam::123456789012:role/"+name || r.Form.Get("ExternalId") != "external-"+name || r.Form.Get("Action") != "AssumeRole" {
			t.Errorf("unexpected role request: %v", r.Form)
			w.WriteHeader(400)
			return
		}
		w.Header().Set("Content-Type", "text/xml")
		fmt.Fprintf(w, `<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><AssumeRoleResult><Credentials><AccessKeyId>KEY-%s</AccessKeyId><SecretAccessKey>fake-secret</SecretAccessKey><SessionToken>token-%s</SessionToken><Expiration>%s</Expiration></Credentials><AssumedRoleUser><AssumedRoleId>id:%s</AssumedRoleId><Arn>%s</Arn></AssumedRoleUser></AssumeRoleResult></AssumeRoleResponse>`, name, name, time.Now().Add(time.Hour).UTC().Format(time.RFC3339), name, role)
	}))
	defer sts.Close()
	t.Setenv("AWS_ACCESS_KEY_ID", "fake-base-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "fake-base-secret")
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_ENDPOINT_URL_STS", sts.URL)
	t.Setenv("SOURCE_EXTERNAL", "external-source")
	t.Setenv("SINK_EXTERNAL", "external-sink")
	for _, name := range []string{"source", "sink"} {
		b, err := build(context.Background(), name, Connection{Kind: "s3vectors", Region: "us-west-2", Bucket: "bucket", Index: name, AWSRoleARN: "arn:aws:iam::123456789012:role/" + name, AWSExternalIDEnv: strings.ToUpper(name) + "_EXTERNAL"})
		if err != nil {
			t.Fatal(err)
		}
		client := b.Source.(*S3Vectors)
		req, _ := http.NewRequest("POST", "https://s3vectors.us-west-2.api.aws/put-vectors", strings.NewReader("{}"))
		if err := client.remote.sign(context.Background(), req, []byte("{}")); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(req.Header.Get("Authorization"), "Credential=KEY-"+name+"/") || req.Header.Get("X-Amz-Security-Token") != "token-"+name {
			t.Fatal("connection used another role's credentials")
		}
	}
}
