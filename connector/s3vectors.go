package connector

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

type S3Vectors struct {
	remote        *remote
	bucket, index string
}

// AWS credentials come from the SDK's refreshable default credential chain.
// The REST wire format preserves metadata JSON without SDK document coercion.
func NewS3Vectors(base, bucket, index, region string, credentials aws.CredentialsProvider) *S3Vectors {
	r := newRemote(base, http.Header{})
	signer := v4.NewSigner()
	r.sign = func(ctx context.Context, req *http.Request, payload []byte) error {
		creds, err := credentials.Retrieve(ctx)
		if err != nil {
			return errors.New("AWS credentials could not be resolved")
		}
		hash := sha256.Sum256(payload)
		if err := signer.SignHTTP(ctx, creds, req, hex.EncodeToString(hash[:]), "s3vectors", region, time.Now()); err != nil {
			return errors.New("AWS request signing failed")
		}
		return nil
	}
	return &S3Vectors{r, bucket, index}
}

type s3Vector struct {
	Key  string `json:"key"`
	Data struct {
		Float32 []float32 `json:"float32"`
	} `json:"data"`
	Metadata map[string]json.RawMessage `json:"metadata,omitempty"`
}

func (c *S3Vectors) Read(ctx context.Context, cursor string, limit int) (Page, error) {
	body := map[string]any{"vectorBucketName": c.bucket, "indexName": c.index, "maxResults": limit, "returnData": true, "returnMetadata": true}
	if cursor != "" {
		body["nextToken"] = cursor
	}
	var result struct {
		Vectors []s3Vector `json:"vectors"`
		Next    string     `json:"nextToken"`
	}
	if err := c.remote.call(ctx, "POST", "/ListVectors", body, &result); err != nil {
		return Page{}, err
	}
	if result.Vectors == nil {
		return Page{}, errors.New("S3 Vectors listing response is missing vectors")
	}
	page := Page{Next: result.Next, Done: result.Next == ""}
	for _, v := range result.Vectors {
		page.Records = append(page.Records, Record{ID: v.Key, Values: v.Data.Float32, Metadata: v.Metadata})
	}
	return page, nil
}

func (c *S3Vectors) Upsert(ctx context.Context, records []Record) error {
	return chunks(records, 500, 8<<20, func(rs []Record) any {
		vs := make([]s3Vector, 0, len(rs))
		for _, r := range rs {
			v := s3Vector{Key: r.ID, Metadata: r.Metadata}
			v.Data.Float32 = r.Values
			vs = append(vs, v)
		}
		return map[string]any{"vectorBucketName": c.bucket, "indexName": c.index, "vectors": vs}
	}, func(body any) error { return c.remote.call(ctx, "POST", "/PutVectors", body, nil) })
}
