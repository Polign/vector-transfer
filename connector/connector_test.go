package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
)

type both interface {
	Source
	Sink
}

func sample(id string) Record {
	return Record{ID: id, Values: []float32{0.25, 0.75}, Metadata: map[string]json.RawMessage{"number": json.RawMessage(`18446744073709551615`), "active": json.RawMessage(`true`), "tags": json.RawMessage(`["a","b"]`)}}
}

// Fixtures enforce each provider's wire shape, authentication, paging, and
// acknowledgement response. They are not live cloud integration tests.
func providerFixture(t *testing.T, kind string, initial []Record) (both, func() []Record) {
	t.Helper()
	stored := map[string]Record{}
	for _, r := range initial {
		stored[r.ID] = r
	}
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		fail := func(message string) { t.Error(kind + ": " + message); http.Error(w, message, 400) }
		if kind == "s3vectors" {
			if !strings.Contains(r.Header.Get("Authorization"), "/us-east-1/s3vectors/aws4_request") || r.Header.Get("X-Amz-Security-Token") != "session" {
				fail("missing AWS signing or session token")
				return
			}
		} else if kind == "pinecone" {
			if r.Header.Get("Api-Key") != "key" || r.Header.Get("X-Pinecone-Api-Version") != "2025-10" {
				fail("missing Pinecone auth/version")
				return
			}
		} else if r.Header.Get("Authorization") != "Bearer key" {
			fail("missing bearer auth")
			return
		}
		var body map[string]json.RawMessage
		if r.Method == "POST" {
			if json.NewDecoder(r.Body).Decode(&body) != nil {
				fail("invalid JSON")
				return
			}
		}
		all := make([]Record, 0, len(stored))
		for _, v := range stored {
			all = append(all, v)
		}
		sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })
		read := r.Method == "GET" || strings.HasSuffix(r.URL.Path, "/query") || r.URL.Path == "/ListVectors"
		if read {
			if kind == "pinecone" && r.URL.Path == "/vectors/fetch" {
				result := map[string]Record{}
				for _, id := range r.URL.Query()["ids"] {
					if v, ok := stored[id]; ok {
						result[id] = v
					}
				}
				json.NewEncoder(w).Encode(map[string]any{"vectors": result})
				return
			}
			offset, limit := 0, 2
			switch kind {
			case "polign":
				if r.URL.Query().Get("typed") != "true" {
					fail("metadata types would be lost")
					return
				}
				offset, _ = strconv.Atoi(r.URL.Query().Get("offset"))
				limit, _ = strconv.Atoi(r.URL.Query().Get("limit"))
			case "pinecone":
				offset, _ = strconv.Atoi(r.URL.Query().Get("paginationToken"))
				limit, _ = strconv.Atoi(r.URL.Query().Get("limit"))
				if r.URL.Query().Get("namespace") != "docs" {
					fail("namespace missing")
					return
				}
			case "s3vectors":
				if string(body["returnData"]) != "true" || string(body["returnMetadata"]) != "true" || string(body["vectorBucketName"]) != `"bucket"` || string(body["indexName"]) != `"index"` {
					fail("missing full vector export fields")
					return
				}
				var cursor string
				json.Unmarshal(body["nextToken"], &cursor)
				offset, _ = strconv.Atoi(cursor)
				json.Unmarshal(body["maxResults"], &limit)
			case "turbopuffer":
				if string(body["include_attributes"]) != "true" || string(body["vector_encoding"]) != `"float"` || string(body["rank_by"]) != `["id","asc"]` {
					fail("missing full export options")
					return
				}
				json.Unmarshal(body["limit"], &limit)
				if body["filters"] != nil {
					var filter []json.RawMessage
					json.Unmarshal(body["filters"], &filter)
					if len(filter) != 3 || string(filter[1]) != `"Gt"` {
						fail("bad continuation filter")
						return
					}
					var id string
					json.Unmarshal(filter[2], &id)
					for offset < len(all) && all[offset].ID <= id {
						offset++
					}
				}
			}
			end := min(offset+limit, len(all))
			records := all[offset:end]
			next := ""
			if end < len(all) {
				next = strconv.Itoa(end)
			}
			switch kind {
			case "polign":
				json.NewEncoder(w).Encode(map[string]any{"vectors": records, "total": len(all)})
			case "pinecone":
				ids := make([]map[string]string, 0)
				for _, v := range records {
					ids = append(ids, map[string]string{"id": v.ID})
				}
				json.NewEncoder(w).Encode(map[string]any{"vectors": ids, "pagination": map[string]string{"next": next}})
			case "s3vectors":
				vectors := make([]s3Vector, 0)
				for _, v := range records {
					x := s3Vector{Key: v.ID, Metadata: v.Metadata}
					x.Data.Float32 = v.Values
					vectors = append(vectors, x)
				}
				json.NewEncoder(w).Encode(map[string]any{"vectors": vectors, "nextToken": next})
			case "turbopuffer":
				rows := make([]map[string]any, 0)
				for _, v := range records {
					row := map[string]any{"id": v.ID, "vector": v.Values}
					for k, m := range v.Metadata {
						row[k] = m
					}
					rows = append(rows, row)
				}
				json.NewEncoder(w).Encode(map[string]any{"rows": rows})
			}
			return
		}
		var records []Record
		switch kind {
		case "polign", "pinecone":
			if err := json.Unmarshal(body["vectors"], &records); err != nil {
				fail("invalid vector upsert body")
				return
			}
			if kind == "pinecone" && string(body["namespace"]) != `"docs"` {
				fail("upsert namespace missing")
				return
			}
		case "s3vectors":
			if r.URL.Path != "/PutVectors" {
				fail("bad write path")
				return
			}
			var vectors []s3Vector
			if json.Unmarshal(body["vectors"], &vectors) != nil {
				fail("invalid AWS vectors")
				return
			}
			for _, v := range vectors {
				records = append(records, Record{ID: v.Key, Values: v.Data.Float32, Metadata: v.Metadata})
			}
		case "turbopuffer":
			if string(body["distance_metric"]) != `"cosine_distance"` {
				fail("distance metric missing")
				return
			}
			var rows []map[string]json.RawMessage
			if json.Unmarshal(body["upsert_rows"], &rows) != nil {
				fail("invalid upsert rows")
				return
			}
			for _, row := range rows {
				v := Record{}
				json.Unmarshal(row["id"], &v.ID)
				json.Unmarshal(row["vector"], &v.Values)
				delete(row, "id")
				delete(row, "vector")
				v.Metadata = row
				records = append(records, v)
			}
		}
		for _, v := range records {
			stored[v.ID] = v
		}
		switch kind {
		case "polign":
			ids := make([]string, 0)
			for _, v := range records {
				ids = append(ids, v.ID)
			}
			json.NewEncoder(w).Encode(map[string]any{"ids": ids})
		case "pinecone":
			json.NewEncoder(w).Encode(map[string]int{"upsertedCount": len(records)})
		case "turbopuffer":
			json.NewEncoder(w).Encode(map[string]int{"rows_upserted": len(records)})
		case "s3vectors":
			w.WriteHeader(200)
		}
	}))
	t.Cleanup(server.Close)
	var adapter both
	switch kind {
	case "polign":
		adapter = NewPolign(server.URL, "docs", "key")
	case "pinecone":
		adapter = NewPinecone(server.URL, "docs", "key")
	case "turbopuffer":
		adapter = NewTurbopuffer(server.URL, "docs", "key", "cosine_distance", "string")
	case "s3vectors":
		adapter = NewS3Vectors(server.URL, "bucket", "index", "us-east-1", aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test-secret", SessionToken: "session"}, nil
		}))
	}
	return adapter, func() []Record {
		mu.Lock()
		defer mu.Unlock()
		out := make([]Record, 0)
		for _, v := range stored {
			out = append(out, v)
		}
		sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
		return out
	}
}

func TestAllProviderTransferDirections(t *testing.T) {
	kinds := []string{"s3vectors", "polign", "turbopuffer", "pinecone"}
	for _, source := range kinds {
		for _, sink := range kinds {
			t.Run(source+"_to_"+sink, func(t *testing.T) {
				want := []Record{sample("a"), sample("b"), sample("c")}
				src, _ := providerFixture(t, source, want)
				dst, stored := providerFixture(t, sink, nil)
				cursor := ""
				for pages := 0; ; pages++ {
					if pages > 4 {
						t.Fatal("pagination did not terminate")
					}
					p, err := src.Read(context.Background(), cursor, 2)
					if err != nil {
						t.Fatal(err)
					}
					if err := Validate(p.Records, 2); err != nil {
						t.Fatal(err)
					}
					if err := dst.Upsert(context.Background(), p.Records); err != nil {
						t.Fatal(err)
					}
					if p.Done {
						break
					}
					cursor = p.Next
				}
				if got := stored(); !reflect.DeepEqual(got, want) {
					t.Fatalf("transfer changed data: got %+v want %+v", got, want)
				}
				if err := dst.Upsert(context.Background(), want); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(stored(), want) {
					t.Fatal("replay changed records")
				}
			})
		}
	}
}

func TestTurbopufferUintCursorPreservesPrecision(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]json.RawMessage
		json.NewDecoder(r.Body).Decode(&body)
		if string(body["filters"]) != `["id","Gt",18446744073709551614]` {
			t.Errorf("numeric cursor corrupted: %s", body["filters"])
		}
		fmt.Fprint(w, `{"rows":[{"id":18446744073709551615,"vector":[0.25,0.75]}]}`)
	}))
	defer server.Close()
	c := NewTurbopuffer(server.URL, "docs", "key", "cosine_distance", "uint")
	p, err := c.Read(context.Background(), "18446744073709551614", 1)
	if err != nil {
		t.Fatal(err)
	}
	if p.Records[0].ID != "18446744073709551615" || p.Next != "18446744073709551615" {
		t.Fatalf("ID lost precision: %+v", p)
	}
}

func TestPineconeFailsOnMissingOrSparseFetch(t *testing.T) {
	for _, response := range []string{`{"vectors":{}}`, `{"vectors":{"a":{"id":"a","values":[1,2],"sparseValues":{"indices":[1],"values":[1]}}}}`} {
		t.Run(response, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/vectors/list" {
					fmt.Fprint(w, `{"vectors":[{"id":"a"}]}`)
				} else {
					fmt.Fprint(w, response)
				}
			}))
			defer server.Close()
			if _, err := NewPinecone(server.URL, "docs", "key").Read(context.Background(), "", 2); err == nil {
				t.Fatal("incomplete vector export succeeded")
			}
		})
	}
}

func TestSinksRejectPartialAcknowledgement(t *testing.T) {
	for _, kind := range []string{"polign", "pinecone", "turbopuffer"} {
		t.Run(kind, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprint(w, `{"ids":["a"],"upsertedCount":1,"rows_upserted":1}`)
			}))
			defer server.Close()
			var sink Sink
			switch kind {
			case "polign":
				sink = NewPolign(server.URL, "docs", "key")
			case "pinecone":
				sink = NewPinecone(server.URL, "docs", "key")
			case "turbopuffer":
				sink = NewTurbopuffer(server.URL, "docs", "key", "cosine_distance", "string")
			}
			if err := sink.Upsert(context.Background(), []Record{sample("a"), sample("b")}); err == nil {
				t.Fatal("partial write acknowledged as complete")
			}
		})
	}
}

func TestHTTPRetriesAndCredentialSafeErrors(t *testing.T) {
	for _, code := range []int{400, 401, 429, 503} {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(code)
				fmt.Fprint(w, "secret-key vector payload")
			}))
			defer server.Close()
			err := newRemote(server.URL, http.Header{}).call(context.Background(), "GET", "/", nil, nil)
			if err == nil || strings.Contains(err.Error(), "secret") || Retryable(err) != (code == 429 || code == 503) {
				t.Fatalf("bad error classification: %v", err)
			}
		})
	}
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("credentials followed a redirect") }))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, 307) }))
	defer redirect.Close()
	if err := newRemote(redirect.URL, http.Header{"Api-Key": {"secret"}}).call(context.Background(), "GET", "/", nil, nil); err == nil {
		t.Fatal("redirect accepted")
	}
}

func TestPineconeSplitsByBytesAndTurbopufferRejectsCollisions(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.ContentLength > 1900000 {
			t.Error("oversized Pinecone request")
		}
		var body struct {
			Vectors []Record `json:"vectors"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		json.NewEncoder(w).Encode(map[string]int{"upsertedCount": len(body.Vectors)})
	}))
	defer server.Close()
	var records []Record
	for _, id := range []string{"a", "b", "c"} {
		r := sample(id)
		b, _ := json.Marshal(strings.Repeat("x", 1000000))
		r.Metadata["large"] = b
		records = append(records, r)
	}
	if err := NewPinecone(server.URL, "docs", "key").Upsert(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	if requests != 3 {
		t.Fatalf("wanted 3 requests, got %d", requests)
	}
	c := NewTurbopuffer(server.URL, "docs", "key", "cosine_distance", "string")
	r := sample("a")
	r.Metadata["id"] = json.RawMessage(`"would overwrite"`)
	if err := c.Upsert(context.Background(), []Record{r}); err == nil {
		t.Fatal("reserved metadata silently overwritten")
	}
	c.idType = "uint"
	r = sample("01")
	if err := c.Upsert(context.Background(), []Record{r}); err == nil {
		t.Fatal("ambiguous ID conversion accepted")
	}
}

func TestConnectionAliasesShareResourceIdentity(t *testing.T) {
	a, err := build(context.Background(), "source", Connection{Kind: "polign", Endpoint: "http://127.0.0.1:23000", Collection: "docs", ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	b, err := build(context.Background(), "sink", Connection{Kind: "polign", Endpoint: "http://127.0.0.1:23000", Collection: "docs"})
	if err != nil {
		t.Fatal(err)
	}
	if a.Resource != b.Resource || a.Fingerprint == b.Fingerprint || a.Sink != nil {
		t.Fatal("resource identity or read-only binding is incorrect")
	}
	if _, err := build(context.Background(), "bad", Connection{Kind: "polign", Endpoint: "http://user:secret@localhost", Collection: "docs"}); err == nil {
		t.Fatal("endpoint credentials accepted")
	}
	if Retryable(errors.New("permanent")) {
		t.Fatal("permanent error classified as retryable")
	}
}

func TestMalformedListingCannotCompleteAnEmptyTransfer(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `{}`) }))
	defer server.Close()
	credentials := aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
	})
	for name, source := range map[string]Source{
		"polign":      NewPolign(server.URL, "docs", "key"),
		"pinecone":    NewPinecone(server.URL, "docs", "key"),
		"turbopuffer": NewTurbopuffer(server.URL, "docs", "key", "cosine_distance", "string"),
		"s3vectors":   NewS3Vectors(server.URL, "bucket", "index", "us-east-1", credentials),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := source.Read(context.Background(), "", 100); err == nil {
				t.Fatal("malformed response treated as end of scan")
			}
		})
	}
}

func TestS3VectorsDefaultEndpoint(t *testing.T) {
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	b, err := build(context.Background(), "s3", Connection{Kind: "s3vectors", Region: "us-east-1", Bucket: "bucket", Index: "index"})
	if err != nil {
		t.Fatal(err)
	}
	if got := b.Source.(*S3Vectors).remote.base; got != "https://s3vectors.us-east-1.api.aws" {
		t.Fatalf("incorrect S3 Vectors endpoint: %s", got)
	}
}
