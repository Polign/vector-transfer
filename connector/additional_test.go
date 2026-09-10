package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
)

type providerProtocol struct{ kind, readPath, readResponse, writePath, writeResponse, id string }

func protocols() []providerProtocol {
	return []providerProtocol{
		{"qdrant", "/collections/docs/points/scroll", `{"status":"ok","result":{"points":[{"id":42,"vector":[1,2],"payload":{"tag":"kept","count":9007199254740993}}],"next_page_offset":null}}`, "/collections/docs/points", `{"status":"ok","result":{"status":"completed"}}`, "42"},
		{"weaviate", "/v1/objects", `{"objects":[{"id":"00000000-0000-0000-0000-000000000042","vector":[1,2],"properties":{"tag":"kept","count":9007199254740993}}]}`, "/v1/batch/objects", `[{"id":"00000000-0000-0000-0000-000000000042","result":{"status":"SUCCESS"}}]`, "00000000-0000-0000-0000-000000000042"},
		{"milvus", "/v2/vectordb/entities/query", `{"code":0,"data":[{"id":42,"vector":[1,2],"tag":"kept","count":9007199254740993}]}`, "/v2/vectordb/entities/upsert", `{"code":0,"data":{"upsertCount":1,"upsertIds":[42]}}`, "42"},
		{"elasticsearch", "/docs/_search", `{"timed_out":false,"_shards":{"failed":0},"hits":{"hits":[{"_id":"42","_source":{"id":"42","vector":[1,2],"tag":"kept","count":9007199254740993},"sort":["42"]}]}}`, "/docs/_bulk", `{"errors":false,"items":[{"index":{"_id":"42","status":201}}]}`, "42"},
		{"opensearch", "/docs/_search", `{"timed_out":false,"_shards":{"failed":0},"hits":{"hits":[{"_id":"42","_source":{"id":"42","vector":[1,2],"tag":"kept","count":9007199254740993},"sort":["42"]}]}}`, "/docs/_bulk", `{"errors":false,"items":[{"index":{"_id":"42","status":201}}]}`, "42"},
		{"chroma", "/api/v2/tenants/default_tenant/databases/default_database/collections/docs/get", `{"ids":["42"],"embeddings":[[1,2]],"metadatas":[{"tag":"kept","count":9007199254740993}],"documents":[null],"uris":[null]}`, "/api/v2/tenants/default_tenant/databases/default_database/collections/docs/upsert", `{}`, "42"},
		{"solr", "/solr/docs/select", `{"responseHeader":{"status":0},"response":{"docs":[{"id":"42","vector":[1,2],"tag":"kept","count":9007199254740993,"_version_":123}]},"nextCursorMark":"end"}`, "/solr/docs/update", `{"responseHeader":{"status":0}}`, "42"},
	}
}
func openProtocol(t *testing.T, kind, base string) Binding {
	t.Helper()
	c := PersonalConfig{Kind: kind, Endpoint: base, Collection: "docs"}
	if kind == "elasticsearch" || kind == "opensearch" {
		c.Index = "docs"
	}
	b, err := openAdditional(c, PersonalCredentials{}, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { Registry{"fixture": b}.Close() })
	return b
}
func TestAdditionalProviderProtocolsAndPolignIngress(t *testing.T) {
	for _, p := range protocols() {
		t.Run(p.kind, func(t *testing.T) {
			writes := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == p.readPath {
					if p.kind == "weaviate" && r.URL.Query().Get("include") != "vector" {
						t.Error("missing vectors")
					}
					io.WriteString(w, p.readResponse)
					return
				}
				if r.URL.Path != p.writePath {
					t.Errorf("unexpected provider route %s", r.URL.Path)
					w.WriteHeader(404)
					return
				}
				writes++
				body, _ := io.ReadAll(r.Body)
				if !strings.Contains(string(body), p.id) || !strings.Contains(string(body), "9007199254740993") || !strings.Contains(string(body), "kept") {
					t.Error("write lost ID or exact metadata")
				}
				if strings.Contains(p.kind, "search") {
					if r.Header.Get("Content-Type") != "application/x-ndjson" || !strings.HasSuffix(string(body), "\n") {
						t.Error("invalid bulk wire format")
					}
				}
				if p.kind == "qdrant" && r.URL.Query().Get("wait") != "true" {
					t.Error("write acknowledged before completion")
				}
				io.WriteString(w, p.writeResponse)
			}))
			defer server.Close()
			b := openProtocol(t, p.kind, server.URL)
			page, err := b.Source.Read(context.Background(), "", 2)
			if err != nil {
				t.Fatal(err)
			}
			if len(page.Records) != 1 || page.Records[0].ID != p.id {
				t.Fatal("source lost record")
			}
			if err := Validate(page.Records, 2); err != nil {
				t.Fatal(err)
			}
			reopened := openProtocol(t, p.kind, server.URL)
			replay, err := reopened.Source.Read(context.Background(), "", 2)
			if err != nil || !reflect.DeepEqual(page, replay) {
				t.Fatal("reopening changed source page")
			}
			for i := 0; i < 2; i++ {
				if err := b.Sink.Upsert(context.Background(), page.Records); err != nil {
					t.Fatal(err)
				}
			}
			if writes != 2 {
				t.Fatal("replay not sent")
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, err := b.Source.Read(ctx, "", 2); err != context.Canceled {
				t.Fatalf("cancellation: %v", err)
			}
			if base := os.Getenv("VECTOR_TRANSFER_TEST_POLIGN_URL"); base != "" {
				assertPolignIngress(t, base, p.kind, page.Records)
			}
		})
	}
}
func assertPolignIngress(t *testing.T, base, kind string, records []Record) {
	t.Helper()
	sink := NewPolign(base, "transfer-integration-"+kind, "")
	for i := 0; i < 2; i++ {
		if err := sink.Upsert(context.Background(), records); err != nil {
			t.Fatal("real Polign ingress:", err)
		}
	}
	page, err := sink.Read(context.Background(), "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if !page.Done || !reflect.DeepEqual(page.Records, records) {
		got, _ := json.Marshal(page.Records)
		want, _ := json.Marshal(records)
		t.Fatalf("Polign readback mismatch\ngot %s\nwant %s", got, want)
	}
}
func TestAdditionalSinksRejectPartialAcknowledgements(t *testing.T) {
	for _, p := range protocols() {
		if p.kind == "chroma" {
			continue
		}
		t.Run(p.kind, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, `{}`) }))
			defer server.Close()
			b := openProtocol(t, p.kind, server.URL)
			if err := b.Sink.Upsert(context.Background(), []Record{{ID: p.id, Values: []float32{1, 2}}}); err == nil {
				t.Fatal("accepted missing/partial acknowledgement")
			}
		})
	}
}
func TestMilvusUnorderedRangeScanIsExhaustiveAndRestartable(t *testing.T) {
	ids := []int64{-9223372036854775808, -7, 0, 1, 2, 42, 9223372036854775807}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Filter string `json:"filter"`
			Limit  int    `json:"limit"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		var low, high int64
		if _, err := fmt.Sscanf(body.Filter, "id >= %d and id <= %d", &low, &high); err != nil {
			t.Error(err)
		}
		docs := []any{}
		for i := len(ids) - 1; i >= 0; i-- {
			if ids[i] >= low && ids[i] <= high {
				docs = append(docs, map[string]any{"id": ids[i], "vector": []float32{1, 2}})
				if len(docs) == body.Limit {
					break
				}
			}
		}
		json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": docs})
	}))
	defer server.Close()
	for _, limit := range []int{1, 2, 3} {
		cursor := ""
		seen := []int64{}
		for pages := 0; pages < 200; pages++ {
			b := openProtocol(t, "milvus", server.URL)
			page, err := b.Source.Read(context.Background(), cursor, limit)
			if err != nil {
				t.Fatal(err)
			}
			if len(page.Records) > limit {
				t.Fatal("unbounded page")
			}
			for _, r := range page.Records {
				id, _ := strconv.ParseInt(r.ID, 10, 64)
				seen = append(seen, id)
			}
			if page.Done {
				break
			}
			if page.Next == "" || page.Next == cursor {
				t.Fatal("stuck cursor")
			}
			cursor = page.Next
		}
		if !reflect.DeepEqual(seen, ids) {
			t.Fatalf("range scan skipped or duplicated IDs: %v", seen)
		}
	}
}
func TestSearchAndChromaRejectLossyReads(t *testing.T) {
	for _, tc := range []struct{ kind, body string }{{"elasticsearch", `{"timed_out":true,"hits":{"hits":[]}}`}, {"opensearch", `{"_shards":{"failed":1},"hits":{"hits":[]}}`}, {"chroma", `{"ids":["a"],"embeddings":[],"metadatas":[{}]}`}, {"qdrant", `{"status":"ok","result":{"points":[{"id":1,"vector":{"named":[1,2]}}]}}`}, {"solr", `{"responseHeader":{"status":0,"partialResults":true},"nextCursorMark":"end"}`}} {
		t.Run(tc.kind, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, tc.body) }))
			defer server.Close()
			b := openProtocol(t, tc.kind, server.URL)
			if _, err := b.Source.Read(context.Background(), "", 2); err == nil {
				t.Fatal("accepted incomplete/unsupported data")
			}
		})
	}
}
func TestRedisHashReplayAndPolignIngress(t *testing.T) {
	server := miniredis.RunT(t)
	c, err := newRedis(additionalDefaults(PersonalConfig{Kind: "redis", Endpoint: "redis://" + server.Addr(), KeyPrefix: "docs:"}), PersonalCredentials{}, false)
	if err != nil {
		t.Fatal(err)
	}
	defer c.client.Close()
	records := []Record{{ID: "a", Values: []float32{1, 2}, Metadata: map[string]json.RawMessage{"tag": json.RawMessage(`"kept"`), "count": json.RawMessage(`9007199254740993`)}}}
	for i := 0; i < 2; i++ {
		if err := c.Upsert(context.Background(), records); err != nil {
			t.Fatal(err)
		}
	}
	page, err := c.Read(context.Background(), "", 1)
	if err != nil || !reflect.DeepEqual(page.Records, records) {
		t.Fatalf("Redis roundtrip: %v", err)
	}
	if base := os.Getenv("VECTOR_TRANSFER_TEST_POLIGN_URL"); base != "" {
		assertPolignIngress(t, base, "redis", records)
	}
	records[0].Metadata = nil
	if err := c.Upsert(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	page, err = c.Read(context.Background(), "", 1)
	if err != nil || len(page.Records[0].Metadata) != 0 {
		t.Fatal("Redis retained stale metadata")
	}
}
func TestNativeAndAdditionalEndpointsAreGuarded(t *testing.T) {
	for _, kind := range []string{"weaviate", "milvus", "elasticsearch", "opensearch", "chroma", "solr", "qdrant", "faiss", "pgvector", "redis", "mongodb"} {
		scheme := "https"
		s := PersonalCredentials{APIKey: "private-key"}
		if kind == "pgvector" || kind == "redis" || kind == "mongodb" {
			scheme = map[string]string{"pgvector": "postgresql", "redis": "rediss", "mongodb": "mongodb"}[kind]
			s = PersonalCredentials{Username: "own-user", Password: "own-password"}
		}
		c := PersonalConfig{Kind: kind, Endpoint: scheme + "://127.0.0.1", Collection: "docs", Index: "docs", Database: "data", KeyPrefix: "docs:"}
		if _, err := OpenPersonal(c, s); err == nil {
			t.Fatalf("%s accepted private endpoint", kind)
		}
		c.Endpoint = scheme + "://public.example?password=secret"
		if _, err := OpenPersonal(c, s); err == nil {
			t.Fatal("accepted credential URL")
		}
	}
	t.Setenv("PGUSER", "server-user")
	t.Setenv("PGPASSWORD", "server-secret")
	t.Setenv("PGHOST", "private-server")
	p, err := newPGVector(additionalDefaults(PersonalConfig{Kind: "pgvector", Endpoint: "postgresql://public.example", Database: "own-db", Collection: "docs"}), PersonalCredentials{Username: "own-user", Password: "own-secret"}, true)
	if err != nil {
		t.Fatal(err)
	}
	if p.config.User != "own-user" || p.config.Password != "own-secret" || p.config.Host != "public.example" || p.config.TLSConfig.InsecureSkipVerify || len(p.config.Fallbacks) > 0 || p.config.DialFunc == nil {
		t.Fatal("PostgreSQL inherited unsafe server settings")
	}
}
