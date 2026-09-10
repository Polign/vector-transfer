package connector

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestElasticsearchLegacySourceFallback(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		var body map[string]json.RawMessage
		json.NewDecoder(r.Body).Decode(&body)
		if calls == 1 {
			w.WriteHeader(400)
			io.WriteString(w, `{"error":"unknown source option; provider details stay private"}`)
			return
		}
		if string(body["_source"]) != "true" {
			t.Error("legacy source shape not used")
		}
		io.WriteString(w, protocols()[3].readResponse)
	}))
	defer server.Close()
	b := openProtocol(t, "elasticsearch", server.URL)
	page, err := b.Source.Read(context.Background(), "", 2)
	if err != nil || calls != 2 || len(page.Records) != 1 {
		t.Fatalf("legacy compatibility: %v", err)
	}
}
func TestChromaClearsOldMetadataAndRejectsLostDocuments(t *testing.T) {
	oldDocument := false
	writes := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path[len(r.URL.Path)-4:] == "/get" {
			document := "null"
			if oldDocument {
				document = `"existing document"`
			}
			io.WriteString(w, `{"ids":["a"],"metadatas":[{"old":"remove","kept":"old"}],"documents":[`+document+`],"uris":[null]}`)
			return
		}
		writes++
		var body struct {
			Metadata []map[string]json.RawMessage `json:"metadatas"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		if string(body.Metadata[0]["old"]) != "null" || string(body.Metadata[0]["kept"]) != `"new"` {
			t.Error("Chroma retained stale metadata")
		}
		io.WriteString(w, `{}`)
	}))
	defer server.Close()
	b := openProtocol(t, "chroma", server.URL)
	rs := []Record{{ID: "a", Values: []float32{1, 2}, Metadata: map[string]json.RawMessage{"kept": json.RawMessage(`"new"`)}}}
	if err := b.Sink.Upsert(context.Background(), rs); err != nil {
		t.Fatal(err)
	}
	oldDocument = true
	if err := b.Sink.Upsert(context.Background(), rs); err == nil {
		t.Fatal("silently retained an absent document")
	}
	if writes != 1 {
		t.Fatal("wrote before rejecting incompatible document")
	}
}
