package connector

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"reflect"
	"testing"
)

func TestFAISSServiceAndPolignIngress(t *testing.T) {
	base := os.Getenv("VECTOR_TRANSFER_TEST_FAISS_URL")
	if base == "" {
		t.Skip("set isolated FAISS adapter URL")
	}
	b, err := OpenPersonal(PersonalConfig{Kind: "faiss", Endpoint: "https://fixture.example", Collection: "vectors"}, PersonalCredentials{APIKey: "fixture-faiss-key-for-integration"})
	if err != nil {
		t.Fatal(err)
	}
	defer Registry{"faiss": b}.Close()
	p := b.Source.(*Polign)
	p.remote.base = base
	p.remote.client.Transport = http.DefaultTransport // Local fixture only; production has no override.
	rs := []Record{{ID: "a", Values: []float32{1, 2}, Metadata: map[string]json.RawMessage{"count": json.RawMessage(`9007199254740993`)}}, {ID: "b", Values: []float32{3, 4}, Metadata: map[string]json.RawMessage{"tag": json.RawMessage(`"kept"`)}}}
	for i := 0; i < 2; i++ {
		if err := b.Sink.Upsert(context.Background(), rs); err != nil {
			t.Fatal(err)
		}
	}
	page, err := b.Source.Read(context.Background(), "", 1)
	if err != nil {
		t.Fatal(err)
	}
	last, err := b.Source.Read(context.Background(), page.Next, 2)
	if err != nil {
		t.Fatal(err)
	}
	got := append(page.Records, last.Records...)
	if !last.Done || !reflect.DeepEqual(got, rs) {
		t.Fatal("FAISS roundtrip changed records")
	}
	if polign := os.Getenv("VECTOR_TRANSFER_TEST_POLIGN_URL"); polign != "" {
		assertPolignIngress(t, polign, "faiss", got)
	}
}
