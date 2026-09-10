package memorydb_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Polign/vector-transfer/connector"
	"github.com/Polign/vector-transfer/connector/conformance"
	"github.com/Polign/vector-transfer/examples/custom/memorydb"
)

func TestSourceContract(t *testing.T) {
	conformance.TestSource(t, conformance.SourceFixture{
		Open: func(t *testing.T) connector.Source { return memorydb.New(memorydb.SampleRecords()) },
		Want: memorydb.SampleRecords(), Dimension: 2,
	})
}

func TestSinkContract(t *testing.T) {
	conformance.TestSink(t, conformance.SinkFixture{
		Open: func(t *testing.T) (connector.Sink, func(context.Context) ([]connector.Record, error)) {
			db := memorydb.New(nil)
			return db, db.Records
		},
		Records: memorydb.SampleRecords(), Dimension: 2,
	})
}

func TestFactoryThroughPublicLoader(t *testing.T) {
	registry, err := connector.LoadConfig(context.Background(), "../config.json", connector.Factories{"memorydb": memorydb.Factory()})
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	if registry["sample"].CanWrite || !registry["destination"].CanWrite {
		t.Fatal("incorrect capabilities")
	}
	page, err := registry["sample"].Source.Read(context.Background(), "", 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry["destination"].Sink.Upsert(context.Background(), page.Records); err != nil {
		t.Fatal(err)
	}
	got, err := registry["destination"].Source.Read(context.Background(), "", 2)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := json.Marshal(page.Records)
	b, _ := json.Marshal(got.Records)
	if string(a) != string(b) {
		t.Fatal("factory-loaded transfer changed records")
	}
}
