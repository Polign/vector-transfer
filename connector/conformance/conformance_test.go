package conformance

import (
	"encoding/json"
	"testing"

	"github.com/Polign/vector-transfer/connector"
)

func TestMetadataComparisonRetainsNumericPrecision(t *testing.T) {
	r := func(raw string) []connector.Record {
		return []connector.Record{{ID: "a", Values: []float32{1}, Metadata: map[string]json.RawMessage{"data": json.RawMessage(raw)}}}
	}
	if !sameRecords(r(`{"a":1,"b":[true,1e3]}`), r(`{ "b": [true,1000.0], "a": 1.0 }`)) {
		t.Fatal("equivalent JSON rejected")
	}
	if sameRecords(r(`9007199254740992`), r(`9007199254740993`)) {
		t.Fatal("large integer precision was lost")
	}
	if sameRecords(r(`"1"`), r(`1`)) {
		t.Fatal("metadata type was lost")
	}
	if sameRecords(append(r(`1`), r(`1`)...), append(r(`1`), r(`1`)...)) {
		t.Fatal("duplicate IDs accepted")
	}
}
