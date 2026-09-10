package connector

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/drivertest"
)

func TestPGVectorRealRoundTripAndPolign(t *testing.T) {
	endpoint := os.Getenv("VECTOR_TRANSFER_TEST_PGVECTOR_ENDPOINT")
	if endpoint == "" {
		t.Skip("set isolated pgvector fixture endpoint and CA file")
	}
	cfg := additionalDefaults(PersonalConfig{Kind: "pgvector", Endpoint: endpoint, Database: "postgres", Collection: "transfer_fixture"})
	c, err := newPGVector(cfg, PersonalCredentials{Username: "transfer_test", Password: "fixture-unused"}, false)
	if err != nil {
		t.Fatal(err)
	}
	pem, err := os.ReadFile(os.Getenv("VECTOR_TRANSFER_TEST_PGVECTOR_CA"))
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		t.Fatal("invalid fixture CA")
	}
	c.config.TLSConfig.RootCAs = pool
	ctx := context.Background()
	conn, err := pgx.ConnectConfig(ctx, c.config)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS vector; CREATE TABLE IF NOT EXISTS transfer_fixture (id text PRIMARY KEY, vector vector(2), metadata jsonb); TRUNCATE transfer_fixture`); err != nil {
		t.Fatal(err)
	}
	records := []Record{{ID: "a", Values: []float32{1, 2}, Metadata: map[string]json.RawMessage{"count": json.RawMessage(`9007199254740993`)}}, {ID: "b'quoted", Values: []float32{3, 4}, Metadata: map[string]json.RawMessage{"tag": json.RawMessage(`"kept"`)}}}
	for i := 0; i < 2; i++ {
		if err := c.Upsert(ctx, records); err != nil {
			t.Fatal(err)
		}
	}
	page, err := c.Read(ctx, "", 1)
	if err != nil || len(page.Records) != 1 || page.Next != "a" {
		t.Fatalf("pgvector first page %v", err)
	}
	reopened := &PGVector{c.config.Copy(), cfg}
	last, err := reopened.Read(ctx, page.Next, 2)
	if err != nil || !last.Done || len(last.Records) != 1 {
		t.Fatalf("pgvector resume %v", err)
	}
	got := append(page.Records, last.Records...)
	if !reflect.DeepEqual(got, records) {
		t.Fatal("pgvector changed records")
	}
	if base := os.Getenv("VECTOR_TRANSFER_TEST_POLIGN_URL"); base != "" {
		assertPolignIngress(t, base, "pgvector", got)
	}
	records[0].Metadata = nil
	if err := c.Upsert(ctx, records[:1]); err != nil {
		t.Fatal(err)
	}
	page, err = c.Read(ctx, "", 1)
	if err != nil || len(page.Records[0].Metadata) != 0 {
		t.Fatal("pgvector retained stale metadata")
	}
}
func TestMongoProtocolReadWriteAndPolign(t *testing.T) {
	empty := bson.D{{Key: "ok", Value: 1}, {Key: "cursor", Value: bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "docs.docs"}, {Key: "firstBatch", Value: bson.A{}}}}}
	row := bson.D{{Key: "_id", Value: "a"}, {Key: "vector", Value: bson.A{1.0, 2.0}}, {Key: "tag", Value: "kept"}}
	page := bson.D{{Key: "ok", Value: 1}, {Key: "cursor", Value: bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "docs.docs"}, {Key: "firstBatch", Value: bson.A{row}}}}}
	deployment := drivertest.NewMockDeployment(empty, page, bson.D{{Key: "ok", Value: 1}, {Key: "n", Value: 1}, {Key: "nModified", Value: 1}}, bson.D{{Key: "ok", Value: 1}, {Key: "n", Value: 1}, {Key: "nModified", Value: 1}})
	writes := 0
	opts := options.Client().SetMonitor(&event.CommandMonitor{Started: func(ctx context.Context, e *event.CommandStartedEvent) {
		if e.CommandName == "update" {
			writes++
			raw := e.Command.Lookup("updates").Array()
			items, _ := raw.Values()
			doc := items[0].Document()
			if !doc.Lookup("upsert").Boolean() {
				t.Error("MongoDB did not upsert")
			}
			if doc.Lookup("u").Document().Lookup("_id").StringValue() != "a" {
				t.Error("MongoDB lost ID")
			}
		}
	}})
	opts.Deployment = deployment
	client, err := mongo.Connect(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Disconnect(context.Background())
	c := &Mongo{client, client.Database("docs").Collection("docs"), additionalDefaults(PersonalConfig{Kind: "mongodb", IDType: "string"})}
	out, err := c.Read(context.Background(), "", 2)
	if err != nil {
		t.Fatal(err)
	}
	if !out.Done || len(out.Records) != 1 || out.Records[0].ID != "a" {
		t.Fatal("MongoDB lost record")
	}
	for i := 0; i < 2; i++ {
		if err := c.Upsert(context.Background(), out.Records); err != nil {
			t.Fatal(err)
		}
	}
	if writes != 2 {
		t.Fatal("missing MongoDB replay")
	}
	if base := os.Getenv("VECTOR_TRANSFER_TEST_POLIGN_URL"); base != "" {
		assertPolignIngress(t, base, "mongodb", out.Records)
	}
	deployment.AddResponses(bson.D{{Key: "ok", Value: 1}, {Key: "n", Value: 0}})
	if err := c.Upsert(context.Background(), out.Records); err == nil {
		t.Fatal("MongoDB accepted missing acknowledgement")
	}
}
