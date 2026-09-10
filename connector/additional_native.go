package connector

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	redisclient "github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

func nativeError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	var n net.Error
	if errors.As(err, &n) {
		return &Transient{errors.New("database network operation failed")}
	}
	return errors.New("database operation failed; check credentials, permissions, and schema")
}

type PGVector struct {
	config *pgx.ConnConfig
	fields PersonalConfig
}

func newPGVector(c PersonalConfig, s PersonalCredentials, public bool) (*PGVector, error) {
	// Parse a fixed configuration solely to initialize pgx's private invariants.
	// Explicitly disable service/pass files and TLS file discovery; never inherit
	// the host's user, password, runtime parameters, certificates, or fallback hosts.
	cfg, err := pgx.ParseConfig("host=localhost port=5432 user=vector_transfer password=unused dbname=postgres sslmode=disable servicefile=/dev/null passfile=/dev/null sslcert='' sslkey='' sslrootcert=''")
	if err != nil {
		return nil, errors.New("PostgreSQL client configuration failed")
	}
	u, _ := url.Parse(c.Endpoint)
	cfg.Host = u.Hostname()
	cfg.Port = 5432
	if u.Port() != "" {
		n, err := strconv.ParseUint(u.Port(), 10, 16)
		if err != nil || n == 0 {
			return nil, errors.New("invalid PostgreSQL port")
		}
		cfg.Port = uint16(n)
	}
	cfg.User = s.Username
	cfg.Password = s.Password
	cfg.Database = c.Database
	cfg.RuntimeParams = map[string]string{"application_name": "vector-transfer"}
	cfg.Fallbacks = nil
	cfg.ConnectTimeout = 10 * time.Second
	cfg.OAuthTokenProvider = nil
	cfg.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12, ServerName: u.Hostname()}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	if public {
		cfg.LookupFunc = func(_ context.Context, host string) ([]string, error) { return []string{host}, nil }
		cfg.DialFunc = publicDial
	}
	return &PGVector{cfg, c}, nil
}
func (c *PGVector) Read(ctx context.Context, cursor string, limit int) (Page, error) {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	conn, err := pgx.ConnectConfig(ctx, c.config)
	if err != nil {
		return Page{}, nativeError(ctx, err)
	}
	defer conn.Close(context.Background())
	id := pgx.Identifier{c.fields.IDField}.Sanitize()
	vec := pgx.Identifier{c.fields.VectorField}.Sanitize()
	meta := pgx.Identifier{c.fields.MetadataField}.Sanitize()
	table := pgx.Identifier{"public", c.fields.Collection}.Sanitize()
	query := "SELECT " + id + "::text," + vec + "::text," + meta + "::text FROM " + table
	args := []any{limit}
	if cursor != "" {
		query += " WHERE " + id + "::text COLLATE \"C\" > $2"
		args = append(args, cursor)
	}
	query += " ORDER BY " + id + "::text COLLATE \"C\" LIMIT $1"
	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return Page{}, nativeError(ctx, err)
	}
	defer rows.Close()
	page := Page{}
	for rows.Next() {
		var id, vector string
		var metadata *string
		if err := rows.Scan(&id, &vector, &metadata); err != nil {
			return Page{}, nativeError(ctx, err)
		}
		r := Record{ID: id}
		if json.Unmarshal([]byte(vector), &r.Values) != nil {
			return Page{}, errors.New("invalid pgvector value")
		}
		if metadata != nil && json.Unmarshal([]byte(*metadata), &r.Metadata) != nil {
			return Page{}, errors.New("pgvector metadata must be a JSON object")
		}
		page.Records = append(page.Records, r)
		page.Next = id
	}
	if err := rows.Err(); err != nil {
		return Page{}, nativeError(ctx, err)
	}
	page.Done = len(page.Records) < limit
	return page, nil
}
func (c *PGVector) Upsert(ctx context.Context, rs []Record) error {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	conn, err := pgx.ConnectConfig(ctx, c.config)
	if err != nil {
		return nativeError(ctx, err)
	}
	defer conn.Close(context.Background())
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nativeError(ctx, err)
	}
	defer tx.Rollback(context.Background())
	id := pgx.Identifier{c.fields.IDField}.Sanitize()
	vec := pgx.Identifier{c.fields.VectorField}.Sanitize()
	meta := pgx.Identifier{c.fields.MetadataField}.Sanitize()
	table := pgx.Identifier{"public", c.fields.Collection}.Sanitize()
	query := "INSERT INTO " + table + " (" + id + "," + vec + "," + meta + ") VALUES ($1,$2::vector,$3::jsonb) ON CONFLICT (" + id + ") DO UPDATE SET " + vec + "=EXCLUDED." + vec + "," + meta + "=EXCLUDED." + meta
	for _, r := range rs {
		vector, err := json.Marshal(r.Values)
		if err != nil {
			return errors.New("invalid vector")
		}
		metadata, err := json.Marshal(r.Metadata)
		if err != nil {
			return errors.New("invalid metadata")
		}
		tag, err := tx.Exec(ctx, query, r.ID, string(vector), string(metadata))
		if err != nil {
			return nativeError(ctx, err)
		}
		if tag.RowsAffected() != 1 {
			return errors.New("PostgreSQL did not acknowledge one row")
		}
	}
	return nativeError(ctx, tx.Commit(ctx))
}

type Redis struct {
	client *redisclient.Client
	config PersonalConfig
}

func newRedis(c PersonalConfig, s PersonalCredentials, public bool) (*Redis, error) {
	u, _ := url.Parse(c.Endpoint)
	address := u.Host
	if u.Port() == "" {
		address = net.JoinHostPort(u.Hostname(), "6379")
	}
	db, err := strconv.Atoi(c.Database)
	if err != nil || db < 0 || db > 15 {
		return nil, errors.New("Redis database must be 0–15")
	}
	o := &redisclient.Options{Addr: address, Username: s.Username, Password: s.Password, DB: db, MaxRetries: -1, PoolSize: 4, Protocol: 2, DialTimeout: 10 * time.Second, ReadTimeout: 45 * time.Second, WriteTimeout: 45 * time.Second, ContextTimeoutEnabled: true, DisableIdentity: true}
	if u.Scheme == "rediss" {
		o.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12, ServerName: u.Hostname()}
	}
	if public {
		o.Dialer = func(ctx context.Context, network, addr string) (net.Conn, error) {
			ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			conn, err := publicDial(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			secure := tls.Client(conn, o.TLSConfig)
			if err := secure.HandshakeContext(ctx); err != nil {
				conn.Close()
				return nil, errors.New("Redis TLS handshake failed")
			}
			return secure, nil
		}
	}
	return &Redis{redisclient.NewClient(o), c}, nil
}

type redisPosition struct {
	Scan    uint64   `json:"scan"`
	Pending []string `json:"pending,omitempty"`
}

func (c *Redis) Read(ctx context.Context, cursor string, limit int) (Page, error) {
	if limit < 1 {
		return Page{}, errors.New("positive page size required")
	}
	p := redisPosition{}
	if cursor != "" {
		if err := decodeCursor(cursor, &p); err != nil {
			return Page{}, err
		}
	}
	if len(p.Pending) == 0 {
		keys, next, err := c.client.Scan(ctx, p.Scan, c.config.KeyPrefix+"*", int64(min(limit, 500))).Result()
		if err != nil {
			return Page{}, nativeError(ctx, err)
		}
		p.Pending = keys
		p.Scan = next
	}
	page := Page{}
	seen := map[string]bool{}
	n := min(limit, len(p.Pending))
	for _, key := range p.Pending[:n] {
		if !strings.HasPrefix(key, c.config.KeyPrefix) {
			return Page{}, errors.New("Redis key escaped configured prefix")
		}
		if seen[key] {
			continue
		}
		seen[key] = true
		fields, err := c.client.HGetAll(ctx, key).Result()
		if err != nil {
			return Page{}, nativeError(ctx, err)
		}
		raw, ok := fields[c.config.VectorField]
		if !ok || len(raw) == 0 || len(raw)%4 != 0 {
			return Page{}, errors.New("Redis HASH needs a FLOAT32 little-endian vector")
		}
		r := Record{ID: strings.TrimPrefix(key, c.config.KeyPrefix), Values: make([]float32, len(raw)/4), Metadata: map[string]json.RawMessage{}}
		for i := range r.Values {
			r.Values[i] = math.Float32frombits(binary.LittleEndian.Uint32([]byte(raw[i*4 : i*4+4])))
		}
		if metadata, ok := fields[c.config.MetadataField]; ok && json.Unmarshal([]byte(metadata), &r.Metadata) != nil {
			return Page{}, errors.New("Redis metadata field must contain a JSON object")
		}
		if r.Metadata == nil {
			r.Metadata = map[string]json.RawMessage{}
		}
		for k, v := range fields {
			if k == c.config.VectorField || k == c.config.MetadataField {
				continue
			}
			if _, exists := r.Metadata[k]; exists {
				return Page{}, errors.New("Redis metadata fields collide")
			}
			r.Metadata[k], _ = json.Marshal(v)
		}
		page.Records = append(page.Records, r)
	}
	p.Pending = p.Pending[n:]
	page.Done = p.Scan == 0 && len(p.Pending) == 0
	page.Next = encodeCursor(p)
	if len(page.Next) > 1<<20 {
		return Page{}, errors.New("Redis scan cursor exceeds safety limit")
	}
	return page, nil
}
func (c *Redis) Upsert(ctx context.Context, rs []Record) error {
	// Replace the complete HASH atomically. Replaying a record cannot retain stale
	// metadata or duplicate it; the key prefix is the configured resource boundary.
	const script = "redis.call('DEL',KEYS[1]); return redis.call('HSET',KEYS[1],ARGV[1],ARGV[2],ARGV[3],ARGV[4])"
	for _, r := range rs {
		if r.ID == "" {
			return errors.New("empty Redis record ID")
		}
		vector := make([]byte, 4*len(r.Values))
		for i, v := range r.Values {
			binary.LittleEndian.PutUint32(vector[i*4:], math.Float32bits(v))
		}
		metadata, err := json.Marshal(r.Metadata)
		if err != nil {
			return errors.New("invalid metadata")
		}
		n, err := c.client.Eval(ctx, script, []string{c.config.KeyPrefix + r.ID}, c.config.VectorField, vector, c.config.MetadataField, string(metadata)).Int()
		if err != nil {
			return nativeError(ctx, err)
		}
		if n != 2 {
			return errors.New("Redis did not acknowledge the record")
		}
	}
	return nil
}

type Mongo struct {
	client     *mongo.Client
	collection *mongo.Collection
	config     PersonalConfig
}
type guardedMongoDialer struct{}

func (guardedMongoDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return publicDial(ctx, network, address)
}
func newMongo(c PersonalConfig, s PersonalCredentials, public bool) (*Mongo, error) {
	o := options.Client().ApplyURI(c.Endpoint).SetTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}).SetMaxPoolSize(4).SetConnectTimeout(10 * time.Second).SetServerSelectionTimeout(15 * time.Second).SetTimeout(60 * time.Second).SetWriteConcern(writeconcern.Majority())
	if s.Username != "" {
		o.SetAuth(options.Credential{AuthSource: c.AuthDatabase, Username: s.Username, Password: s.Password})
	}
	if public {
		o.SetDialer(guardedMongoDialer{})
	}
	client, err := mongo.Connect(o)
	if err != nil {
		return nil, errors.New("invalid MongoDB connection configuration")
	}
	return &Mongo{client, client.Database(c.Database).Collection(c.Collection), c}, nil
}
func (c *Mongo) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return c.client.Disconnect(ctx)
}
func (c *Mongo) recordID(id string) (any, error) {
	switch c.config.IDType {
	case "objectid":
		v, err := bson.ObjectIDFromHex(id)
		if err != nil {
			return nil, errors.New("MongoDB requires ObjectID hex IDs")
		}
		return v, nil
	case "int64":
		v, err := strconv.ParseInt(id, 10, 64)
		if err != nil || strconv.FormatInt(v, 10) != id {
			return nil, errors.New("MongoDB requires canonical INT64 IDs")
		}
		return v, nil
	default:
		return id, nil
	}
}
func (c *Mongo) Read(ctx context.Context, cursor string, limit int) (Page, error) {
	// Reject mixed ID types before applying a type-bracketed keyset predicate.
	typ := map[string]string{"objectid": "objectId", "int64": "long", "string": "string"}[c.config.IDType]
	err := c.collection.FindOne(ctx, bson.M{"$expr": bson.M{"$ne": bson.A{bson.M{"$type": "$_id"}, typ}}}, options.FindOne().SetCollation(&options.Collation{Locale: "simple"}).SetProjection(bson.M{"_id": 1})).Err()
	if err == nil {
		return Page{}, errors.New("MongoDB collection contains incompatible ID types")
	}
	if !errors.Is(err, mongo.ErrNoDocuments) {
		return Page{}, nativeError(ctx, err)
	}
	filter := bson.M{}
	if cursor != "" {
		id, err := c.recordID(cursor)
		if err != nil {
			return Page{}, err
		}
		filter["_id"] = bson.M{"$gt": id}
	}
	rows, err := c.collection.Find(ctx, filter, options.Find().SetCollation(&options.Collation{Locale: "simple"}).SetSort(bson.D{{Key: "_id", Value: 1}}).SetLimit(int64(limit)))
	if err != nil {
		return Page{}, nativeError(ctx, err)
	}
	defer rows.Close(ctx)
	page := Page{}
	for rows.Next(ctx) {
		var doc bson.M
		if err := rows.Decode(&doc); err != nil {
			return Page{}, errors.New("invalid MongoDB document")
		}
		r := Record{}
		switch id := doc["_id"].(type) {
		case bson.ObjectID:
			r.ID = id.Hex()
		case string:
			r.ID = id
		case int64:
			r.ID = strconv.FormatInt(id, 10)
		default:
			return Page{}, errors.New("unsupported MongoDB ID")
		}
		values, ok := doc[c.config.VectorField].(bson.A)
		if !ok {
			return Page{}, errors.New("MongoDB vector field must be a dense numeric array")
		}
		for _, value := range values {
			switch v := value.(type) {
			case float64:
				r.Values = append(r.Values, float32(v))
			case int32:
				r.Values = append(r.Values, float32(v))
			case int64:
				r.Values = append(r.Values, float32(v))
			default:
				return Page{}, errors.New("MongoDB vector contains a nonnumeric value")
			}
		}
		delete(doc, "_id")
		delete(doc, c.config.VectorField)
		raw, err := bson.MarshalExtJSON(doc, true, false)
		if err != nil || json.Unmarshal(raw, &r.Metadata) != nil {
			return Page{}, errors.New("MongoDB metadata could not be represented as Extended JSON")
		}
		page.Records = append(page.Records, r)
		page.Next = r.ID
	}
	if err := rows.Err(); err != nil {
		return Page{}, nativeError(ctx, err)
	}
	page.Done = len(page.Records) < limit
	return page, nil
}
func (c *Mongo) Upsert(ctx context.Context, rs []Record) error {
	for _, r := range rs {
		if _, ok := r.Metadata["_id"]; ok {
			return errors.New("MongoDB _id is reserved")
		}
		if _, ok := r.Metadata[c.config.VectorField]; ok {
			return errors.New("MongoDB vector field collides with metadata")
		}
		raw, err := json.Marshal(r.Metadata)
		if err != nil {
			return errors.New("invalid metadata")
		}
		doc := bson.M{}
		if string(raw) != "null" && bson.UnmarshalExtJSON(raw, false, &doc) != nil {
			return errors.New("invalid MongoDB Extended JSON metadata")
		}
		id, err := c.recordID(r.ID)
		if err != nil {
			return err
		}
		doc["_id"] = id
		values := bson.A{}
		for _, v := range r.Values {
			values = append(values, float64(v))
		}
		doc[c.config.VectorField] = values
		out, err := c.collection.ReplaceOne(ctx, bson.M{"_id": id}, doc, options.Replace().SetCollation(&options.Collation{Locale: "simple"}).SetUpsert(true))
		if err != nil {
			return nativeError(ctx, err)
		}
		if !out.Acknowledged || out.MatchedCount+out.UpsertedCount != 1 {
			return errors.New("MongoDB did not acknowledge one record")
		}
	}
	return nil
}
