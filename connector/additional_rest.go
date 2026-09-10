package connector

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math"
	"net/url"
	"sort"
	"strconv"
	"strings"
)

// RESTDatabase implements exhaustive scans and acknowledged ID-based writes.
// Provider cursors contain positions only, never credentials or remote URLs.
type RESTDatabase struct {
	remote *remote
	config PersonalConfig
}

func (c *RESTDatabase) Read(ctx context.Context, cursor string, limit int) (Page, error) {
	if limit < 1 {
		return Page{}, errors.New("positive page size required")
	}
	limit = min(limit, 500)
	switch c.config.Kind {
	case "qdrant":
		return c.readQdrant(ctx, cursor, limit)
	case "weaviate":
		return c.readWeaviate(ctx, cursor, limit)
	case "milvus":
		return c.readMilvus(ctx, cursor, limit)
	case "chroma":
		return c.readChroma(ctx, cursor, limit)
	case "elasticsearch", "opensearch":
		return c.readSearch(ctx, cursor, limit)
	case "solr":
		return c.readSolr(ctx, cursor, limit)
	}
	return Page{}, errors.New("unsupported REST source")
}
func (c *RESTDatabase) Upsert(ctx context.Context, rs []Record) error {
	if len(rs) == 0 {
		return nil
	}
	return chunks(rs, 100, 4<<20, func(rs []Record) any { return rs }, func(body any) error {
		rs := body.([]Record)
		switch c.config.Kind {
		case "qdrant":
			return c.writeQdrant(ctx, rs)
		case "weaviate":
			return c.writeWeaviate(ctx, rs)
		case "milvus":
			return c.writeMilvus(ctx, rs)
		case "chroma":
			return c.writeChroma(ctx, rs)
		case "elasticsearch", "opensearch":
			return c.writeSearch(ctx, rs)
		case "solr":
			return c.writeSolr(ctx, rs)
		}
		return errors.New("unsupported REST sink")
	})
}
func (c *RESTDatabase) readQdrant(ctx context.Context, cursor string, limit int) (Page, error) {
	body := map[string]any{"limit": limit, "with_vector": true, "with_payload": true}
	if cursor != "" {
		if !json.Valid([]byte(cursor)) {
			return Page{}, errors.New("invalid Qdrant cursor")
		}
		body["offset"] = json.RawMessage(cursor)
	}
	var out struct {
		Status string `json:"status"`
		Result *struct {
			Points []struct {
				ID      json.RawMessage            `json:"id"`
				Vector  []float32                  `json:"vector"`
				Payload map[string]json.RawMessage `json:"payload"`
			} `json:"points"`
			Next json.RawMessage `json:"next_page_offset"`
		} `json:"result"`
	}
	if err := c.remote.call(ctx, "POST", "/collections/"+url.PathEscape(c.config.Collection)+"/points/scroll", body, &out); err != nil {
		return Page{}, err
	}
	if out.Status != "ok" || out.Result == nil {
		return Page{}, errors.New("Qdrant scan was not acknowledged")
	}
	page := Page{Next: string(out.Result.Next), Done: len(out.Result.Next) == 0 || string(out.Result.Next) == "null"}
	for _, p := range out.Result.Points {
		id, err := decodeID(p.ID)
		if err != nil {
			return Page{}, err
		}
		page.Records = append(page.Records, Record{ID: id, Values: p.Vector, Metadata: p.Payload})
	}
	return page, nil
}
func qdrantID(id string) (any, error) {
	if n, err := strconv.ParseUint(id, 10, 64); err == nil && strconv.FormatUint(n, 10) == id {
		return n, nil
	}
	if uuidPattern.MatchString(id) {
		return id, nil
	}
	return nil, errors.New("Qdrant IDs must be UUIDs or unsigned integers")
}
func (c *RESTDatabase) writeQdrant(ctx context.Context, rs []Record) error {
	points := []any{}
	for _, r := range rs {
		id, err := qdrantID(r.ID)
		if err != nil {
			return err
		}
		points = append(points, map[string]any{"id": id, "vector": r.Values, "payload": r.Metadata})
	}
	var out struct {
		Status string `json:"status"`
		Result struct {
			Status string `json:"status"`
		} `json:"result"`
	}
	if err := c.remote.call(ctx, "PUT", "/collections/"+url.PathEscape(c.config.Collection)+"/points?wait=true", map[string]any{"points": points}, &out); err != nil {
		return err
	}
	if out.Status != "ok" || out.Result.Status != "completed" {
		return errors.New("Qdrant write was not completed")
	}
	return nil
}
func (c *RESTDatabase) readWeaviate(ctx context.Context, cursor string, limit int) (Page, error) {
	q := url.Values{"class": {c.config.Collection}, "include": {"vector"}, "limit": {strconv.Itoa(limit)}}
	if c.config.Tenant != "" {
		q.Set("tenant", c.config.Tenant)
	}
	if cursor != "" {
		if !uuidPattern.MatchString(cursor) {
			return Page{}, errors.New("invalid Weaviate cursor")
		}
		q.Set("after", cursor)
	}
	var out struct {
		Objects *[]struct {
			ID         string                     `json:"id"`
			Vector     []float32                  `json:"vector"`
			Properties map[string]json.RawMessage `json:"properties"`
		} `json:"objects"`
	}
	if err := c.remote.call(ctx, "GET", "/v1/objects?"+q.Encode(), nil, &out); err != nil {
		return Page{}, err
	}
	if out.Objects == nil {
		return Page{}, errors.New("missing Weaviate objects")
	}
	page := Page{Done: len(*out.Objects) < limit}
	for _, o := range *out.Objects {
		if !uuidPattern.MatchString(o.ID) {
			return Page{}, errors.New("invalid Weaviate object ID")
		}
		page.Records = append(page.Records, Record{ID: o.ID, Values: o.Vector, Metadata: o.Properties})
		page.Next = o.ID
	}
	return page, nil
}
func (c *RESTDatabase) writeWeaviate(ctx context.Context, rs []Record) error {
	objects := []any{}
	for _, r := range rs {
		if !uuidPattern.MatchString(r.ID) {
			return errors.New("Weaviate requires UUID record IDs")
		}
		o := map[string]any{"class": c.config.Collection, "id": r.ID, "vector": r.Values, "properties": r.Metadata}
		if c.config.Tenant != "" {
			o["tenant"] = c.config.Tenant
		}
		objects = append(objects, o)
	}
	var out []struct {
		ID     string `json:"id"`
		Result struct {
			Status string          `json:"status"`
			Errors json.RawMessage `json:"errors"`
		} `json:"result"`
	}
	if err := c.remote.call(ctx, "POST", "/v1/batch/objects", map[string]any{"objects": objects}, &out); err != nil {
		return err
	}
	if len(out) != len(rs) {
		return errors.New("Weaviate did not acknowledge every object")
	}
	seen := map[string]bool{}
	for _, o := range out {
		if o.Result.Status != "SUCCESS" || (len(o.Result.Errors) > 0 && string(o.Result.Errors) != "null") || seen[o.ID] {
			return errors.New("Weaviate batch contained failed objects")
		}
		seen[o.ID] = true
	}
	for _, r := range rs {
		if !seen[r.ID] {
			return errors.New("Weaviate acknowledged unexpected IDs")
		}
	}
	return nil
}

// Milvus 2.x query responses are unordered and offset pagination is capped.
// Split disjoint INT64 primary-key ranges until the entire range fits one page.
// Never advance past a truncated range. This remains restartable without a
// server-side iterator lease, at the cost of extra requests on dense ranges.
func (c *RESTDatabase) readMilvus(ctx context.Context, cursor string, limit int) (Page, error) {
	low := int64(math.MinInt64)
	if cursor != "" {
		var err error
		low, err = strconv.ParseInt(cursor, 10, 64)
		if err != nil {
			return Page{}, errors.New("invalid Milvus cursor")
		}
	}
	high := int64(math.MaxInt64)
	for {
		filter := c.config.IDField + " >= " + strconv.FormatInt(low, 10) + " and " + c.config.IDField + " <= " + strconv.FormatInt(high, 10)
		var out struct {
			Code *int                         `json:"code"`
			Data []map[string]json.RawMessage `json:"data"`
		}
		body := map[string]any{"collectionName": c.config.Collection, "dbName": c.config.Database, "filter": filter, "limit": limit + 1, "outputFields": []string{"*", c.config.VectorField}, "consistencyLevel": "Strong"}
		if err := c.remote.call(ctx, "POST", "/v2/vectordb/entities/query", body, &out); err != nil {
			return Page{}, err
		}
		if out.Code == nil || *out.Code != 0 {
			return Page{}, errors.New("Milvus query failed")
		}
		if len(out.Data) > limit {
			if low == high {
				return Page{}, errors.New("duplicate Milvus primary keys")
			}
			high = low + int64((uint64(high)-uint64(low))/2)
			continue
		}
		page := Page{Done: high == math.MaxInt64}
		if !page.Done {
			page.Next = strconv.FormatInt(high+1, 10)
		}
		for _, doc := range out.Data {
			r, err := documentRecord(doc, c.config)
			if err != nil {
				return Page{}, err
			}
			id, err := strconv.ParseInt(r.ID, 10, 64)
			if err != nil || id < low || id > high {
				return Page{}, errors.New("Milvus returned an unexpected primary key")
			}
			page.Records = append(page.Records, r)
		}
		sort.Slice(page.Records, func(i, j int) bool {
			a, _ := strconv.ParseInt(page.Records[i].ID, 10, 64)
			b, _ := strconv.ParseInt(page.Records[j].ID, 10, 64)
			return a < b
		})
		return page, nil
	}
}
func (c *RESTDatabase) writeMilvus(ctx context.Context, rs []Record) error {
	docs := []any{}
	for _, r := range rs {
		id, err := strconv.ParseInt(r.ID, 10, 64)
		if err != nil || strconv.FormatInt(id, 10) != r.ID {
			return errors.New("Milvus INT64 primary key requires canonical integer IDs")
		}
		doc, err := recordDocument(r, c.config, id)
		if err != nil {
			return err
		}
		docs = append(docs, doc)
	}
	var out struct {
		Code *int `json:"code"`
		Data struct {
			Count *int              `json:"upsertCount"`
			IDs   []json.RawMessage `json:"upsertIds"`
		} `json:"data"`
	}
	if err := c.remote.call(ctx, "POST", "/v2/vectordb/entities/upsert", map[string]any{"collectionName": c.config.Collection, "dbName": c.config.Database, "data": docs}, &out); err != nil {
		return err
	}
	if out.Code == nil || *out.Code != 0 || out.Data.Count == nil || *out.Data.Count != len(rs) {
		return errors.New("Milvus did not acknowledge every upsert")
	}
	if len(out.Data.IDs) != len(rs) {
		return errors.New("Milvus did not acknowledge the original record IDs")
	}
	for i, raw := range out.Data.IDs {
		id, err := decodeID(raw)
		if err != nil || id != rs[i].ID {
			return errors.New("Milvus changed record IDs; disable AutoID")
		}
	}
	return nil
}
func (c *RESTDatabase) chromaPath() string {
	return "/api/v2/tenants/" + url.PathEscape(c.config.Tenant) + "/databases/" + url.PathEscape(c.config.Database) + "/collections/" + url.PathEscape(c.config.Collection)
}
func (c *RESTDatabase) readChroma(ctx context.Context, cursor string, limit int) (Page, error) {
	offset := 0
	if cursor != "" {
		var err error
		offset, err = strconv.Atoi(cursor)
		if err != nil || offset < 0 {
			return Page{}, errors.New("invalid Chroma cursor")
		}
	}
	var out struct {
		IDs        *[]string                    `json:"ids"`
		Embeddings [][]float32                  `json:"embeddings"`
		Metadata   []map[string]json.RawMessage `json:"metadatas"`
		Documents  []json.RawMessage            `json:"documents"`
		URIs       []json.RawMessage            `json:"uris"`
	}
	if err := c.remote.call(ctx, "POST", c.chromaPath()+"/get", map[string]any{"offset": offset, "limit": limit, "include": []string{"embeddings", "metadatas", "documents", "uris"}}, &out); err != nil {
		return Page{}, err
	}
	if out.IDs == nil || len(*out.IDs) != len(out.Embeddings) || len(*out.IDs) != len(out.Metadata) {
		return Page{}, errors.New("incomplete Chroma response")
	}
	page := Page{Done: len(*out.IDs) < limit, Next: strconv.Itoa(offset + len(*out.IDs))}
	for i, id := range *out.IDs {
		m := out.Metadata[i]
		if m == nil {
			m = map[string]json.RawMessage{}
		}
		for key, values := range map[string][]json.RawMessage{"_chroma_document": out.Documents, "_chroma_uri": out.URIs} {
			if _, ok := m[key]; ok {
				return Page{}, errors.New("Chroma metadata uses a reserved transfer field")
			}
			if len(values) > 0 {
				if len(values) != len(*out.IDs) {
					return Page{}, errors.New("incomplete Chroma auxiliary fields")
				}
				if string(values[i]) != "null" {
					m[key] = values[i]
				}
			}
		}
		page.Records = append(page.Records, Record{ID: id, Values: out.Embeddings[i], Metadata: m})
	}
	return page, nil
}
func (c *RESTDatabase) writeChroma(ctx context.Context, rs []Record) error {
	ids := []string{}
	vectors := [][]float32{}
	metadata := []map[string]json.RawMessage{}
	documents := []any{}
	uris := []any{}
	for _, r := range rs {
		ids = append(ids, r.ID)
		vectors = append(vectors, r.Values)
		m := map[string]json.RawMessage{}
		for k, v := range r.Metadata {
			if k != "_chroma_document" && k != "_chroma_uri" {
				if string(v) == "null" || strings.HasPrefix(k, "chroma:") {
					return errors.New("Chroma does not support null or internal metadata fields")
				}
				m[k] = v
			}
		}
		if len(m) == 0 {
			m = nil
		}
		metadata = append(metadata, m)
		for key, target := range map[string]*[]any{"_chroma_document": &documents, "_chroma_uri": &uris} {
			var value any
			if raw, ok := r.Metadata[key]; ok {
				var s string
				if json.Unmarshal(raw, &s) != nil {
					return errors.New("Chroma document and URI must be strings")
				}
				value = s
			}
			*target = append(*target, value)
		}
	}
	// Chroma merges metadata on upsert. Explicitly remove old keys using its
	// null update values, so replay/replacement cannot leave stale metadata.
	var existing struct {
		IDs       *[]string                    `json:"ids"`
		Metadata  []map[string]json.RawMessage `json:"metadatas"`
		Documents []json.RawMessage            `json:"documents"`
		URIs      []json.RawMessage            `json:"uris"`
	}
	if err := c.remote.call(ctx, "POST", c.chromaPath()+"/get", map[string]any{"ids": ids, "include": []string{"metadatas", "documents", "uris"}}, &existing); err != nil {
		return err
	}
	if existing.IDs == nil || len(*existing.IDs) != len(existing.Metadata) {
		return errors.New("incomplete Chroma destination metadata response")
	}
	positions := map[string]int{}
	for i, id := range ids {
		positions[id] = i
	}
	for j, id := range *existing.IDs {
		i, ok := positions[id]
		if !ok {
			return errors.New("Chroma returned an unexpected destination ID")
		}
		if metadata[i] == nil {
			metadata[i] = map[string]json.RawMessage{}
		}
		for key := range existing.Metadata[j] {
			if _, present := metadata[i][key]; !present {
				metadata[i][key] = json.RawMessage("null")
			}
		}
		for key, old := range map[string][]json.RawMessage{"document": existing.Documents, "URI": existing.URIs} {
			if len(old) != 0 && len(old) != len(*existing.IDs) {
				return errors.New("incomplete Chroma destination auxiliary fields")
			}
			value := documents[i]
			if key == "URI" {
				value = uris[i]
			}
			if len(old) > 0 && string(old[j]) != "null" && value == nil {
				return errors.New("Chroma destination has a document or URI absent from the source; use an empty destination")
			}
		}
		if len(metadata[i]) == 0 {
			metadata[i] = nil
		}
	}
	return c.remote.call(ctx, "POST", c.chromaPath()+"/upsert", map[string]any{"ids": ids, "embeddings": vectors, "metadatas": metadata, "documents": documents, "uris": uris}, nil)
}
func encodeCursor(v any) string {
	b, _ := json.Marshal(v)
	return base64.RawURLEncoding.EncodeToString(b)
}
func decodeCursor(s string, v any) error {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil || len(b) > 1<<20 || json.Unmarshal(b, v) != nil {
		return errors.New("invalid connector cursor")
	}
	return nil
}
func (c *RESTDatabase) readSearch(ctx context.Context, cursor string, limit int) (Page, error) {
	body := map[string]any{"size": limit, "query": map[string]any{"match_all": map[string]any{}}, "sort": []any{map[string]string{c.config.IDField: "asc"}}, "track_total_hits": false, "_source": true}
	if c.config.Kind == "elasticsearch" {
		body["_source"] = map[string]any{"exclude_vectors": false}
	}
	if cursor != "" {
		var after []json.RawMessage
		if err := decodeCursor(cursor, &after); err != nil || len(after) != 1 {
			return Page{}, errors.New("invalid search cursor")
		}
		body["search_after"] = after
	}
	var out struct {
		TimedOut bool `json:"timed_out"`
		Shards   struct {
			Failed int `json:"failed"`
		} `json:"_shards"`
		Hits *struct {
			Hits []struct {
				ID     string                     `json:"_id"`
				Source map[string]json.RawMessage `json:"_source"`
				Sort   []json.RawMessage          `json:"sort"`
			} `json:"hits"`
		} `json:"hits"`
	}
	err := c.remote.call(ctx, "POST", "/"+url.PathEscape(c.config.Index)+"/_search?allow_partial_search_results=false", body, &out)
	var status *httpStatusError
	if c.config.Kind == "elasticsearch" && errors.As(err, &status) && status.Status == 400 {
		// Older Elasticsearch includes vectors in _source by default and does
		// not recognize exclude_vectors. Retry this read with the older shape.
		body["_source"] = true
		err = c.remote.call(ctx, "POST", "/"+url.PathEscape(c.config.Index)+"/_search?allow_partial_search_results=false", body, &out)
	}
	if err != nil {
		return Page{}, err
	}
	if out.TimedOut || out.Shards.Failed > 0 || out.Hits == nil {
		return Page{}, errors.New("search returned incomplete results")
	}
	page := Page{Done: len(out.Hits.Hits) < limit}
	last := cursor
	for _, hit := range out.Hits.Hits {
		if len(hit.Sort) != 1 {
			return Page{}, errors.New("source requires a unique sortable ID field")
		}
		next := encodeCursor(hit.Sort)
		if next == last {
			return Page{}, errors.New("source sort field is not unique")
		}
		last = next
		r, err := documentRecord(hit.Source, c.config)
		if err != nil {
			return Page{}, err
		}
		if r.ID != hit.ID {
			return Page{}, errors.New("source ID field must equal the document _id")
		}
		page.Records = append(page.Records, r)
		page.Next = next
	}
	return page, nil
}
func (c *RESTDatabase) writeSearch(ctx context.Context, rs []Record) error {
	var payload bytes.Buffer
	for _, r := range rs {
		doc, err := recordDocument(r, c.config, r.ID)
		if err != nil {
			return err
		}
		if err := json.NewEncoder(&payload).Encode(map[string]any{"index": map[string]string{"_id": r.ID}}); err != nil {
			return err
		}
		if err := json.NewEncoder(&payload).Encode(doc); err != nil {
			return err
		}
	}
	var out struct {
		Errors *bool `json:"errors"`
		Items  []map[string]struct {
			ID     string `json:"_id"`
			Status int    `json:"status"`
		} `json:"items"`
	}
	if err := c.remote.callBytes(ctx, "POST", "/"+url.PathEscape(c.config.Index)+"/_bulk", payload.Bytes(), "application/x-ndjson", &out); err != nil {
		return err
	}
	if out.Errors == nil || *out.Errors || len(out.Items) != len(rs) {
		return errors.New("search bulk write did not acknowledge every record")
	}
	for i, item := range out.Items {
		ack, ok := item["index"]
		if !ok || ack.ID != rs[i].ID || ack.Status < 200 || ack.Status >= 300 {
			return errors.New("search bulk write contained a failed item")
		}
	}
	return nil
}
func (c *RESTDatabase) readSolr(ctx context.Context, cursor string, limit int) (Page, error) {
	if cursor == "" {
		cursor = "*"
	}
	q := url.Values{"q": {"*:*"}, "sort": {c.config.IDField + " asc"}, "rows": {strconv.Itoa(limit)}, "cursorMark": {cursor}, "fl": {"*"}, "wt": {"json"}, "omitHeader": {"false"}}
	var out struct {
		Header struct {
			Status  *int            `json:"status"`
			Partial json.RawMessage `json:"partialResults"`
		} `json:"responseHeader"`
		Response struct {
			Docs []map[string]json.RawMessage `json:"docs"`
		} `json:"response"`
		Next string `json:"nextCursorMark"`
	}
	if err := c.remote.call(ctx, "GET", "/solr/"+url.PathEscape(c.config.Collection)+"/select?"+q.Encode(), nil, &out); err != nil {
		return Page{}, err
	}
	if out.Header.Status == nil || *out.Header.Status != 0 || out.Next == "" || (len(out.Header.Partial) > 0 && string(out.Header.Partial) != "false") {
		return Page{}, errors.New("Solr returned incomplete results")
	}
	page := Page{Done: out.Next == cursor, Next: out.Next}
	for _, doc := range out.Response.Docs {
		delete(doc, "_version_")
		r, err := documentRecord(doc, c.config)
		if err != nil {
			return Page{}, err
		}
		page.Records = append(page.Records, r)
	}
	return page, nil
}
func (c *RESTDatabase) writeSolr(ctx context.Context, rs []Record) error {
	docs := []any{}
	for _, r := range rs {
		if _, ok := r.Metadata["_version_"]; ok {
			return errors.New("Solr _version_ is reserved")
		}
		doc, err := recordDocument(r, c.config, r.ID)
		if err != nil {
			return err
		}
		docs = append(docs, doc)
	}
	var out struct {
		Header struct {
			Status *int `json:"status"`
		} `json:"responseHeader"`
	}
	if err := c.remote.call(ctx, "POST", "/solr/"+url.PathEscape(c.config.Collection)+"/update?overwrite=true&commit=true&wt=json", docs, &out); err != nil {
		return err
	}
	if out.Header.Status == nil || *out.Header.Status != 0 {
		return errors.New("Solr write was not acknowledged")
	}
	return nil
}
