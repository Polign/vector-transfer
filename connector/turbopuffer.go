package connector

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type Turbopuffer struct {
	remote                    *remote
	namespace, metric, idType string
}

func NewTurbopuffer(base, namespace, key, metric, idType string) *Turbopuffer {
	h := http.Header{}
	h.Set("Authorization", "Bearer "+key)
	return &Turbopuffer{newRemote(base, h), url.PathEscape(namespace), metric, idType}
}

func (c *Turbopuffer) Read(ctx context.Context, cursor string, limit int) (Page, error) {
	body := map[string]any{"rank_by": []string{"id", "asc"}, "limit": limit, "include_attributes": true, "vector_encoding": "float"}
	if cursor != "" {
		if !json.Valid([]byte(cursor)) {
			return Page{}, errors.New("invalid Turbopuffer cursor")
		}
		body["filters"] = []any{"id", "Gt", json.RawMessage(cursor)}
	}
	var result struct {
		Rows []map[string]json.RawMessage `json:"rows"`
	}
	if err := c.remote.call(ctx, "POST", "/v2/namespaces/"+c.namespace+"/query", body, &result); err != nil {
		return Page{}, err
	}
	if result.Rows == nil {
		return Page{}, errors.New("Turbopuffer query response is missing rows")
	}
	page := Page{Done: len(result.Rows) < limit}
	for _, row := range result.Rows {
		r := Record{}
		id := row["id"]
		if err := json.Unmarshal(id, &r.ID); err != nil {
			n, err := strconv.ParseUint(string(id), 10, 64)
			if err != nil {
				return Page{}, errors.New("unsupported Turbopuffer ID")
			}
			r.ID = strconv.FormatUint(n, 10)
		}
		if err := json.Unmarshal(row["vector"], &r.Values); err != nil || len(r.Values) == 0 {
			return Page{}, errors.New("Turbopuffer record has no dense vector attribute named vector")
		}
		page.Next = string(id) // Retain numeric cursor type without float conversion.
		delete(row, "id")
		delete(row, "vector")
		delete(row, "$dist")
		r.Metadata = row
		page.Records = append(page.Records, r)
	}
	return page, nil
}

func (c *Turbopuffer) Upsert(ctx context.Context, records []Record) error {
	rows := make([]map[string]any, 0, len(records))
	for _, r := range records {
		row := make(map[string]any, len(r.Metadata)+2)
		for k, v := range r.Metadata {
			if k == "id" || k == "vector" || strings.HasPrefix(k, "$") {
				return errors.New("metadata contains a reserved Turbopuffer attribute name")
			}
			row[k] = v
		}
		if c.idType == "uint" {
			n, err := strconv.ParseUint(r.ID, 10, 64)
			if err != nil || strconv.FormatUint(n, 10) != r.ID {
				return errors.New("Turbopuffer uint destination requires canonical unsigned integer IDs")
			}
			row["id"] = n
		} else {
			if len(r.ID) > 64 {
				return errors.New("Turbopuffer string IDs must not exceed 64 bytes")
			}
			row["id"] = r.ID
		}
		row["vector"] = r.Values
		rows = append(rows, row)
	}
	if len(rows) == 0 {
		return nil
	}
	body := map[string]any{"upsert_rows": rows, "distance_metric": c.metric, "schema": map[string]any{"id": c.idType}}
	var result struct {
		Count *int `json:"rows_upserted"`
	}
	if err := c.remote.call(ctx, "POST", "/v2/namespaces/"+c.namespace, body, &result); err != nil {
		return err
	}
	if result.Count == nil || *result.Count != len(rows) {
		return errors.New("Turbopuffer did not acknowledge every record")
	}
	return nil
}
