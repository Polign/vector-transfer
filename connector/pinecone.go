package connector

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
)

type Pinecone struct {
	remote    *remote
	namespace string
}

func NewPinecone(base, namespace, key string) *Pinecone {
	h := http.Header{}
	h.Set("Api-Key", key)
	h.Set("X-Pinecone-Api-Version", "2025-10")
	return &Pinecone{newRemote(base, h), namespace}
}

func (c *Pinecone) Read(ctx context.Context, cursor string, limit int) (Page, error) {
	q := url.Values{"namespace": {c.namespace}, "limit": {strconv.Itoa(limit)}}
	if cursor != "" {
		q.Set("paginationToken", cursor)
	}
	var listing struct {
		Vectors []struct {
			ID string `json:"id"`
		} `json:"vectors"`
		Pagination struct {
			Next string `json:"next"`
		} `json:"pagination"`
	}
	if err := c.remote.call(ctx, "GET", "/vectors/list?"+q.Encode(), nil, &listing); err != nil {
		return Page{}, err
	}
	if listing.Vectors == nil {
		return Page{}, errors.New("Pinecone listing response is missing vectors")
	}
	page := Page{Next: listing.Pagination.Next, Done: listing.Pagination.Next == ""}
	// Keep fetch URLs bounded even for long IDs.
	for start := 0; start < len(listing.Vectors); {
		q := url.Values{"namespace": {c.namespace}}
		end := start
		for end < len(listing.Vectors) && end-start < 100 {
			q.Add("ids", listing.Vectors[end].ID)
			end++
			if len(q.Encode()) > 6000 {
				break
			}
		}
		var fetched struct {
			Vectors map[string]struct {
				Record
				Sparse json.RawMessage `json:"sparseValues"`
			} `json:"vectors"`
		}
		if err := c.remote.call(ctx, "GET", "/vectors/fetch?"+q.Encode(), nil, &fetched); err != nil {
			return Page{}, err
		}
		for _, v := range listing.Vectors[start:end] {
			r, ok := fetched.Vectors[v.ID]
			if !ok || r.ID != v.ID {
				return Page{}, errors.New("Pinecone listed a record that could not be fetched; keep source stable")
			}
			if len(r.Sparse) > 0 && string(r.Sparse) != "null" {
				return Page{}, errors.New("sparse vectors are not supported by dense transfers")
			}
			page.Records = append(page.Records, r.Record)
		}
		start = end
	}
	return page, nil
}

func (c *Pinecone) Upsert(ctx context.Context, records []Record) error {
	return chunks(records, 1000, 1900000, func(rs []Record) any { return map[string]any{"vectors": rs, "namespace": c.namespace} }, func(body any) error {
		var result struct {
			Count *int `json:"upsertedCount"`
		}
		if err := c.remote.call(ctx, "POST", "/vectors/upsert", body, &result); err != nil {
			return err
		}
		n := len(body.(map[string]any)["vectors"].([]Record))
		if result.Count == nil || *result.Count != n {
			return errors.New("Pinecone did not acknowledge every record")
		}
		return nil
	})
}
