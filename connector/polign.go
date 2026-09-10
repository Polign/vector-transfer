package connector

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
)

type Polign struct {
	remote     *remote
	collection string
}

func NewPolign(base, collection, key string) *Polign {
	h := http.Header{}
	if key != "" {
		h.Set("Authorization", "Bearer "+key)
	}
	return &Polign{newRemote(base, h), url.PathEscape(collection)}
}

func (c *Polign) Read(ctx context.Context, cursor string, limit int) (Page, error) {
	offset := 0
	if cursor != "" {
		var err error
		offset, err = strconv.Atoi(cursor)
		if err != nil || offset < 0 {
			return Page{}, errors.New("invalid Polign cursor")
		}
	}
	var result struct {
		Vectors []Record `json:"vectors"`
		Total   *int     `json:"total"`
	}
	path := fmt.Sprintf("/v1/collections/%s/vectors?typed=true&limit=%d&offset=%d", c.collection, limit, offset)
	if err := c.remote.call(ctx, "GET", path, nil, &result); err != nil {
		return Page{}, err
	}
	if result.Total == nil || *result.Total < offset+len(result.Vectors) {
		return Page{}, errors.New("inconsistent Polign listing response; keep source stable")
	}
	if len(result.Vectors) == 0 && offset < *result.Total {
		return Page{}, errors.New("Polign listing made no progress")
	}
	next := offset + len(result.Vectors)
	return Page{Records: result.Vectors, Next: strconv.Itoa(next), Done: next >= *result.Total}, nil
}

func (c *Polign) Upsert(ctx context.Context, records []Record) error {
	return chunks(records, 500, 8<<20, func(rs []Record) any { return map[string]any{"vectors": rs} }, func(body any) error {
		var result struct {
			IDs []string `json:"ids"`
		}
		if err := c.remote.call(ctx, "POST", "/v1/collections/"+c.collection+"/vectors:batch", body, &result); err != nil {
			return err
		}
		rs := body.(map[string]any)["vectors"].([]Record)
		if len(result.IDs) != len(rs) {
			return errors.New("Polign did not acknowledge every record")
		}
		for i, r := range rs {
			if result.IDs[i] != r.ID {
				return errors.New("Polign acknowledged an unexpected record ID")
			}
		}
		return nil
	})
}
