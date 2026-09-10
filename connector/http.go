package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

const maxResponseBytes = 64 << 20

type requestSigner func(context.Context, *http.Request, []byte) error
type httpStatusError struct{ Status int }

func (e *httpStatusError) Error() string {
	return fmt.Sprintf("connector HTTP %d (%s)", e.Status, http.StatusText(e.Status))
}

type remote struct {
	base    string
	headers http.Header
	client  *http.Client
	sign    requestSigner
}

func newRemote(base string, headers http.Header) *remote {
	return &remote{base: base, headers: headers, client: &http.Client{
		Timeout: 60 * time.Second,
		// Never forward provider credentials to a redirect target.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
}

func (c *remote) call(ctx context.Context, method, path string, body, out any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var payload []byte
	var err error
	if body != nil {
		payload, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}
	return c.callBytes(ctx, method, path, payload, "application/json", out)
}

func (c *remote) callBytes(ctx context.Context, method, path string, payload []byte, contentType string, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, bytes.NewReader(payload))
	if err != nil {
		return errors.New("invalid connector request URL")
	}
	req.Header = c.headers.Clone()
	if req.Header == nil {
		req.Header = make(http.Header)
	}
	req.Header.Set("Content-Type", contentType)
	if c.sign != nil {
		if err := c.sign(ctx, req, payload); err != nil {
			return err
		}
	}
	resp, err := c.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// URL/network errors and provider bodies may contain secrets or data.
		return &Transient{errors.New("connector network request failed")}
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		err := &httpStatusError{Status: resp.StatusCode}
		if resp.StatusCode == 408 || resp.StatusCode == 429 || resp.StatusCode >= 500 {
			return &Transient{err}
		}
		return err
	}
	if out == nil {
		_, err = io.Copy(io.Discard, io.LimitReader(resp.Body, maxResponseBytes))
		return err
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return &Transient{errors.New("connector response read failed")}
	}
	if len(data) > maxResponseBytes {
		return errors.New("connector response exceeds 64 MiB; reduce batch size")
	}
	if err := json.Unmarshal(data, out); err != nil {
		return errors.New("connector returned an invalid response")
	}
	return nil
}

// Send in bounded chunks; replay of an earlier chunk is safe after a later
// chunk fails. The engine checkpoints only after every chunk succeeds.
func chunks(records []Record, maxRecords, maxBytes int, body func([]Record) any, send func(any) error) error {
	for len(records) > 0 {
		n := min(len(records), maxRecords)
		for {
			payload := body(records[:n])
			b, err := json.Marshal(payload)
			if err != nil {
				return err
			}
			if len(b) <= maxBytes {
				if err := send(payload); err != nil {
					return err
				}
				break
			}
			if n == 1 {
				return errors.New("a record exceeds the destination request size limit")
			}
			n = max(1, n/2)
		}
		records = records[n:]
	}
	return nil
}
