// Package ingest is the agent-side client that POSTs signed observation batches
// to the tracker API. It retries, and on persistent failure spools the batch to
// disk so nothing is lost while the API/network is down; spooled batches are
// resent on the next flush. Server-side dedup (dedup_key) makes resends safe.
package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/signing"
)

type Client struct {
	baseURL  string
	agent    string
	secret   string
	spoolDir string
	hc       *http.Client
	retries  int
}

func New(baseURL, agent, secret, spoolDir string) (*Client, error) {
	if spoolDir != "" {
		if err := os.MkdirAll(spoolDir, 0o755); err != nil {
			return nil, err
		}
	}
	return &Client{
		baseURL: baseURL, agent: agent, secret: secret, spoolDir: spoolDir,
		hc: &http.Client{Timeout: 30 * time.Second}, retries: 3,
	}, nil
}

type envelope struct {
	Agent string              `json:"agent"`
	Batch []model.Observation `json:"batch"`
}

type ingestResp struct {
	Accepted int `json:"accepted"`
	Rejected int `json:"rejected"`
}

// Submit sends a batch. On persistent failure it spools the batch to disk and
// returns nil (the agent keeps going); it returns an error only if spooling also
// fails (or there is no spool dir configured).
func (c *Client) Submit(ctx context.Context, batch []model.Observation) error {
	if len(batch) == 0 {
		return nil
	}
	body, err := json.Marshal(envelope{Agent: c.agent, Batch: batch})
	if err != nil {
		return err
	}
	if _, sendErr := c.post(ctx, body); sendErr != nil {
		return c.spool(body, sendErr)
	}
	return nil
}

// FlushSpool tries to resend every spooled batch; successful ones are deleted.
func (c *Client) FlushSpool(ctx context.Context) (resent int, err error) {
	if c.spoolDir == "" {
		return 0, nil
	}
	entries, err := os.ReadDir(c.spoolDir)
	if err != nil {
		return 0, err
	}
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}
		p := filepath.Join(c.spoolDir, e.Name())
		body, rerr := os.ReadFile(p)
		if rerr != nil {
			continue
		}
		if _, perr := c.post(ctx, body); perr != nil {
			return resent, perr // stop on first failure; try again next cycle
		}
		_ = os.Remove(p)
		resent++
	}
	return resent, nil
}

func (c *Client) post(ctx context.Context, body []byte) (ingestResp, error) {
	var last error
	for attempt := 0; attempt <= c.retries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ingestResp{}, ctx.Err()
			case <-time.After(time.Duration(attempt) * time.Second):
			}
		}
		resp, err := c.doPost(ctx, body)
		if err == nil {
			return resp, nil
		}
		last = err
	}
	return ingestResp{}, last
}

func (c *Client) doPost(ctx context.Context, body []byte) (ingestResp, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+"/api/v1/ingest/observations", bytes.NewReader(body))
	if err != nil {
		return ingestResp{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Agent-Signature", signing.Sign(c.secret, body))
	resp, err := c.hc.Do(req)
	if err != nil {
		return ingestResp{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return ingestResp{}, fmt.Errorf("ingest HTTP %d", resp.StatusCode)
	}
	var r ingestResp
	_ = json.NewDecoder(resp.Body).Decode(&r)
	return r, nil
}

func (c *Client) spool(body []byte, cause error) error {
	if c.spoolDir == "" {
		return fmt.Errorf("envio falhou e não há spool dir: %w", cause)
	}
	name := fmt.Sprintf("batch-%d-%d.json", time.Now().UnixNano(), len(body))
	if err := os.WriteFile(filepath.Join(c.spoolDir, name), body, 0o644); err != nil {
		return fmt.Errorf("spool falhou: %w (causa: %v)", err, cause)
	}
	return nil
}
