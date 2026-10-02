// Package clients tem os clientes HTTP dos simuladores (DICT, SPI, Bacen).
// Todos propagam o contexto de trace e devolvem *APIError para respostas
// fora de 2xx, com o código de erro da API.
package clients

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// APIError é uma resposta de erro da API chamada.
type APIError struct {
	Status  int
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *APIError) Error() string {
	return fmt.Sprintf("status %d: %s: %s", e.Status, e.Code, e.Message)
}

// ErrorCode devolve o código de erro da API (ex.: AM04).
func (e *APIError) ErrorCode() string { return e.Code }

// Temporary indica erro do servidor (5xx ou 429), em que repetir faz sentido.
func (e *APIError) Temporary() bool {
	return e.Status >= 500 || e.Status == http.StatusTooManyRequests
}

type httpClient struct {
	base string
	hc   *http.Client
}

func newHTTPClient(base string, timeout time.Duration) httpClient {
	return httpClient{base: base, hc: &http.Client{
		Timeout:   timeout,
		Transport: otelhttp.NewTransport(http.DefaultTransport),
	}}
}

// do envia in como JSON (se não for nil) e decodifica a resposta 2xx em out
// (se não for nil). Devolve o status para quem precisa distinguir 200 de 204.
func (c httpClient) do(ctx context.Context, method, path string, header http.Header, in, out any) (int, error) {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return 0, err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, body)
	if err != nil {
		return 0, err
	}
	for k, v := range header {
		req.Header[k] = v
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		apiErr := &APIError{Status: resp.StatusCode}
		_ = json.NewDecoder(io.LimitReader(resp.Body, 1<<16)).Decode(apiErr)
		return resp.StatusCode, apiErr
	}
	if out != nil && resp.StatusCode != http.StatusNoContent {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return resp.StatusCode, fmt.Errorf("decodificar resposta: %w", err)
		}
	}
	return resp.StatusCode, nil
}
