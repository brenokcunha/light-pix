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

// newUntracedHTTPClient não cria spans: serve ao long-polling, que de outro
// modo geraria um trace lento a cada espera sem mensagens.
func newUntracedHTTPClient(base string) httpClient {
	return httpClient{base: base, hc: &http.Client{}}
}

// do envia in como JSON (se não for nil) e decodifica a resposta 2xx em out
// (se não for nil). Devolve o status para quem precisa distinguir 200 de 204.
func (c httpClient) do(ctx context.Context, method, path string, header http.Header, in, out any) (int, error) {
	status, _, err := c.doHeader(ctx, method, path, header, in, out)
	return status, err
}

// doHeader é do, devolvendo também os headers da resposta.
func (c httpClient) doHeader(ctx context.Context, method, path string, header http.Header, in, out any) (int, http.Header, error) {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return 0, nil, err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, body)
	if err != nil {
		return 0, nil, err
	}
	for k, v := range header {
		req.Header[k] = v
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		apiErr := &APIError{Status: resp.StatusCode}
		_ = json.NewDecoder(io.LimitReader(resp.Body, 1<<16)).Decode(apiErr)
		return resp.StatusCode, resp.Header, apiErr
	}
	if out != nil && resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusAccepted {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return resp.StatusCode, resp.Header, fmt.Errorf("decodificar resposta: %w", err)
		}
	}
	return resp.StatusCode, resp.Header, nil
}
