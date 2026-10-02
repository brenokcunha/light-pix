package testutil

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"
)

// EnvRabbitURL aponta para o RabbitMQ dos testes de integração, sem vhost,
// ex.: amqp://lightpix:lightpix@localhost:5672 com a stack no ar. O painel de
// gerenciamento precisa estar em localhost:15672 (ou em EnvRabbitMgmtURL).
const (
	EnvRabbitURL     = "LIGHTPIX_TEST_RABBITMQ_URL"
	EnvRabbitMgmtURL = "LIGHTPIX_TEST_RABBITMQ_MGMT_URL"
)

// RabbitMQ cria um vhost exclusivo (apagado ao fim do teste) e devolve a URL
// AMQP que aponta para ele. Sem a variável de ambiente, o teste é pulado.
func RabbitMQ(t testing.TB) string {
	t.Helper()
	raw := os.Getenv(EnvRabbitURL)
	if raw == "" {
		t.Skip(EnvRabbitURL + " não definida; teste de integração pulado")
	}
	u, err := url.Parse(raw)
	if err != nil || u.User == nil {
		t.Fatalf("%s inválida: %q", EnvRabbitURL, raw)
	}
	mgmt := os.Getenv(EnvRabbitMgmtURL)
	if mgmt == "" {
		mgmt = "http://" + u.Hostname() + ":15672"
	}
	user := u.User.Username()
	pass, _ := u.User.Password()

	var b [6]byte
	_, _ = rand.Read(b[:])
	vhost := "test_" + hex.EncodeToString(b[:])
	call := func(method, path, body string) {
		t.Helper()
		req, _ := http.NewRequest(method, mgmt+path, bytes.NewBufferString(body))
		req.SetBasicAuth(user, pass)
		req.Header.Set("Content-Type", "application/json")
		client := &http.Client{Timeout: 10 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("API de gerenciamento do RabbitMQ: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			t.Fatalf("%s %s: status %d", method, path, resp.StatusCode)
		}
	}
	call(http.MethodPut, "/api/vhosts/"+vhost, `{"default_queue_type":"quorum"}`)
	call(http.MethodPut, "/api/permissions/"+vhost+"/"+user, `{"configure":".*","write":".*","read":".*"}`)
	t.Cleanup(func() {
		req, _ := http.NewRequest(http.MethodDelete, mgmt+"/api/vhosts/"+vhost, nil)
		req.SetBasicAuth(user, pass)
		if resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req); err == nil {
			resp.Body.Close()
		}
	})
	return fmt.Sprintf("%s://%s@%s/%s", u.Scheme, u.User.String(), u.Host, vhost)
}
