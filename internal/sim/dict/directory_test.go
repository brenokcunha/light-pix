package dict_test

import (
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/brenokcunha/light-pix/internal/pix"
	"github.com/brenokcunha/light-pix/internal/sim/dict"
)

// A listagem de /admin/entries é só do laboratório: sem cabeçalhos de
// participante, com filtros e limite.
func TestAdminList(t *testing.T) {
	e := setup(t, defaultCfg,
		entry(pix.KeyEmail, "alice@example.com", ispbA, "1000001", cpfAlice),
		entry(pix.KeyCPF, cpfAlice, ispbA, "1000001", cpfAlice),
		entry(pix.KeyEmail, "bob@example.com", ispbB, "2000001", cpfBob),
		entry(pix.KeyPhone, "+5521990000001", ispbB, "2000001", cpfBob),
	)
	list := func(query string, want int) []dict.Entry {
		t.Helper()
		resp, err := http.Get(e.url + "/admin/entries?" + query)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("?%s: %d", query, resp.StatusCode)
		}
		var out []dict.Entry
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}
		if len(out) != want {
			t.Fatalf("?%s: %d vínculos, esperava %d (%+v)", query, len(out), want, out)
		}
		return out
	}

	all := list("", 4)
	if all[0].Owner.Name != "Titular 1000001" || all[3].Owner.Name != "Titular 2000001" {
		t.Errorf("ordem por titular: %q ... %q", all[0].Owner.Name, all[3].Owner.Name)
	}
	list("participant="+ispbB, 2)
	list("participant="+ispbA+"&branch=0001&account=1000001", 2)
	list("participant="+ispbA+"&account=2000001", 0)
	list("q="+url.QueryEscape("BOB@"), 1)
	list("q=Titular+2000001", 2)
	list("q="+url.QueryEscape("+5521"), 1) // literal, não expressão regular
	list("q="+url.QueryEscape(".*"), 0)
	list("limit=3", 3)
}
