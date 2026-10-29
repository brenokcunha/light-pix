package psp_test

import (
	"encoding/json"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/brenokcunha/light-pix/internal/psp"
	"github.com/brenokcunha/light-pix/internal/sim/dict"
)

// getList faz um GET que devolve um array JSON.
func getList[T any](t *testing.T, n *node, path string) []T {
	t.Helper()
	resp, err := http.Get(n.apiURL + path)
	must(t, err)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: %d", path, resp.StatusCode)
	}
	var out []T
	must(t, json.NewDecoder(resp.Body).Decode(&out))
	return out
}

// Consultas da interface de testes: contas, extrato (enviado e recebido),
// chaves da conta e busca no diretório.
func TestLabQueries(t *testing.T) {
	w := newWorld(t, worldOpts{})
	a, b := w.nodes[ispbA], w.nodes[ispbB]

	t.Run("contas do PSP com saldo", func(t *testing.T) {
		resp := a.call(http.MethodGet, "/v1/accounts", "", nil)
		if resp.status != http.StatusOK || resp.str("ispb") != ispbA {
			t.Fatalf("contas: %d %v", resp.status, resp.body)
		}
		accs, _ := resp.body["accounts"].([]any)
		if len(accs) != 2 {
			t.Fatalf("esperava 2 contas no PSP A, veio %d", len(accs))
		}
		first, _ := accs[0].(map[string]any)
		if first["number"] != alice {
			t.Errorf("contas fora de ordem: %v", first["number"])
		}
	})

	t.Run("chaves cadastradas na conta", func(t *testing.T) {
		keys := getList[dict.Entry](t, a, "/v1/accounts/0001/"+alice+"/keys")
		got := []string{}
		for _, k := range keys {
			if k.Account.AccountNumber != alice || k.Account.Participant != ispbA {
				t.Errorf("chave de outra conta na lista: %+v", k)
			}
			got = append(got, k.Key)
		}
		if len(got) != 4 || !slices.Contains(got, "alice@example.com") {
			t.Errorf("chaves da Alice: %v", got)
		}
		if resp := a.call(http.MethodGet, "/v1/accounts/0001/9999999/keys", "", nil); resp.status != http.StatusNotFound {
			t.Errorf("conta inexistente: %d", resp.status)
		}
	})

	t.Run("busca no diretório por trecho da chave ou do nome", func(t *testing.T) {
		byName := getList[dict.Entry](t, a, "/v1/directory/keys?q=bob%20bar")
		if len(byName) != 4 {
			t.Errorf("busca por nome: %d chaves, esperava 4 (Bob)", len(byName))
		}
		byKey := getList[dict.Entry](t, a, "/v1/directory/keys?q=DAVE@")
		if len(byKey) != 1 || byKey[0].Key != "dave@example.com" {
			t.Errorf("busca por chave sem diferenciar maiúsculas: %+v", byKey)
		}
		// O texto é literal: "." e "+" não viram expressão regular.
		if lit := getList[dict.Entry](t, a, "/v1/directory/keys?q=%2B5521"); len(lit) != 2 {
			t.Errorf("busca literal por +5521: %d, esperava 2", len(lit))
		}
		if all := getList[dict.Entry](t, a, "/v1/directory/keys"); len(all) != 16 {
			t.Errorf("sem filtro: %d, esperava as 16 chaves do seed", len(all))
		}
	})

	t.Run("extrato mostra Pix enviado e recebido, do mais novo ao mais velho", func(t *testing.T) {
		if items := getList[psp.ActivityItem](t, b, "/v1/accounts/0001/"+bob+"/payments"); len(items) != 0 {
			t.Fatalf("extrato deveria começar vazio: %d", len(items))
		}
		first := creditedPix(t, w, "10.00", "lab-1")
		time.Sleep(5 * time.Millisecond)
		second := creditedPix(t, w, "25.50", "lab-2")

		recv := getList[psp.ActivityItem](t, b, "/v1/accounts/0001/"+bob+"/payments")
		if len(recv) != 2 || recv[0].EndToEndID != second || recv[1].EndToEndID != first {
			t.Fatalf("extrato do Bob: %+v", recv)
		}
		if recv[0].Kind != "PIX" || recv[0].State != psp.StateCredited || recv[0].Creditor.Number != bob {
			t.Errorf("item recebido: %+v", recv[0])
		}

		sent := getList[psp.ActivityItem](t, a, "/v1/accounts/0001/"+alice+"/payments")
		if len(sent) != 2 || sent[0].State != psp.StateConfirmed {
			t.Errorf("extrato da Alice: %+v", sent)
		}
		if other := getList[psp.ActivityItem](t, a, "/v1/accounts/0001/1000002/payments"); len(other) != 0 {
			t.Errorf("a conta da Carol não participou de nenhum Pix: %d", len(other))
		}
		if one := getList[psp.ActivityItem](t, b, "/v1/accounts/0001/"+bob+"/payments?limit=1"); len(one) != 1 || one[0].EndToEndID != second {
			t.Errorf("limit=1: %+v", one)
		}

		// Devolução entra no extrato como RETURN, nos dois lados.
		r := b.ret(second, "lab-ret", "5.00", "")
		if r.status != http.StatusCreated {
			t.Fatalf("devolução: %d %v", r.status, r.body)
		}
		b.waitReturn(r.str("endToEndId"), psp.StateConfirmed)
		after := getList[psp.ActivityItem](t, b, "/v1/accounts/0001/"+bob+"/payments")
		if len(after) != 3 || after[0].Kind != "RETURN" {
			t.Errorf("extrato após devolução: %+v", after)
		}
	})
}
