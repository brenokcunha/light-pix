package dict_test

import (
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/brenokcunha/light-pix/internal/pix"
	"github.com/brenokcunha/light-pix/internal/sim/dict"
)

type report struct {
	ID    string `json:"id"`
	State string `json:"state"`
	Over  bool   `json:"overdue"`
	Code  string `json:"code"`
}

func (e *env) report(method, path, participant string, body any) (int, report) {
	e.t.Helper()
	resp, raw := e.do(method, path, participant, "", body)
	var r report
	_ = json.Unmarshal(raw, &r)
	return resp.StatusCode, r
}

func newReport(e2e string, amount json.Number) map[string]any {
	return map[string]any{"endToEndId": e2e, "reason": "FRAUD", "details": "golpe", "counterpartyIspb": ispbB, "amount": amount}
}

func TestInfractionReportLifecycle(t *testing.T) {
	e := setup(t, dict.Config{BucketCapacity: 100, RefillPerMinute: 60, NotFoundCost: 1, AnalysisDeadline: time.Hour})
	e2e := pix.NewEndToEndID(ispbA, time.Now())

	code, rep := e.report(http.MethodPost, "/api/v1/infraction-reports", ispbA, newReport(e2e, json.Number("80.50")))
	if code != http.StatusCreated || rep.State != dict.InfractionOpen || rep.ID == "" || rep.Over {
		t.Fatalf("criação: %d %+v", code, rep)
	}
	// Um relato ativo por Pix.
	if code, r := e.report(http.MethodPost, "/api/v1/infraction-reports", ispbA, newReport(e2e, json.Number("80.50"))); code != http.StatusConflict || r.Code != "INFRACTION_ALREADY_REPORTED" {
		t.Fatalf("segundo relato do mesmo Pix: %d %+v", code, r)
	}

	// O recebedor vê o relato na sua fila; o pagador não o vê como "a analisar".
	var list []report
	_, raw := e.do(http.MethodGet, "/api/v1/infraction-reports?state=OPEN", ispbB, "", nil)
	must(t, json.Unmarshal(raw, &list))
	if len(list) != 1 || list[0].ID != rep.ID {
		t.Fatalf("fila do recebedor: %+v", list)
	}
	_, raw = e.do(http.MethodGet, "/api/v1/infraction-reports?state=OPEN", ispbA, "", nil)
	must(t, json.Unmarshal(raw, &list))
	if len(list) != 0 {
		t.Fatalf("o pagador não analisa o próprio relato: %+v", list)
	}
	// Quem não participa não vê o relato.
	if code, _ := e.report(http.MethodGet, "/api/v1/infraction-reports/"+rep.ID, "33333333", nil); code != http.StatusNotFound {
		t.Fatalf("terceiro: %d", code)
	}

	// Só o recebedor analisa; o pagador não decide por ele.
	analysis := map[string]any{"result": "AGREED", "details": "confirmado", "returnId": pix.NewReturnID(ispbB, time.Now()), "returnedAmount": json.Number("80.50")}
	if code, _ := e.report(http.MethodPost, "/api/v1/infraction-reports/"+rep.ID+"/analysis", ispbA, analysis); code != http.StatusForbidden {
		t.Fatalf("pagador analisando: %d", code)
	}
	if code, r := e.report(http.MethodPost, "/api/v1/infraction-reports/"+rep.ID+"/analysis", ispbB, analysis); code != http.StatusOK || r.State != dict.InfractionAgreed {
		t.Fatalf("análise: %d %+v", code, r)
	}
	// Uma decisão só, e depois dela não se cancela.
	if code, r := e.report(http.MethodPost, "/api/v1/infraction-reports/"+rep.ID+"/analysis", ispbB, analysis); code != http.StatusConflict || r.Code != "INFRACTION_NOT_OPEN" {
		t.Fatalf("segunda análise: %d %+v", code, r)
	}
	if code, r := e.report(http.MethodPost, "/api/v1/infraction-reports/"+rep.ID+"/cancel", ispbA, nil); code != http.StatusConflict {
		t.Fatalf("cancelar depois de decidido: %d %+v", code, r)
	}
}

func TestInfractionCancelAllowsANewReport(t *testing.T) {
	e := setup(t, defaultCfg)
	e2e := pix.NewEndToEndID(ispbA, time.Now())
	_, rep := e.report(http.MethodPost, "/api/v1/infraction-reports", ispbA, newReport(e2e, json.Number("10.00")))

	if code, _ := e.report(http.MethodPost, "/api/v1/infraction-reports/"+rep.ID+"/cancel", ispbB, nil); code != http.StatusForbidden {
		t.Fatalf("recebedor cancelando: %d", code)
	}
	if code, r := e.report(http.MethodPost, "/api/v1/infraction-reports/"+rep.ID+"/cancel", ispbA, nil); code != http.StatusOK || r.State != dict.InfractionCancelled {
		t.Fatalf("cancelamento: %d %+v", code, r)
	}
	if code, _ := e.report(http.MethodPost, "/api/v1/infraction-reports", ispbA, newReport(e2e, json.Number("10.00"))); code != http.StatusCreated {
		t.Fatalf("novo relato depois do cancelamento: %d", code)
	}
}

func TestInfractionValidation(t *testing.T) {
	e := setup(t, defaultCfg)
	e2e := pix.NewEndToEndID(ispbA, time.Now())
	bad := func(mutate func(map[string]any)) map[string]any {
		m := newReport(e2e, json.Number("10.00"))
		mutate(m)
		return m
	}
	cases := []struct {
		name        string
		participant string
		body        map[string]any
		status      int
	}{
		{"só o PSP do pagador relata", ispbB, newReport(e2e, json.Number("10.00")), http.StatusForbidden},
		{"EndToEndId inválido", ispbA, bad(func(m map[string]any) { m["endToEndId"] = "E123" }), http.StatusBadRequest},
		{"recebedor igual ao pagador", ispbA, bad(func(m map[string]any) { m["counterpartyIspb"] = ispbA }), http.StatusBadRequest},
		{"valor zero", ispbA, bad(func(m map[string]any) { m["amount"] = json.Number("0.00") }), http.StatusBadRequest},
		{"motivo desconhecido", ispbA, bad(func(m map[string]any) { m["reason"] = "OUTRO" }), http.StatusBadRequest},
	}
	for _, c := range cases {
		if code, _ := e.report(http.MethodPost, "/api/v1/infraction-reports", c.participant, c.body); code != c.status {
			t.Errorf("%s: %d, quer %d", c.name, code, c.status)
		}
	}
	_, rep := e.report(http.MethodPost, "/api/v1/infraction-reports", ispbA, newReport(e2e, json.Number("10.00")))
	if code, _ := e.report(http.MethodPost, "/api/v1/infraction-reports/"+rep.ID+"/analysis", ispbB, map[string]any{"result": "TALVEZ"}); code != http.StatusBadRequest {
		t.Errorf("resultado inválido: %d", code)
	}
	if code, _ := e.report(http.MethodPost, "/api/v1/infraction-reports/"+rep.ID+"/analysis", ispbB,
		map[string]any{"result": "DISAPPROVED", "returnId": pix.NewReturnID(ispbB, time.Now())}); code != http.StatusBadRequest {
		t.Errorf("recusa com devolução: %d", code)
	}
}

func TestInfractionOverdue(t *testing.T) {
	e := setup(t, dict.Config{BucketCapacity: 100, RefillPerMinute: 60, NotFoundCost: 1, AnalysisDeadline: 100 * time.Millisecond})
	_, rep := e.report(http.MethodPost, "/api/v1/infraction-reports", ispbA, newReport(pix.NewEndToEndID(ispbA, time.Now()), "10.00"))
	time.Sleep(200 * time.Millisecond)
	if _, got := e.report(http.MethodGet, "/api/v1/infraction-reports/"+rep.ID, ispbB, nil); !got.Over {
		t.Fatalf("relato aberto além do prazo deveria vir como overdue: %+v", got)
	}
}

// Duas decisões simultâneas: só uma vale.
func TestInfractionOnlyOneDecisionWins(t *testing.T) {
	e := setup(t, defaultCfg)
	_, rep := e.report(http.MethodPost, "/api/v1/infraction-reports", ispbA, newReport(pix.NewEndToEndID(ispbA, time.Now()), "10.00"))

	var ok, conflict atomic.Int32
	var wg sync.WaitGroup
	for range 6 {
		wg.Go(func() {
			switch code, _ := e.report(http.MethodPost, "/api/v1/infraction-reports/"+rep.ID+"/analysis", ispbB, map[string]any{"result": "DISAPPROVED"}); code {
			case http.StatusOK:
				ok.Add(1)
			case http.StatusConflict:
				conflict.Add(1)
			default:
				t.Errorf("status %d", code)
			}
		})
	}
	wg.Wait()
	if ok.Load() != 1 || conflict.Load() != 5 {
		t.Fatalf("decisões aceitas %d, recusadas %d; quer 1 e 5", ok.Load(), conflict.Load())
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
