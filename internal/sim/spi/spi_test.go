package spi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/brenokcunha/light-pix/internal/clients"
	"github.com/brenokcunha/light-pix/internal/iso20022"
	"github.com/brenokcunha/light-pix/internal/pix"
	"github.com/brenokcunha/light-pix/internal/sim/bacen"
	"github.com/brenokcunha/light-pix/internal/sim/spi"
	"github.com/brenokcunha/light-pix/internal/testutil"
)

const (
	ispbA = "11111111"
	ispbB = "22222222"
	ispbC = "33333333" // participante habilitado, mas alheio às transações
)

func TestMain(m *testing.M) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	otel.SetTracerProvider(sdktrace.NewTracerProvider())
	os.Exit(m.Run())
}

// env é um SPI com bacen-sim real por trás, ambos em bancos de teste.
type env struct {
	t     *testing.T
	url   string
	bacen *clients.Bacen
}

// flakySettler falha as primeiras chamadas com erro temporário (503).
type flakySettler struct {
	next  spi.Settler
	fails atomic.Int32
}

func (f *flakySettler) Settle(ctx context.Context, req bacen.SettlementRequest) (bacen.SettlementResult, error) {
	if f.fails.Add(-1) >= 0 {
		return bacen.SettlementResult{}, &clients.APIError{Status: http.StatusServiceUnavailable, Code: "UNAVAILABLE"}
	}
	return f.next.Settle(ctx, req)
}

func setup(t *testing.T, tweak func(*spi.Config)) *env {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	bClient, bDB := testutil.Mongo(t)
	b, err := bacen.New(ctx, bClient, bDB)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Seed(ctx, map[string]pix.Amount{ispbA: 1000_00, ispbB: 1000_00, ispbC: 0}); err != nil {
		t.Fatal(err)
	}
	bMux := http.NewServeMux()
	b.Routes(bMux)
	bSrv := httptest.NewServer(bMux)
	bacenClient := clients.NewBacen(bSrv.URL, 5*time.Second)

	cfg := spi.Config{
		Participants:    []string{ispbA, ispbB, ispbC},
		CreditorTimeout: 10 * time.Second,
		PollWait:        3 * time.Second,
		Lease:           time.Second,
		SweepInterval:   100 * time.Millisecond,
		SettleRetry:     300 * time.Millisecond,
		Settler:         bacenClient,
	}
	if tweak != nil {
		tweak(&cfg)
	}
	sClient, sDB := testutil.Mongo(t)
	s, err := spi.New(ctx, sClient, sDB, cfg)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	s.Routes(mux)
	srv := httptest.NewServer(otelhttp.NewHandler(mux, "spi"))
	done := make(chan struct{})
	go func() { _ = s.Run(ctx); close(done) }()

	// Ordem inversa: para a varredura antes de fechar servidores e bancos.
	t.Cleanup(func() { srv.Close(); bSrv.Close() })
	t.Cleanup(func() { cancel(); <-done })
	return &env{t: t, url: srv.URL, bacen: bacenClient}
}

// send posta a mensagem como o participante from e devolve status e código de erro.
func (e *env) send(from string, msg iso20022.Envelope, header ...string) (int, string) {
	e.t.Helper()
	body, _ := json.Marshal(msg)
	req, _ := http.NewRequest(http.MethodPost, e.url+"/api/v1/in/"+from+"/msgs", bytes.NewReader(body))
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	var apiErr struct{ Code string }
	_ = json.NewDecoder(resp.Body).Decode(&apiErr)
	return resp.StatusCode, apiErr.Code
}

// pull faz um long-polling; path é o caminho completo (o PI-PULL-NEXT anterior)
// ou vazio para começar.
func (e *env) pull(ispb, path string, wait time.Duration) ([]spi.Delivery, string) {
	e.t.Helper()
	if path == "" {
		path = "/api/v1/out/" + ispb + "/stream"
	}
	sep := "?"
	if strings.Contains(path, "?") {
		sep = "&"
	}
	resp, err := http.Get(e.url + path + sep + "wait=" + wait.String())
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	next := resp.Header.Get(pix.HeaderPullNext)
	switch resp.StatusCode {
	case http.StatusNoContent:
		return nil, next
	case http.StatusOK:
		var b spi.Batch
		if err := json.NewDecoder(resp.Body).Decode(&b); err != nil {
			e.t.Fatal(err)
		}
		return b.Messages, next
	default:
		e.t.Fatalf("pull %s: status %d", ispb, resp.StatusCode)
		return nil, ""
	}
}

// pullOne espera exatamente uma mensagem e confirma o lote.
func (e *env) pullOne(ispb string, wait time.Duration) spi.Delivery {
	e.t.Helper()
	msgs, next := e.pull(ispb, "", wait)
	if len(msgs) != 1 {
		e.t.Fatalf("%s recebeu %d mensagens, quer 1", ispb, len(msgs))
	}
	e.pull(ispb, next, 0) // ack
	return msgs[0]
}

func (e *env) expectEmpty(ispb string) {
	e.t.Helper()
	if msgs, _ := e.pull(ispb, "", 0); len(msgs) != 0 {
		e.t.Fatalf("%s recebeu %d mensagens inesperadas: %+v", ispb, len(msgs), msgs[0].Envelope.AppHdr)
	}
}

func (e *env) status(requester, e2e string) (int, iso20022.TxInfAndSts) {
	e.t.Helper()
	req, _ := http.NewRequest(http.MethodGet, e.url+"/api/v1/status/"+e2e, nil)
	req.Header.Set(pix.HeaderRequestingParticipant, requester)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	var st iso20022.TxInfAndSts
	_ = json.NewDecoder(resp.Body).Decode(&st)
	return resp.StatusCode, st
}

func (e *env) balance(ispb string) pix.Amount {
	e.t.Helper()
	acc, err := e.bacen.Account(context.Background(), ispb)
	if err != nil {
		e.t.Fatal(err)
	}
	return acc.Balance
}

func pacs008(from, to string, amount pix.Amount) iso20022.Envelope {
	return iso20022.NewPacs008Envelope(iso20022.CdtTrfTxInf{
		PmtId:          iso20022.PmtId{EndToEndId: pix.NewEndToEndID(from, time.Now())},
		IntrBkSttlmAmt: iso20022.ActiveCurrencyAndAmount{Ccy: "BRL", Value: amount},
		AccptncDtTm:    time.Now().UTC(),
		LclInstrm:      "DICT",
		Dbtr:           iso20022.Party{Nm: "Alice Andrade", Id: "12345678909"},
		DbtrAcct:       iso20022.Account{Issr: "0001", Id: "1000001", Tp: "CACC"},
		DbtrAgt:        iso20022.Agent{MmbId: from},
		Cdtr:           iso20022.Party{Nm: "Bob Barros", Id: "11144477735"},
		CdtrAcct:       iso20022.Account{Issr: "0001", Id: "2000001", Tp: "CACC", Prxy: "bob@example.com"},
		CdtrAgt:        iso20022.Agent{MmbId: to},
	}, time.Now())
}

// reply é a pacs.002 do recebedor para o SPI.
func reply(from string, orig iso20022.Envelope, sts, reason string) iso20022.Envelope {
	st := iso20022.TxInfAndSts{OrgnlEndToEndId: e2eOf(orig), TxSts: sts}
	if reason != "" {
		st.StsRsnInf = &iso20022.StsRsnInf{Rsn: reason}
	} else {
		now := time.Now().UTC()
		st.AccptncDtTm = &now
	}
	return iso20022.NewPacs002Envelope(from, pix.BCBISPB, orig.FIToFICstmrCdtTrf.GrpHdr.MsgId, st, time.Now())
}

func e2eOf(msg iso20022.Envelope) string { return msg.FIToFICstmrCdtTrf.CdtTrfTxInf.PmtId.EndToEndId }

func wantStatus(t *testing.T, d spi.Delivery, sts, reason string) {
	t.Helper()
	r := d.Envelope.FIToFIPmtStsRpt
	if d.Envelope.AppHdr.MsgDefIdr != iso20022.MsgPacs002 || r == nil || r.TxInfAndSts.TxSts != sts {
		t.Fatalf("mensagem = %+v, quer pacs.002 %s", d.Envelope.AppHdr, sts)
	}
	got := ""
	if r.TxInfAndSts.StsRsnInf != nil {
		got = r.TxInfAndSts.StsRsnInf.Rsn
	}
	if got != reason {
		t.Fatalf("motivo = %q, quer %q", got, reason)
	}
}

func TestPaymentSettles(t *testing.T) {
	e := setup(t, nil)
	msg := pacs008(ispbA, ispbB, 150_00)
	e2e := e2eOf(msg)
	const traceID = "4bf92f3577b34da6a3ce929b0e0e4736"

	if code, _ := e.send(ispbA, msg, "traceparent", "00-"+traceID+"-00f067aa0ba902b7-01"); code != http.StatusAccepted {
		t.Fatalf("envio da pacs.008: %d", code)
	}
	if code, st := e.status(ispbA, e2e); code != http.StatusOK || st.TxSts != iso20022.StatusPending {
		t.Fatalf("status antes da resposta: %d %+v", code, st)
	}
	if code, _ := e.status(ispbC, e2e); code != http.StatusNotFound {
		t.Fatalf("terceiro consultou a transação: %d", code)
	}

	fwd := e.pullOne(ispbB, time.Second)
	h := fwd.Envelope.AppHdr
	if h.MsgDefIdr != iso20022.MsgPacs008 || h.Fr != pix.BCBISPB || h.To != ispbB || e2eOf(fwd.Envelope) != e2e {
		t.Fatalf("pacs.008 encaminhada com cabeçalho %+v", h)
	}
	if !strings.Contains(fwd.TraceContext["traceparent"], traceID) {
		t.Fatalf("trace não propagado até o recebedor: %v", fwd.TraceContext)
	}

	if code, _ := e.send(ispbB, reply(ispbB, msg, iso20022.StatusAccepted, "")); code != http.StatusAccepted {
		t.Fatalf("envio do ACSP: %d", code)
	}
	wantStatus(t, e.pullOne(ispbA, time.Second), iso20022.StatusSettled, "")
	wantStatus(t, e.pullOne(ispbB, time.Second), iso20022.StatusSettled, "")
	if _, st := e.status(ispbB, e2e); st.TxSts != iso20022.StatusSettled || st.FctvIntrBkSttlmDt == nil || st.AccptncDtTm == nil {
		t.Fatalf("status final: %+v", st)
	}
	if a, b := e.balance(ispbA), e.balance(ispbB); a != 850_00 || b != 1150_00 {
		t.Fatalf("saldos PI = %s / %s", a, b)
	}

	t.Run("ACSP repetido não liquida de novo", func(t *testing.T) {
		if code, _ := e.send(ispbB, reply(ispbB, msg, iso20022.StatusAccepted, "")); code != http.StatusAccepted {
			t.Fatalf("ACSP repetido: %d", code)
		}
		e.expectEmpty(ispbA)
		if a := e.balance(ispbA); a != 850_00 {
			t.Fatalf("saldo PI = %s", a)
		}
	})
}

func TestCreditorRejects(t *testing.T) {
	e := setup(t, nil)
	msg := pacs008(ispbA, ispbB, 10_00)
	e.send(ispbA, msg)
	e.pullOne(ispbB, time.Second)

	if code, _ := e.send(ispbB, reply(ispbB, msg, iso20022.StatusRejected, iso20022.ReasonInvalidAccount)); code != http.StatusAccepted {
		t.Fatalf("envio do RJCT: %d", code)
	}
	wantStatus(t, e.pullOne(ispbA, time.Second), iso20022.StatusRejected, iso20022.ReasonInvalidAccount)
	e.expectEmpty(ispbB) // quem rejeitou não recebe aviso
	if a, b := e.balance(ispbA), e.balance(ispbB); a != 1000_00 || b != 1000_00 {
		t.Fatalf("saldos PI mudaram: %s / %s", a, b)
	}
}

func TestCreditorTimeout(t *testing.T) {
	e := setup(t, func(c *spi.Config) { c.CreditorTimeout = 500 * time.Millisecond })
	msg := pacs008(ispbA, ispbB, 10_00)
	e.send(ispbA, msg)
	e.pullOne(ispbB, time.Second)

	// Recebedor não responde: os dois lados recebem RJCT AB03.
	wantStatus(t, e.pullOne(ispbA, 3*time.Second), iso20022.StatusRejected, iso20022.ReasonSettlementTimeout)
	wantStatus(t, e.pullOne(ispbB, 3*time.Second), iso20022.StatusRejected, iso20022.ReasonSettlementTimeout)

	// O ACSP tardio é aceito e ignorado.
	if code, _ := e.send(ispbB, reply(ispbB, msg, iso20022.StatusAccepted, "")); code != http.StatusAccepted {
		t.Fatalf("ACSP tardio: %d", code)
	}
	if _, st := e.status(ispbA, e2eOf(msg)); st.TxSts != iso20022.StatusRejected || st.StsRsnInf.Rsn != iso20022.ReasonSettlementTimeout {
		t.Fatalf("status após ACSP tardio: %+v", st)
	}
	if a := e.balance(ispbA); a != 1000_00 {
		t.Fatalf("ACSP tardio liquidou: saldo %s", a)
	}
}

func TestInsufficientFunds(t *testing.T) {
	e := setup(t, nil)
	msg := pacs008(ispbA, ispbB, 1000_01)
	e.send(ispbA, msg)
	e.pullOne(ispbB, time.Second)
	e.send(ispbB, reply(ispbB, msg, iso20022.StatusAccepted, ""))

	// O recebedor já tinha aceitado: precisa saber que não haverá crédito.
	wantStatus(t, e.pullOne(ispbA, time.Second), iso20022.StatusRejected, iso20022.ReasonInsufficientFunds)
	wantStatus(t, e.pullOne(ispbB, time.Second), iso20022.StatusRejected, iso20022.ReasonInsufficientFunds)
}

func TestSettlementRetriedAfterTemporaryFailure(t *testing.T) {
	var flaky *flakySettler
	e := setup(t, func(c *spi.Config) {
		flaky = &flakySettler{next: c.Settler}
		flaky.fails.Store(2)
		c.Settler = flaky
	})
	msg := pacs008(ispbA, ispbB, 10_00)
	e.send(ispbA, msg)
	e.pullOne(ispbB, time.Second)
	e.send(ispbB, reply(ispbB, msg, iso20022.StatusAccepted, ""))

	if _, st := e.status(ispbA, e2eOf(msg)); st.TxSts != iso20022.StatusAccepted {
		t.Fatalf("após falha temporária: %+v, quer ACSP (liquidando)", st)
	}
	// A varredura tenta de novo até o bacen responder.
	wantStatus(t, e.pullOne(ispbA, 3*time.Second), iso20022.StatusSettled, "")
	if a := e.balance(ispbA); a != 990_00 {
		t.Fatalf("saldo PI = %s", a)
	}
}

func TestDuplicatePacs008(t *testing.T) {
	e := setup(t, nil)
	msg := pacs008(ispbA, ispbB, 10_00)
	e.send(ispbA, msg)
	if code, _ := e.send(ispbA, msg); code != http.StatusAccepted {
		t.Fatalf("reenvio idêntico: %d, quer 202", code)
	}
	e.pullOne(ispbB, time.Second) // pullOne falha se houver duas

	other := pacs008(ispbA, ispbB, 20_00)
	other.FIToFICstmrCdtTrf.CdtTrfTxInf.PmtId = msg.FIToFICstmrCdtTrf.CdtTrfTxInf.PmtId
	if code, errCode := e.send(ispbA, other); code != http.StatusConflict || errCode != "DUPLICATE_END_TO_END_ID" {
		t.Fatalf("mesmo EndToEndId em outra mensagem: %d %s", code, errCode)
	}
}

func TestRejectsInvalidMessages(t *testing.T) {
	e := setup(t, nil)
	msg := pacs008(ispbA, ispbB, 10_00)
	e.send(ispbA, msg)

	cases := []struct {
		name string
		from string
		msg  iso20022.Envelope
		code int
		err  string
	}{
		{"recebedor não habilitado", ispbA, pacs008(ispbA, "44444444", 1_00), http.StatusUnprocessableEntity, "UNKNOWN_PARTICIPANT"},
		{"remetente diferente do caminho", ispbB, pacs008(ispbA, ispbB, 1_00), http.StatusForbidden, "FORBIDDEN"},
		{"mesmo participante", ispbA, pacs008(ispbA, ispbA, 1_00), http.StatusBadRequest, "INVALID_MESSAGE"},
		{"pacs.002 do pagador", ispbA, reply(ispbA, msg, iso20022.StatusAccepted, ""), http.StatusForbidden, "FORBIDDEN"},
		{"pacs.002 de transação inexistente", ispbB, reply(ispbB, pacs008(ispbA, ispbB, 1_00), iso20022.StatusAccepted, ""), http.StatusNotFound, "UNKNOWN_END_TO_END_ID"},
		{"RJCT sem motivo", ispbB, func() iso20022.Envelope {
			r := reply(ispbB, msg, iso20022.StatusRejected, "X")
			r.FIToFIPmtStsRpt.TxInfAndSts.StsRsnInf = nil
			return r
		}(), http.StatusBadRequest, "INVALID_MESSAGE"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if code, errCode := e.send(c.from, c.msg); code != c.code || errCode != c.err {
				t.Fatalf("resposta %d %s, quer %d %s", code, errCode, c.code, c.err)
			}
		})
	}
}

// Sem confirmação, o lote volta a ser entregue quando o lease vence.
func TestRedeliveryWithoutAck(t *testing.T) {
	e := setup(t, nil)
	e.send(ispbA, pacs008(ispbA, ispbB, 10_00))

	first, _ := e.pull(ispbB, "", time.Second)
	if len(first) != 1 {
		t.Fatalf("primeiro pull: %d mensagens", len(first))
	}
	e.expectEmpty(ispbB) // reservada pelo lease

	time.Sleep(1100 * time.Millisecond)
	again, next := e.pull(ispbB, "", 2*time.Second)
	if len(again) != 1 || again[0].Envelope.AppHdr.BizMsgIdr != first[0].Envelope.AppHdr.BizMsgIdr {
		t.Fatalf("reentrega: %d mensagens", len(again))
	}
	if !strings.Contains(next, "ack=") {
		t.Fatalf("PI-PULL-NEXT sem ack: %q", next)
	}
	e.pull(ispbB, next, 0)
	time.Sleep(1100 * time.Millisecond)
	e.expectEmpty(ispbB) // confirmada não volta
}

// O long-polling acorda assim que chega mensagem, sem esperar o prazo.
func TestLongPollWakesOnMessage(t *testing.T) {
	e := setup(t, nil)
	got := make(chan int, 1)
	go func() {
		msgs, _ := e.pull(ispbB, "", 3*time.Second)
		got <- len(msgs)
	}()
	time.Sleep(200 * time.Millisecond)
	start := time.Now()
	e.send(ispbA, pacs008(ispbA, ispbB, 10_00))
	select {
	case n := <-got:
		if n != 1 {
			t.Fatalf("pull acordou com %d mensagens", n)
		}
		if d := time.Since(start); d > 900*time.Millisecond {
			t.Fatalf("pull demorou %v para acordar", d)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("pull não voltou")
	}
}
