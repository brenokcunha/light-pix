package spi_test

import (
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/brenokcunha/light-pix/internal/iso20022"
	"github.com/brenokcunha/light-pix/internal/pix"
	"github.com/brenokcunha/light-pix/internal/sim/spi"
)

// settledPix liquida um Pix de A para B e devolve a pacs.008.
func (e *env) settledPix(amount pix.Amount) iso20022.Envelope {
	e.t.Helper()
	msg := pacs008(ispbA, ispbB, amount)
	if code, _ := e.send(ispbA, msg); code != http.StatusAccepted {
		e.t.Fatalf("pacs.008: %d", code)
	}
	e.pullOne(ispbB, time.Second)
	e.send(ispbB, reply(ispbB, msg, iso20022.StatusAccepted, ""))
	wantStatus(e.t, e.pullOne(ispbA, time.Second), iso20022.StatusSettled, "")
	wantStatus(e.t, e.pullOne(ispbB, time.Second), iso20022.StatusSettled, "")
	return msg
}

// pacs004 é a devolução de B (recebedor do Pix original) para A (pagador).
func pacs004(original iso20022.Envelope, amount pix.Amount) iso20022.Envelope {
	now := time.Now()
	return iso20022.NewPacs004Envelope(iso20022.RtrTxInf{
		RtrId:              pix.NewReturnID(ispbB, now),
		OrgnlEndToEndId:    e2eOf(original),
		RtrdIntrBkSttlmAmt: iso20022.ActiveCurrencyAndAmount{Ccy: "BRL", Value: amount},
		RtrRsnInf:          iso20022.RtrRsnInf{Rsn: iso20022.ReturnCustomerRequest},
		Dbtr:               iso20022.Party{Nm: "Bob Barros", Id: "11144477735"},
		DbtrAcct:           iso20022.Account{Issr: "0001", Id: "2000001", Tp: "CACC"},
		DbtrAgt:            iso20022.Agent{MmbId: ispbB},
		Cdtr:               iso20022.Party{Nm: "Alice Andrade", Id: "12345678909"},
		CdtrAcct:           iso20022.Account{Issr: "0001", Id: "1000001", Tp: "CACC"},
		CdtrAgt:            iso20022.Agent{MmbId: ispbA},
	}, now)
}

func returnID(m iso20022.Envelope) string { return m.PmtRtr.TxInf.RtrId }

// replyReturn é a pacs.002 do pagador original sobre a devolução.
func replyReturn(from string, ret iso20022.Envelope, sts, reason string) iso20022.Envelope {
	st := iso20022.TxInfAndSts{OrgnlEndToEndId: returnID(ret), TxSts: sts}
	if reason != "" {
		st.StsRsnInf = &iso20022.StsRsnInf{Rsn: reason}
	} else {
		now := time.Now().UTC()
		st.AccptncDtTm = &now
	}
	return iso20022.NewPacs002Envelope(from, pix.BCBISPB, ret.PmtRtr.GrpHdr.MsgId, st, time.Now())
}

func TestReturnSettlesAndMovesTheMoneyBack(t *testing.T) {
	e := setup(t, nil)
	orig := e.settledPix(100_00)
	ret := pacs004(orig, 40_00)

	if code, _ := e.send(ispbB, ret); code != http.StatusAccepted {
		t.Fatalf("pacs.004: %d", code)
	}
	fwd := e.pullOne(ispbA, time.Second)
	if h := fwd.Envelope.AppHdr; h.MsgDefIdr != iso20022.MsgPacs004 || h.Fr != pix.BCBISPB || h.To != ispbA ||
		fwd.Envelope.PmtRtr.TxInf.OrgnlEndToEndId != e2eOf(orig) {
		t.Fatalf("pacs.004 encaminhada: %+v", h)
	}
	if code, st := e.status(ispbB, returnID(ret)); code != http.StatusOK || st.TxSts != iso20022.StatusPending {
		t.Fatalf("status antes do aceite: %d %+v", code, st)
	}

	if code, _ := e.send(ispbA, replyReturn(ispbA, ret, iso20022.StatusAccepted, "")); code != http.StatusAccepted {
		t.Fatalf("ACSP da devolução: %d", code)
	}
	for _, ispb := range []string{ispbB, ispbA} {
		d := e.pullOne(ispb, time.Second)
		wantStatus(t, d, iso20022.StatusSettled, "")
		if got := d.Envelope.FIToFIPmtStsRpt.OrgnlGrpInfAndSts.OrgnlMsgNmId; got != iso20022.MsgPacs004 {
			t.Errorf("OrgnlMsgNmId = %q", got)
		}
	}
	// 1000 − 100 + 40 em A; 1000 + 100 − 40 em B.
	if a, b := e.balance(ispbA), e.balance(ispbB); a != 940_00 || b != 1060_00 {
		t.Fatalf("contas PI: %s / %s", a, b)
	}
	if code, st := e.status(ispbA, returnID(ret)); code != http.StatusOK || st.TxSts != iso20022.StatusSettled {
		t.Fatalf("status final: %d %+v", code, st)
	}
}

func TestReturnValidation(t *testing.T) {
	e := setup(t, nil)
	orig := e.settledPix(100_00)

	t.Run("Pix original inexistente", func(t *testing.T) {
		ghost := pacs008(ispbA, ispbB, 1_00)
		if code, errCode := e.send(ispbB, pacs004(ghost, 1_00)); code != http.StatusNotFound || errCode != "UNKNOWN_END_TO_END_ID" {
			t.Fatalf("%d %s", code, errCode)
		}
	})
	t.Run("Pix original ainda não liquidado", func(t *testing.T) {
		pending := pacs008(ispbA, ispbB, 5_00)
		e.send(ispbA, pending)
		e.pullOne(ispbB, time.Second)
		if code, errCode := e.send(ispbB, pacs004(pending, 1_00)); code != http.StatusUnprocessableEntity || errCode != "ORIGINAL_NOT_SETTLED" {
			t.Fatalf("%d %s", code, errCode)
		}
	})
	t.Run("só o recebedor devolve", func(t *testing.T) {
		wrong := pacs004(orig, 1_00)
		wrong.PmtRtr.TxInf.RtrId = pix.NewReturnID(ispbA, time.Now())
		wrong.PmtRtr.TxInf.DbtrAgt.MmbId = ispbA
		wrong.PmtRtr.TxInf.CdtrAgt.MmbId = ispbB
		wrong.AppHdr.Fr = ispbA
		if code, errCode := e.send(ispbA, wrong); code != http.StatusForbidden || errCode != "FORBIDDEN" {
			t.Fatalf("%d %s", code, errCode)
		}
	})
	t.Run("destinatário da devolução precisa ser o pagador original", func(t *testing.T) {
		wrong := pacs004(orig, 1_00)
		wrong.PmtRtr.TxInf.CdtrAgt.MmbId = ispbC
		if code, errCode := e.send(ispbB, wrong); code != http.StatusBadRequest || errCode != "INVALID_MESSAGE" {
			t.Fatalf("%d %s", code, errCode)
		}
	})
	t.Run("valor acima do Pix", func(t *testing.T) {
		if code, errCode := e.send(ispbB, pacs004(orig, 100_01)); code != http.StatusUnprocessableEntity || errCode != "RETURN_AMOUNT_EXCEEDED" {
			t.Fatalf("%d %s", code, errCode)
		}
	})
	t.Run("a soma das devoluções não passa do Pix", func(t *testing.T) {
		first := pacs004(orig, 60_00)
		if code, _ := e.send(ispbB, first); code != http.StatusAccepted {
			t.Fatalf("primeira devolução: %d", code)
		}
		if code, errCode := e.send(ispbB, pacs004(orig, 40_01)); code != http.StatusUnprocessableEntity || errCode != "RETURN_AMOUNT_EXCEEDED" {
			t.Fatalf("segunda acima do resto: %d %s", code, errCode)
		}
		if code, _ := e.send(ispbB, pacs004(orig, 40_00)); code != http.StatusAccepted {
			t.Fatalf("segunda no limite: %d", code)
		}
		// Mensagem repetida é aceita sem reservar de novo; outro conteúdo no mesmo id é conflito.
		if code, _ := e.send(ispbB, first); code != http.StatusAccepted {
			t.Fatalf("pacs.004 repetida: %d", code)
		}
		clash := pacs004(orig, 1_00)
		clash.PmtRtr.TxInf.RtrId = returnID(first)
		if code, errCode := e.send(ispbB, clash); code != http.StatusConflict || errCode != "DUPLICATE_END_TO_END_ID" {
			t.Fatalf("mesmo id, outra mensagem: %d %s", code, errCode)
		}
	})
}

func TestReturnWindowExpires(t *testing.T) {
	e := setup(t, func(c *spi.Config) { c.ReturnWindow = 300 * time.Millisecond })
	orig := e.settledPix(10_00)
	time.Sleep(400 * time.Millisecond)
	if code, errCode := e.send(ispbB, pacs004(orig, 1_00)); code != http.StatusUnprocessableEntity || errCode != "RETURN_WINDOW_EXPIRED" {
		t.Fatalf("%d %s", code, errCode)
	}
}

// A devolução recusada pelo pagador original devolve o valor ao que ainda se
// pode devolver, e nada se move nas contas PI.
func TestRejectedReturnReleasesTheAmount(t *testing.T) {
	e := setup(t, nil)
	orig := e.settledPix(100_00)

	ret := pacs004(orig, 100_00)
	e.send(ispbB, ret)
	e.pullOne(ispbA, time.Second)
	e.send(ispbA, replyReturn(ispbA, ret, iso20022.StatusRejected, iso20022.ReasonBlockedAccount))
	wantStatus(t, e.pullOne(ispbB, time.Second), iso20022.StatusRejected, iso20022.ReasonBlockedAccount)
	if a, b := e.balance(ispbA), e.balance(ispbB); a != 900_00 || b != 1100_00 {
		t.Fatalf("contas PI mudaram: %s / %s", a, b)
	}
	if code, errCode := e.send(ispbB, pacs004(orig, 100_00)); code != http.StatusAccepted {
		t.Fatalf("nova devolução do valor cheio: %d %s", code, errCode)
	}
}

func TestReturnTimeoutReleasesTheAmount(t *testing.T) {
	e := setup(t, func(c *spi.Config) { c.CreditorTimeout = 400 * time.Millisecond })
	orig := e.settledPix(50_00)
	ret := pacs004(orig, 50_00)
	e.send(ispbB, ret)
	// O pagador original não responde: o SPI rejeita (AB03) e libera o valor.
	wantStatus(t, e.pullOne(ispbB, 3*time.Second), iso20022.StatusRejected, iso20022.ReasonSettlementTimeout)
	if code, _ := e.send(ispbB, pacs004(orig, 50_00)); code != http.StatusAccepted {
		t.Fatalf("depois do timeout: %d", code)
	}
}

// Devoluções simultâneas nunca somam mais que o Pix original.
func TestConcurrentReturnsNeverExceedTheOriginal(t *testing.T) {
	e := setup(t, nil)
	orig := e.settledPix(100_00)

	var accepted, exceeded atomic.Int32
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			switch code, errCode := e.send(ispbB, pacs004(orig, 20_00)); {
			case code == http.StatusAccepted:
				accepted.Add(1)
			case errCode == "RETURN_AMOUNT_EXCEEDED":
				exceeded.Add(1)
			default:
				t.Errorf("resposta inesperada: %d %s", code, errCode)
			}
		})
	}
	wg.Wait()
	if accepted.Load() != 5 || exceeded.Load() != 5 {
		t.Fatalf("aceitas %d, recusadas %d; quer 5 e 5", accepted.Load(), exceeded.Load())
	}
}
