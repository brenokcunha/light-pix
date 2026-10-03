package iso20022_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/brenokcunha/light-pix/internal/iso20022"
	"github.com/brenokcunha/light-pix/internal/pix"
)

func returnTx(now time.Time) iso20022.RtrTxInf {
	return iso20022.RtrTxInf{
		RtrId:              pix.NewReturnID("22222222", now),
		OrgnlEndToEndId:    pix.NewEndToEndID("11111111", now.Add(-time.Hour)),
		RtrdIntrBkSttlmAmt: iso20022.ActiveCurrencyAndAmount{Ccy: "BRL", Value: 25_00},
		RtrRsnInf:          iso20022.RtrRsnInf{Rsn: iso20022.ReturnCustomerRequest},
		Dbtr:               iso20022.Party{Nm: "Bob Barros", Id: "11144477735"},
		DbtrAcct:           iso20022.Account{Issr: "0001", Id: "2000001", Tp: "CACC"},
		DbtrAgt:            iso20022.Agent{MmbId: "22222222"},
		Cdtr:               iso20022.Party{Nm: "Alice Andrade", Id: "12345678909"},
		CdtrAcct:           iso20022.Account{Issr: "0001", Id: "1000001", Tp: "CACC"},
		CdtrAgt:            iso20022.Agent{MmbId: "11111111"},
	}
}

func TestPacs004Validation(t *testing.T) {
	now := time.Now()
	ok := iso20022.NewPacs004Envelope(returnTx(now), now)
	if err := ok.Validate(); err != nil {
		t.Fatalf("pacs.004 válida: %v", err)
	}
	if ok.AppHdr.Fr != "22222222" || ok.AppHdr.To != pix.BCBISPB || ok.AppHdr.MsgDefIdr != iso20022.MsgPacs004 {
		t.Fatalf("cabeçalho: %+v", ok.AppHdr)
	}

	// Vai e volta como JSON sem perder nada.
	b, _ := json.Marshal(ok)
	if !strings.Contains(string(b), `"PmtRtr"`) || !strings.Contains(string(b), `"RtrdIntrBkSttlmAmt":{"Ccy":"BRL","Value":25.00}`) {
		t.Fatalf("JSON: %s", b)
	}
	var back iso20022.Envelope
	if err := json.Unmarshal(b, &back); err != nil || back.Validate() != nil {
		t.Fatalf("round trip: %v / %v", err, back.Validate())
	}

	for name, mutate := range map[string]func(*iso20022.RtrTxInf){
		"RtrId de outro PSP":      func(x *iso20022.RtrTxInf) { x.RtrId = pix.NewReturnID("33333333", now) },
		"RtrId no formato de Pix": func(x *iso20022.RtrTxInf) { x.RtrId = pix.NewEndToEndID("22222222", now) },
		"original inválido":       func(x *iso20022.RtrTxInf) { x.OrgnlEndToEndId = "E123" },
		"valor zero":              func(x *iso20022.RtrTxInf) { x.RtrdIntrBkSttlmAmt.Value = 0 },
		"moeda":                   func(x *iso20022.RtrTxInf) { x.RtrdIntrBkSttlmAmt.Ccy = "USD" },
		"devolve a si mesmo":      func(x *iso20022.RtrTxInf) { x.CdtrAgt.MmbId = "22222222" },
		"conta incompleta":        func(x *iso20022.RtrTxInf) { x.CdtrAcct.Id = "" },
		"motivo desconhecido":     func(x *iso20022.RtrTxInf) { x.RtrRsnInf.Rsn = "XX99" },
	} {
		tx := returnTx(now)
		mutate(&tx)
		if err := iso20022.NewPacs004Envelope(tx, now).Validate(); err == nil {
			t.Errorf("%s: deveria ser inválida", name)
		}
	}
}

// O status de uma devolução usa o identificador "D...".
func TestPacs002AcceptsReturnIDs(t *testing.T) {
	now := time.Now()
	for _, id := range []string{pix.NewEndToEndID("11111111", now), pix.NewReturnID("22222222", now)} {
		st := iso20022.TxInfAndSts{OrgnlEndToEndId: id, TxSts: iso20022.StatusSettled}
		env := iso20022.NewPacs002Envelope(pix.BCBISPB, "11111111", "M11111111"+strings.Repeat("a", 23), st, now)
		if err := env.Validate(); err != nil {
			t.Errorf("%s: %v", id, err)
		}
	}
}
