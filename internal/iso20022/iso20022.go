// Package iso20022 define as mensagens do SPI usadas no laboratório: pacs.008
// (ordem de pagamento), pacs.002 (status) e pacs.004 (devolução). O formato é JSON com os nomes de
// campo do ISO 20022, só com os campos usados; estruturas profundas do padrão
// (ex.: Dbtr.Id.PrvtId.Othr.Id) foram achatadas e estão indicadas.
package iso20022

import (
	"errors"
	"fmt"
	"time"

	"github.com/brenokcunha/light-pix/internal/pix"
)

// Identificadores de mensagem (AppHdr.MsgDefIdr).
const (
	MsgPacs008 = "pacs.008"
	MsgPacs002 = "pacs.002"
	MsgPacs004 = "pacs.004"
)

// Status da transação (TxSts).
const (
	StatusAccepted = "ACSP" // aceita pelo recebedor, aguardando liquidação
	StatusSettled  = "ACSC" // liquidada
	StatusRejected = "RJCT"
	StatusPending  = "PDNG" // só na consulta de status: aguardando o recebedor
)

// Códigos de motivo de rejeição (StsRsnInf.Rsn) usados no laboratório.
const (
	ReasonInsufficientFunds = "AM04" // saldo insuficiente na conta PI do pagador
	ReasonSettlementTimeout = "AB03" // recebedor não respondeu no prazo
	ReasonInvalidAccount    = "AC03" // conta do recebedor inexistente
	ReasonBlockedAccount    = "AC06" // conta do recebedor bloqueada
	ReasonSettlementFailed  = "ED05" // falha na liquidação
	ReasonAmountInvalid     = "AM09" // valor da devolução acima do que resta do Pix original
	ReasonInconsistent      = "BE01" // devolução inconsistente com o Pix original
)

// Motivos de devolução (RtrRsnInf.Rsn).
const (
	ReturnBankError       = "BE08" // erro do PSP
	ReturnFraud           = "FR01" // fraude (MED)
	ReturnCustomerRequest = "MD06" // solicitação de reembolso do cliente
	ReturnSpecial         = "SL02" // devolução especial
)

// ValidReturnReason diz se o motivo é um dos aceitos no laboratório.
func ValidReturnReason(r string) bool {
	switch r {
	case ReturnBankError, ReturnFraud, ReturnCustomerRequest, ReturnSpecial:
		return true
	}
	return false
}

// Envelope é a unidade trafegada com o SPI: cabeçalho de aplicação (head.001)
// e exatamente um documento.
type Envelope struct {
	AppHdr            AppHdr   `json:"AppHdr"`
	FIToFICstmrCdtTrf *Pacs008 `json:"FIToFICstmrCdtTrf,omitempty"`
	FIToFIPmtStsRpt   *Pacs002 `json:"FIToFIPmtStsRpt,omitempty"`
	PmtRtr            *Pacs004 `json:"PmtRtr,omitempty"`
}

// AppHdr identifica remetente, destinatário e tipo da mensagem.
type AppHdr struct {
	Fr        string    `json:"Fr"` // ISPB do remetente
	To        string    `json:"To"` // ISPB do destinatário
	BizMsgIdr string    `json:"BizMsgIdr"`
	MsgDefIdr string    `json:"MsgDefIdr"`
	CreDt     time.Time `json:"CreDt"`
}

// GrpHdr é o cabeçalho do documento.
type GrpHdr struct {
	MsgId   string    `json:"MsgId"`
	CreDtTm time.Time `json:"CreDtTm"`
}

// Pacs008 é a ordem de crédito do PSP pagador para o recebedor.
type Pacs008 struct {
	GrpHdr      GrpHdr      `json:"GrpHdr"`
	CdtTrfTxInf CdtTrfTxInf `json:"CdtTrfTxInf"`
}

// CdtTrfTxInf descreve a transação.
type CdtTrfTxInf struct {
	PmtId          PmtId                   `json:"PmtId"`
	IntrBkSttlmAmt ActiveCurrencyAndAmount `json:"IntrBkSttlmAmt"`
	// AccptncDtTm é o instante em que o PSP pagador aceitou a ordem do
	// cliente (início da medição do IGA do lado do pagador).
	AccptncDtTm time.Time `json:"AccptncDtTm"`
	// LclInstrm é a forma de iniciação: MANU (manual), DICT (chave), QRDN/QRES (QR code).
	LclInstrm string  `json:"LclInstrm,omitempty"`
	Dbtr      Party   `json:"Dbtr"`
	DbtrAcct  Account `json:"DbtrAcct"`
	DbtrAgt   Agent   `json:"DbtrAgt"`
	Cdtr      Party   `json:"Cdtr"`
	CdtrAcct  Account `json:"CdtrAcct"`
	CdtrAgt   Agent   `json:"CdtrAgt"`
	RmtInf    string  `json:"RmtInf,omitempty"` // simplifica RmtInf.Ustrd
}

// PmtId carrega o EndToEndId, chave de idempotência em todo o fluxo.
type PmtId struct {
	EndToEndId string `json:"EndToEndId"`
}

// ActiveCurrencyAndAmount é o valor com moeda.
type ActiveCurrencyAndAmount struct {
	Ccy   string     `json:"Ccy"`
	Value pix.Amount `json:"Value"`
}

// Party é pagador ou recebedor.
type Party struct {
	Nm string `json:"Nm"`
	Id string `json:"Id"` // CPF ou CNPJ; simplifica Id.PrvtId|OrgId.Othr.Id
}

// Account é a conta transacional.
type Account struct {
	Issr string `json:"Issr"`           // agência
	Id   string `json:"Id"`             // número da conta
	Tp   string `json:"Tp"`             // CACC, SVGS, TRAN, SLRY
	Prxy string `json:"Prxy,omitempty"` // chave Pix usada; simplifica Prxy.Id
}

// Agent é o participante; simplifica FinInstnId.ClrSysMmbId.MmbId.
type Agent struct {
	MmbId string `json:"MmbId"` // ISPB
}

// Pacs002 informa o status de uma pacs.008.
type Pacs002 struct {
	GrpHdr            GrpHdr      `json:"GrpHdr"`
	OrgnlGrpInfAndSts OrgnlGrpInf `json:"OrgnlGrpInfAndSts"`
	TxInfAndSts       TxInfAndSts `json:"TxInfAndSts"`
}

// OrgnlGrpInf referencia a mensagem original.
type OrgnlGrpInf struct {
	OrgnlMsgId   string `json:"OrgnlMsgId"`
	OrgnlMsgNmId string `json:"OrgnlMsgNmId"`
}

// TxInfAndSts traz o status da transação.
type TxInfAndSts struct {
	OrgnlEndToEndId string     `json:"OrgnlEndToEndId"`
	TxSts           string     `json:"TxSts"`
	StsRsnInf       *StsRsnInf `json:"StsRsnInf,omitempty"`
	// AccptncDtTm: instante do aceite pelo recebedor (ACSP).
	AccptncDtTm *time.Time `json:"AccptncDtTm,omitempty"`
	// FctvIntrBkSttlmDt: instante da liquidação (ACSC); simplifica FctvIntrBkSttlmDt.DtTm.
	FctvIntrBkSttlmDt *time.Time `json:"FctvIntrBkSttlmDt,omitempty"`
}

// StsRsnInf explica uma rejeição.
type StsRsnInf struct {
	Rsn      string `json:"Rsn"`
	AddtlInf string `json:"AddtlInf,omitempty"`
}

// Pacs004 é a devolução: o recebedor do Pix original devolve o valor (todo ou
// parte) ao pagador. Segue o mesmo caminho da pacs.008: SPI → pagador original
// (que responde com pacs.002) → liquidação na conta PI.
type Pacs004 struct {
	GrpHdr GrpHdr   `json:"GrpHdr"`
	TxInf  RtrTxInf `json:"TxInf"`
}

// RtrTxInf descreve a devolução. Dbtr é quem devolve (o recebedor do Pix
// original) e Cdtr quem recebe a devolução (o pagador original).
type RtrTxInf struct {
	RtrId              string                  `json:"RtrId"`           // identificador da devolução ("D...")
	OrgnlEndToEndId    string                  `json:"OrgnlEndToEndId"` // Pix devolvido
	RtrdIntrBkSttlmAmt ActiveCurrencyAndAmount `json:"RtrdIntrBkSttlmAmt"`
	RtrRsnInf          RtrRsnInf               `json:"RtrRsnInf"`
	Dbtr               Party                   `json:"Dbtr"`
	DbtrAcct           Account                 `json:"DbtrAcct"`
	DbtrAgt            Agent                   `json:"DbtrAgt"`
	Cdtr               Party                   `json:"Cdtr"`
	CdtrAcct           Account                 `json:"CdtrAcct"`
	CdtrAgt            Agent                   `json:"CdtrAgt"`
}

// RtrRsnInf é o motivo da devolução.
type RtrRsnInf struct {
	Rsn      string `json:"Rsn"`
	AddtlInf string `json:"AddtlInf,omitempty"`
}

// NewPacs004Envelope embrulha a devolução com AppHdr de quem devolve para o SPI.
func NewPacs004Envelope(tx RtrTxInf, now time.Time) Envelope {
	from := tx.DbtrAgt.MmbId
	msgID := pix.NewMsgID(from)
	return Envelope{
		AppHdr: AppHdr{Fr: from, To: pix.BCBISPB, BizMsgIdr: msgID, MsgDefIdr: MsgPacs004, CreDt: now.UTC()},
		PmtRtr: &Pacs004{GrpHdr: GrpHdr{MsgId: msgID, CreDtTm: now.UTC()}, TxInf: tx},
	}
}

// NewPacs008Envelope embrulha a pacs.008 com AppHdr do pagador para o SPI.
func NewPacs008Envelope(tx CdtTrfTxInf, now time.Time) Envelope {
	from := tx.DbtrAgt.MmbId
	msgID := pix.NewMsgID(from)
	return Envelope{
		AppHdr: AppHdr{Fr: from, To: pix.BCBISPB, BizMsgIdr: msgID, MsgDefIdr: MsgPacs008, CreDt: now.UTC()},
		FIToFICstmrCdtTrf: &Pacs008{
			GrpHdr:      GrpHdr{MsgId: msgID, CreDtTm: now.UTC()},
			CdtTrfTxInf: tx,
		},
	}
}

// NewPacs002Envelope monta uma pacs.002 de from para to.
func NewPacs002Envelope(from, to, orgnlMsgID string, st TxInfAndSts, now time.Time) Envelope {
	msgID := pix.NewMsgID(from)
	return Envelope{
		AppHdr: AppHdr{Fr: from, To: to, BizMsgIdr: msgID, MsgDefIdr: MsgPacs002, CreDt: now.UTC()},
		FIToFIPmtStsRpt: &Pacs002{
			GrpHdr:            GrpHdr{MsgId: msgID, CreDtTm: now.UTC()},
			OrgnlGrpInfAndSts: OrgnlGrpInf{OrgnlMsgId: orgnlMsgID, OrgnlMsgNmId: MsgPacs008},
			TxInfAndSts:       st,
		},
	}
}

// Validate confere a estrutura do envelope e os campos obrigatórios. Regras
// de negócio (participante conhecido, saldo...) ficam com quem processa.
func (e Envelope) Validate() error {
	h := e.AppHdr
	if !pix.ValidISPB(h.Fr) || !pix.ValidISPB(h.To) || h.BizMsgIdr == "" {
		return errors.New("iso20022: AppHdr incompleto")
	}
	switch h.MsgDefIdr {
	case MsgPacs008:
		if e.FIToFICstmrCdtTrf == nil || e.FIToFIPmtStsRpt != nil {
			return errors.New("iso20022: pacs.008 sem documento FIToFICstmrCdtTrf")
		}
		return e.FIToFICstmrCdtTrf.CdtTrfTxInf.validate()
	case MsgPacs004:
		if e.PmtRtr == nil || e.FIToFICstmrCdtTrf != nil || e.FIToFIPmtStsRpt != nil {
			return errors.New("iso20022: pacs.004 sem documento PmtRtr")
		}
		return e.PmtRtr.TxInf.validate()
	case MsgPacs002:
		if e.FIToFIPmtStsRpt == nil || e.FIToFICstmrCdtTrf != nil {
			return errors.New("iso20022: pacs.002 sem documento FIToFIPmtStsRpt")
		}
		return e.FIToFIPmtStsRpt.TxInfAndSts.validate()
	default:
		return fmt.Errorf("iso20022: MsgDefIdr não suportado: %q", h.MsgDefIdr)
	}
}

func (tx CdtTrfTxInf) validate() error {
	payer, err := pix.ParseEndToEndID(tx.PmtId.EndToEndId)
	switch {
	case err != nil:
		return err
	case payer != tx.DbtrAgt.MmbId:
		return errors.New("iso20022: EndToEndId não pertence ao DbtrAgt")
	case tx.IntrBkSttlmAmt.Ccy != "BRL" || tx.IntrBkSttlmAmt.Value <= 0:
		return errors.New("iso20022: IntrBkSttlmAmt deve ser positivo em BRL")
	case !pix.ValidISPB(tx.CdtrAgt.MmbId):
		return errors.New("iso20022: CdtrAgt inválido")
	case tx.CdtrAcct.Issr == "" || tx.CdtrAcct.Id == "":
		return errors.New("iso20022: CdtrAcct incompleta")
	}
	return nil
}

func (tx RtrTxInf) validate() error {
	returner, err := pix.ParseReturnID(tx.RtrId)
	switch {
	case err != nil:
		return err
	case returner != tx.DbtrAgt.MmbId:
		return errors.New("iso20022: RtrId não pertence ao DbtrAgt")
	}
	if _, err := pix.ParseEndToEndID(tx.OrgnlEndToEndId); err != nil {
		return err
	}
	switch {
	case tx.RtrdIntrBkSttlmAmt.Ccy != "BRL" || tx.RtrdIntrBkSttlmAmt.Value <= 0:
		return errors.New("iso20022: RtrdIntrBkSttlmAmt deve ser positivo em BRL")
	case !pix.ValidISPB(tx.CdtrAgt.MmbId) || tx.CdtrAgt.MmbId == tx.DbtrAgt.MmbId:
		return errors.New("iso20022: CdtrAgt inválido")
	case tx.CdtrAcct.Issr == "" || tx.CdtrAcct.Id == "":
		return errors.New("iso20022: CdtrAcct incompleta")
	case !ValidReturnReason(tx.RtrRsnInf.Rsn):
		return fmt.Errorf("iso20022: motivo de devolução inválido: %q", tx.RtrRsnInf.Rsn)
	}
	return nil
}

func (st TxInfAndSts) validate() error {
	// O status refere-se a um Pix ("E...") ou a uma devolução ("D...").
	if _, _, err := pix.ParseTransactionID(st.OrgnlEndToEndId); err != nil {
		return err
	}
	switch st.TxSts {
	case StatusAccepted, StatusSettled:
		return nil
	case StatusRejected:
		if st.StsRsnInf == nil || st.StsRsnInf.Rsn == "" {
			return errors.New("iso20022: RJCT sem StsRsnInf.Rsn")
		}
		return nil
	default:
		return fmt.Errorf("iso20022: TxSts desconhecido: %q", st.TxSts)
	}
}
