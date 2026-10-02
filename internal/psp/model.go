// Package psp é o núcleo do PSP: contas e razão, pagamentos Pix enviados e o
// tratamento das mensagens que chegam do SPI. Cada papel (api, relay,
// connector, worker) usa a parte que lhe cabe.
package psp

import (
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/brenokcunha/light-pix/internal/pix"
)

// Sentidos do pagamento.
const DirectionOut = "OUT"

// Estados do Pix enviado. Só avançam: PENDING → SENT → CONFIRMED | REJECTED.
const (
	StatePending   = "PENDING"   // gravado com o saldo reservado; ainda não entregue ao SPI
	StateSent      = "SENT"      // SPI aceitou a pacs.008
	StateConfirmed = "CONFIRMED" // liquidado (ACSC): a reserva foi baixada
	StateRejected  = "REJECTED"  // recusado (RJCT): a reserva voltou ao saldo
)

// Situações da conta.
const (
	AccountActive  = "ACTIVE"
	AccountBlocked = "BLOCKED"
	AccountClosed  = "CLOSED"
)

// Tipos de lançamento do razão. Cada lançamento diz quanto moveu em cada
// parte do saldo, então available e held são sempre a soma dos lançamentos.
const (
	EntrySeed    = "SEED"    // saldo inicial
	EntryHold    = "HOLD"    // reserva ao iniciar o Pix: available −, held +
	EntryConfirm = "CONFIRM" // liquidação: held −
	EntryRelease = "RELEASE" // rejeição: held −, available +
)

// Holder é o titular da conta.
type Holder struct {
	Type  string `bson:"type" json:"type"` // NATURAL_PERSON | LEGAL_PERSON
	Name  string `bson:"name" json:"name"`
	TaxID string `bson:"taxId" json:"taxId"` // CPF ou CNPJ
}

// Balance é o saldo em centavos: available pode ser usado; held está
// reservado por Pix enviados ainda não liquidados.
type Balance struct {
	Available pix.Amount `bson:"available" json:"available"`
	Held      pix.Amount `bson:"held" json:"held"`
}

// Account é uma conta transacional do PSP.
type Account struct {
	ID        bson.ObjectID `bson:"_id" json:"-"`
	Branch    string        `bson:"branch" json:"branch"`
	Number    string        `bson:"number" json:"number"`
	Type      string        `bson:"type" json:"type"` // CACC, SVGS, SLRY, TRAN
	Holder    Holder        `bson:"holder" json:"holder"`
	Balance   Balance       `bson:"balance" json:"balance"`
	Status    string        `bson:"status" json:"status"`
	CreatedAt time.Time     `bson:"createdAt" json:"-"`
	UpdatedAt time.Time     `bson:"updatedAt" json:"updatedAt"`
}

// LedgerEntry é um lançamento append-only. EndToEndID liga o lançamento ao
// pagamento; (conta, EndToEndID, tipo) é único, o que torna cada baixa ou
// estorno idempotente.
type LedgerEntry struct {
	ID             bson.ObjectID `bson:"_id"`
	AccountID      bson.ObjectID `bson:"accountId"`
	EndToEndID     string        `bson:"endToEndId"`
	Kind           string        `bson:"kind"`
	AvailableDelta pix.Amount    `bson:"availableDelta"`
	HeldDelta      pix.Amount    `bson:"heldDelta"`
	CreatedAt      time.Time     `bson:"createdAt"`
}

// Party é uma ponta do pagamento.
type Party struct {
	ISPB   string `bson:"ispb" json:"ispb"`
	Branch string `bson:"branch" json:"branch"`
	Number string `bson:"number" json:"number"`
	Type   string `bson:"type" json:"type"`
	Name   string `bson:"name" json:"name"`
	TaxID  string `bson:"taxId" json:"taxId"`
	Key    string `bson:"key,omitempty" json:"key,omitempty"` // chave Pix usada (só no recebedor)
	// AccountID liga o pagador à conta do PSP; não vai na API.
	AccountID bson.ObjectID `bson:"accountId,omitempty" json:"-"`
}

// Transition é uma mudança de estado no histórico do pagamento.
type Transition struct {
	State  string    `bson:"state" json:"state"`
	At     time.Time `bson:"at" json:"at"`
	Reason string    `bson:"reason,omitempty" json:"reason,omitempty"`
}

// Payment é um Pix. O _id é o EndToEndId.
type Payment struct {
	EndToEndID  string       `bson:"_id" json:"endToEndId"`
	Direction   string       `bson:"direction" json:"direction"`
	State       string       `bson:"state" json:"state"`
	Amount      pix.Amount   `bson:"amount" json:"amount"`
	Description string       `bson:"description,omitempty" json:"description,omitempty"`
	Debtor      Party        `bson:"debtor" json:"debtor"`
	Creditor    Party        `bson:"creditor" json:"creditor"`
	MsgID       string       `bson:"msgId" json:"-"` // MsgId da pacs.008
	Reason      string       `bson:"reason,omitempty" json:"reason,omitempty"`
	History     []Transition `bson:"history" json:"history"`
	CreatedAt   time.Time    `bson:"createdAt" json:"createdAt"` // t0: o pedido chegou à API
	SentAt      *time.Time   `bson:"sentAt,omitempty" json:"sentAt,omitempty"`
	SettledAt   *time.Time   `bson:"settledAt,omitempty" json:"settledAt,omitempty"`
	FinishedAt  *time.Time   `bson:"finishedAt,omitempty" json:"finishedAt,omitempty"`
	UpdatedAt   time.Time    `bson:"updatedAt" json:"updatedAt"`
}

// IdempotencyKey liga uma Idempotency-Key (por conta) ao pagamento criado.
type IdempotencyKey struct {
	ID          bson.ObjectID `bson:"_id"`
	Scope       string        `bson:"scope"` // conta pagadora
	Key         string        `bson:"key"`
	RequestHash string        `bson:"requestHash"`
	EndToEndID  string        `bson:"endToEndId"`
	CreatedAt   time.Time     `bson:"createdAt"`
}
