package psp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/brenokcunha/light-pix/internal/broker"
	"github.com/brenokcunha/light-pix/internal/clients"
	"github.com/brenokcunha/light-pix/internal/iso20022"
	"github.com/brenokcunha/light-pix/internal/outbox"
	"github.com/brenokcunha/light-pix/internal/pix"
	"github.com/brenokcunha/light-pix/internal/sim/dict"
	"github.com/brenokcunha/light-pix/internal/store"
)

// Error é um erro de negócio com o status HTTP e o código que a API devolve.
type Error struct {
	Status     int
	Code       string
	Message    string
	RetryAfter time.Duration // preenchido em 429
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

func newError(status int, code, format string, args ...any) *Error {
	return &Error{Status: status, Code: code, Message: fmt.Sprintf(format, args...)}
}

// KeyResolver resolve uma chave Pix em nome do usuário pagador (CPF/CNPJ).
type KeyResolver interface {
	Lookup(ctx context.Context, key, payerID string) (dict.Entry, error)
}

// Service concentra as regras do PSP sobre contas e pagamentos.
type Service struct {
	ispb  string
	store *Store
	keys  KeyResolver
	// ReturnWindow é o prazo para devolver um Pix recebido (padrão: 90 dias).
	ReturnWindow time.Duration
	// InfractionWindow é o prazo para relatar um Pix como infração (padrão: 80 dias).
	InfractionWindow time.Duration
	med              MEDGateway

	tracer   trace.Tracer
	created  metric.Int64Counter
	finished metric.Int64Counter
	received metric.Int64Counter
	duration metric.Float64Histogram
	// returnsFinished conta devoluções encerradas; ficam fora das métricas de Pix
	// para não entrar nos SLOs.
	returnsFinished  metric.Int64Counter
	infractionEvents metric.Int64Counter
}

// NewService cria o serviço do PSP identificado por ispb.
func NewService(ispb string, st *Store, keys KeyResolver) (*Service, error) {
	if !pix.ValidISPB(ispb) {
		return nil, fmt.Errorf("psp: ISPB inválido: %q", ispb)
	}
	s := &Service{ispb: ispb, store: st, keys: keys, tracer: otel.Tracer("github.com/brenokcunha/light-pix/internal/psp")}
	meter := otel.Meter("github.com/brenokcunha/light-pix/internal/psp")
	var err error
	if s.created, err = meter.Int64Counter("pix.payments.created",
		metric.WithDescription("Pedidos de Pix recebidos pela API, por resultado")); err != nil {
		return nil, err
	}
	if s.finished, err = meter.Int64Counter("pix.payments.finished",
		metric.WithDescription("Pix enviados que chegaram a um estado final")); err != nil {
		return nil, err
	}
	if s.received, err = meter.Int64Counter("pix.receipts",
		metric.WithDescription("Pix recebidos, por estado alcançado (ACCEPTED, REJECTED, CREDITED)")); err != nil {
		return nil, err
	}
	if s.returnsFinished, err = meter.Int64Counter("pix.returns.finished",
		metric.WithDescription("Devoluções que chegaram a um estado final, por sentido, estado e motivo")); err != nil {
		return nil, err
	}
	if s.infractionEvents, err = meter.Int64Counter("med.infractions",
		metric.WithDescription("Mudanças de estado dos relatos de infração (MED), por papel e estado")); err != nil {
		return nil, err
	}
	s.duration, err = meter.Float64Histogram("pix.payment.duration", metric.WithUnit("s"),
		metric.WithDescription("Do pedido na API até o estado final: a experiência do pagador"))
	return s, err
}

// CreateRequest é o pedido de um Pix enviado por chave.
type CreateRequest struct {
	DebtorAccount AccountRef `json:"debtorAccount"`
	Key           string     `json:"key"`
	Amount        pix.Amount `json:"amount"`
	Description   string     `json:"description,omitempty"`
}

// AccountRef identifica uma conta do PSP.
type AccountRef struct {
	Branch string `json:"branch"`
	Number string `json:"number"`
}

var errInsufficientFunds = errors.New("saldo insuficiente")

// CreatePayment inicia um Pix. Numa única transação, grava a chave de
// idempotência, reserva o saldo (condicionado a available ≥ valor), grava o
// pagamento PENDING e o evento da pacs.008 na outbox. Repetir a mesma
// Idempotency-Key com o mesmo pedido devolve o pagamento original
// (created=false); com outro pedido, é erro.
func (s *Service) CreatePayment(ctx context.Context, idemKey string, req CreateRequest) (p Payment, created bool, err error) {
	defer func() {
		result := "created"
		var apiErr *Error
		switch {
		case errors.As(err, &apiErr):
			result = strings.ToLower(apiErr.Code)
		case err != nil:
			result = "error"
		case !created:
			result = "replay"
		}
		s.created.Add(ctx, 1, metric.WithAttributes(attribute.String("result", result)))
	}()

	req.Key = strings.TrimSpace(req.Key)
	if strings.Contains(req.Key, "@") { // o DICT guarda e-mails em minúsculas
		req.Key = strings.ToLower(req.Key)
	}
	switch {
	case idemKey == "" || len(idemKey) > 64:
		return p, false, newError(http.StatusBadRequest, "INVALID_REQUEST", "header Idempotency-Key obrigatório (até 64 caracteres)")
	case req.Amount <= 0:
		return p, false, newError(http.StatusBadRequest, "INVALID_REQUEST", "amount deve ser positivo")
	case req.Key == "":
		return p, false, newError(http.StatusBadRequest, "INVALID_REQUEST", "key obrigatória")
	case req.DebtorAccount.Branch == "" || req.DebtorAccount.Number == "":
		return p, false, newError(http.StatusBadRequest, "INVALID_REQUEST", "debtorAccount.branch e number obrigatórios")
	}

	acc, err := s.store.FindAccount(ctx, req.DebtorAccount.Branch, req.DebtorAccount.Number)
	switch {
	case errors.Is(err, mongo.ErrNoDocuments):
		return p, false, newError(http.StatusNotFound, "ACCOUNT_NOT_FOUND", "conta pagadora não encontrada")
	case err != nil:
		return p, false, err
	case acc.Status != AccountActive:
		return p, false, newError(http.StatusUnprocessableEntity, "ACCOUNT_NOT_ACTIVE", "conta pagadora %s", strings.ToLower(acc.Status))
	}

	scope, hash := acc.ID.Hex(), requestHash(req)
	if prev, found, err := s.replay(ctx, scope, idemKey, hash); err != nil || found {
		return prev, false, err
	}

	entry, err := s.keys.Lookup(ctx, req.Key, acc.Holder.TaxID)
	if err != nil {
		return p, false, mapDictError(err)
	}
	if entry.Account.Participant == s.ispb {
		return p, false, newError(http.StatusUnprocessableEntity, "SAME_PSP_NOT_SUPPORTED", "chave de conta deste mesmo PSP: transferência interna fora do escopo")
	}

	now := store.Now()
	e2e := pix.NewEndToEndID(s.ispb, now)
	env := iso20022.NewPacs008Envelope(iso20022.CdtTrfTxInf{
		PmtId:          iso20022.PmtId{EndToEndId: e2e},
		IntrBkSttlmAmt: iso20022.ActiveCurrencyAndAmount{Ccy: "BRL", Value: req.Amount},
		AccptncDtTm:    now,
		LclInstrm:      "DICT",
		Dbtr:           iso20022.Party{Nm: acc.Holder.Name, Id: acc.Holder.TaxID},
		DbtrAcct:       iso20022.Account{Issr: acc.Branch, Id: acc.Number, Tp: acc.Type},
		DbtrAgt:        iso20022.Agent{MmbId: s.ispb},
		Cdtr:           iso20022.Party{Nm: entry.Owner.Name, Id: entry.Owner.TaxIdNumber},
		CdtrAcct:       iso20022.Account{Issr: entry.Account.Branch, Id: entry.Account.AccountNumber, Tp: entry.Account.AccountType, Prxy: entry.Key},
		CdtrAgt:        iso20022.Agent{MmbId: entry.Account.Participant},
		RmtInf:         req.Description,
	}, now)
	if err := env.Validate(); err != nil {
		return p, false, fmt.Errorf("pacs.008 inválida: %w", err)
	}
	body, err := json.Marshal(env)
	if err != nil {
		return p, false, err
	}
	trace.SpanFromContext(ctx).SetAttributes(attribute.String("pix.end_to_end_id", e2e))

	p = Payment{
		EndToEndID:  e2e,
		Direction:   DirectionOut,
		State:       StatePending,
		Amount:      req.Amount,
		Description: req.Description,
		Debtor: Party{ISPB: s.ispb, Branch: acc.Branch, Number: acc.Number, Type: acc.Type,
			Name: acc.Holder.Name, TaxID: acc.Holder.TaxID, AccountID: acc.ID},
		Creditor: Party{ISPB: entry.Account.Participant, Branch: entry.Account.Branch, Number: entry.Account.AccountNumber,
			Type: entry.Account.AccountType, Name: entry.Owner.Name, TaxID: entry.Owner.TaxIdNumber, Key: entry.Key},
		MsgID:     env.FIToFICstmrCdtTrf.GrpHdr.MsgId,
		History:   []Transition{{State: StatePending, At: now}},
		CreatedAt: now,
		UpdatedAt: now,
	}

	err = store.Transact(ctx, s.store.client, func(ctx context.Context) error {
		if _, err := s.store.idem.InsertOne(ctx, IdempotencyKey{
			ID: bson.NewObjectID(), Scope: scope, Key: idemKey, RequestHash: hash, EndToEndID: e2e, CreatedAt: now,
		}); err != nil {
			return err
		}
		res, err := s.store.accounts.UpdateOne(ctx,
			bson.D{{Key: "_id", Value: acc.ID}, {Key: "status", Value: AccountActive},
				{Key: "balance.available", Value: bson.D{{Key: "$gte", Value: req.Amount}}}},
			bson.D{{Key: "$inc", Value: bson.D{{Key: "balance.available", Value: -req.Amount}, {Key: "balance.held", Value: req.Amount}}},
				{Key: "$set", Value: bson.D{{Key: "updatedAt", Value: now}}}})
		if err != nil {
			return err
		}
		if res.MatchedCount == 0 {
			return errInsufficientFunds
		}
		if _, err := s.store.ledger.InsertOne(ctx, LedgerEntry{
			ID: bson.NewObjectID(), AccountID: acc.ID, EndToEndID: e2e, Kind: EntryHold,
			AvailableDelta: -req.Amount, HeldDelta: req.Amount, CreatedAt: now,
		}); err != nil {
			return err
		}
		if _, err := s.store.payments.InsertOne(ctx, p); err != nil {
			return err
		}
		return outbox.Add(ctx, s.store.outbox, broker.QueueSPIOut, env.AppHdr.BizMsgIdr, body)
	})
	switch {
	case errors.Is(err, errInsufficientFunds):
		return Payment{}, false, newError(http.StatusUnprocessableEntity, "INSUFFICIENT_FUNDS", "saldo insuficiente")
	case mongo.IsDuplicateKeyError(err):
		// Outra requisição com a mesma Idempotency-Key venceu a corrida.
		prev, found, rerr := s.replay(ctx, scope, idemKey, hash)
		if rerr != nil || !found {
			return Payment{}, false, errors.Join(err, rerr)
		}
		return prev, false, nil
	case err != nil:
		return Payment{}, false, err
	}
	slog.InfoContext(ctx, "pix iniciado", "end_to_end_id", e2e, "creditor_ispb", p.Creditor.ISPB, "amount", p.Amount.String())
	return p, true, nil
}

// replay devolve o pagamento já criado para a Idempotency-Key, se houver.
func (s *Service) replay(ctx context.Context, scope, key, hash string) (Payment, bool, error) {
	var ik IdempotencyKey
	err := s.store.idem.FindOne(ctx, bson.D{{Key: "scope", Value: scope}, {Key: "key", Value: key}}).Decode(&ik)
	switch {
	case errors.Is(err, mongo.ErrNoDocuments):
		return Payment{}, false, nil
	case err != nil:
		return Payment{}, false, err
	case ik.RequestHash != hash:
		return Payment{}, false, newError(http.StatusUnprocessableEntity, "IDEMPOTENCY_KEY_REUSED", "Idempotency-Key já usada com outro pedido")
	}
	p, err := s.store.FindTx(ctx, ik.EndToEndID)
	if err != nil {
		return Payment{}, false, fmt.Errorf("pagamento %s da chave de idempotência: %w", ik.EndToEndID, err)
	}
	return p, true, nil
}

func requestHash(req any) string {
	b, _ := json.Marshal(req)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// mapDictError traduz falhas do gateway do DICT em erros da API.
func mapDictError(err error) error {
	var apiErr *clients.APIError
	switch {
	case errors.Is(err, clients.ErrDictUnavailable):
		return newError(http.StatusServiceUnavailable, "DICT_UNAVAILABLE", "diretório de chaves indisponível; tente novamente")
	case errors.As(err, &apiErr) && apiErr.Status == http.StatusNotFound:
		return newError(http.StatusUnprocessableEntity, "KEY_NOT_FOUND", "chave Pix não encontrada")
	case errors.As(err, &apiErr) && apiErr.Status == http.StatusTooManyRequests:
		return newError(http.StatusTooManyRequests, "DICT_RATE_LIMITED", "limite de consultas de chave atingido")
	default:
		return newError(http.StatusBadGateway, "DICT_ERROR", "falha ao consultar o diretório de chaves: %v", err)
	}
}

// GetPayment devolve o Pix pelo EndToEndId.
func (s *Service) GetPayment(ctx context.Context, endToEndID string) (Payment, error) {
	p, err := s.store.FindPayment(ctx, endToEndID)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return Payment{}, newError(http.StatusNotFound, "PAYMENT_NOT_FOUND", "pagamento não encontrado")
	}
	return p, err
}

// GetAccount devolve a conta com os saldos.
func (s *Service) GetAccount(ctx context.Context, branch, number string) (Account, error) {
	acc, err := s.store.FindAccount(ctx, branch, number)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return Account{}, newError(http.StatusNotFound, "ACCOUNT_NOT_FOUND", "conta não encontrada")
	}
	return acc, err
}
