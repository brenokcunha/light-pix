// Package bacen simula a visão do Banco Central sobre o participante: a conta
// PI (reservas usadas na liquidação do Pix), as transferências STR ↔ PI e o
// extrato de lançamentos, base da reconciliação diária do PSP.
package bacen

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/brenokcunha/light-pix/internal/httpx"
	"github.com/brenokcunha/light-pix/internal/pix"
	"github.com/brenokcunha/light-pix/internal/store"
)

// Tipos de lançamento. O valor do lançamento tem sinal (débito negativo), então
// o saldo é sempre a soma do extrato.
const (
	KindSettlementDebit  = "SETTLEMENT_DEBIT"
	KindSettlementCredit = "SETTLEMENT_CREDIT"
	KindSTRIn            = "STR_IN"  // aporte: STR → PI
	KindSTROut           = "STR_OUT" // retirada: PI → STR
)

// Códigos de erro da API.
const (
	CodeInsufficientFunds = "AM04"
	CodeUnknownAccount    = "UNKNOWN_ACCOUNT"
	CodeConflict          = "CONFLICT"
	CodeInvalidRequest    = "INVALID_REQUEST"
)

var (
	errInsufficientFunds = errors.New("saldo insuficiente na conta PI")
	errUnknownAccount    = errors.New("conta PI inexistente")
	errConflict          = errors.New("referência já usada com outros dados")
)

// Account é a conta PI de um participante.
type Account struct {
	ISPB      string     `bson:"_id" json:"ispb"`
	Balance   pix.Amount `bson:"balance" json:"balance"`
	UpdatedAt time.Time  `bson:"updatedAt" json:"updatedAt"`
}

// Entry é um lançamento do extrato (append-only).
type Entry struct {
	ID           bson.ObjectID `bson:"_id" json:"id"`
	ISPB         string        `bson:"ispb" json:"ispb"`
	Kind         string        `bson:"kind" json:"kind"`
	Amount       pix.Amount    `bson:"amount" json:"amount"`
	Reference    string        `bson:"reference" json:"reference"` // EndToEndId ou id da transferência STR
	Counterparty string        `bson:"counterparty,omitempty" json:"counterparty,omitempty"`
	BalanceAfter pix.Amount    `bson:"balanceAfter" json:"balanceAfter"`
	CreatedAt    time.Time     `bson:"createdAt" json:"createdAt"`
}

// SettlementRequest é o pedido de liquidação feito pelo SPI.
type SettlementRequest struct {
	EndToEndId   string     `json:"endToEndId"`
	DebtorISPB   string     `json:"debtorIspb"`
	CreditorISPB string     `json:"creditorIspb"`
	Amount       pix.Amount `json:"amount"`
}

// SettlementResult confirma a liquidação.
type SettlementResult struct {
	EndToEndId string    `json:"endToEndId"`
	SettledAt  time.Time `json:"settledAt"`
}

// STRTransfer é um aporte (IN) ou retirada (OUT) da conta PI.
type STRTransfer struct {
	TransferId string     `json:"transferId"`
	Direction  string     `json:"direction"`
	Amount     pix.Amount `json:"amount"`
}

// Server guarda as contas PI no MongoDB.
type Server struct {
	client      *mongo.Client
	accounts    *mongo.Collection
	entries     *mongo.Collection
	settlements metric.Int64Counter
	iga         *mongo.Collection
	igaM        igaMetrics
}

// New prepara as coleções e as métricas.
func New(ctx context.Context, client *mongo.Client, dbName string) (*Server, error) {
	db := client.Database(dbName)
	s := &Server{client: client, accounts: db.Collection("pi_accounts"), entries: db.Collection("pi_entries")}

	_, err := s.entries.Indexes().CreateMany(ctx, []mongo.IndexModel{
		// Idempotência: uma liquidação ou transferência gera no máximo um
		// lançamento por conta.
		{Keys: bson.D{{Key: "ispb", Value: 1}, {Key: "reference", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "ispb", Value: 1}, {Key: "createdAt", Value: 1}}},
	})
	if err != nil {
		return nil, fmt.Errorf("bacen: índices: %w", err)
	}

	if err := s.initIGA(ctx, db); err != nil {
		return nil, err
	}
	meter := otel.Meter("github.com/brenokcunha/light-pix/internal/sim/bacen")
	if s.settlements, err = meter.Int64Counter("bacen_sim.settlements",
		metric.WithDescription("Pedidos de liquidação por resultado")); err != nil {
		return nil, err
	}
	_, err = meter.Float64ObservableGauge("bacen_sim.pi_account.balance",
		metric.WithDescription("Saldo da conta PI em reais"),
		metric.WithUnit("{BRL}"),
		metric.WithFloat64Callback(s.observeBalances))
	return s, err
}

// Seed cria as contas PI que ainda não existem, com o saldo inicial lançado
// como aporte STR (assim o saldo continua igual à soma do extrato).
func (s *Server) Seed(ctx context.Context, initial map[string]pix.Amount) error {
	for ispb, amount := range initial {
		now := store.Now()
		err := store.Transact(ctx, s.client, func(ctx context.Context) error {
			if _, err := s.accounts.InsertOne(ctx, Account{ISPB: ispb, Balance: amount, UpdatedAt: now}); err != nil {
				return err
			}
			_, err := s.entries.InsertOne(ctx, Entry{ID: bson.NewObjectID(), ISPB: ispb, Kind: KindSTRIn,
				Amount: amount, Reference: "seed", BalanceAfter: amount, CreatedAt: now})
			return err
		})
		switch {
		case mongo.IsDuplicateKeyError(err):
			continue
		case err != nil:
			return fmt.Errorf("bacen: seed %s: %w", ispb, err)
		}
		slog.InfoContext(ctx, "conta PI criada", "ispb", ispb, "balance", amount.String())
	}
	return nil
}

// Routes registra a API.
func (s *Server) Routes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/settlements", s.handleSettle)
	mux.HandleFunc("GET /api/v1/pi-accounts/{ispb}", s.handleAccount)
	mux.HandleFunc("GET /api/v1/pi-accounts/{ispb}/statement", s.handleStatement)
	mux.HandleFunc("POST /api/v1/pi-accounts/{ispb}/str-transfers", s.handleSTRTransfer)
	mux.HandleFunc("POST /api/v1/iga/spi-records", s.handleIGASPI)
	mux.HandleFunc("POST /api/v1/iga/reports", s.handleIGAReports)
	mux.HandleFunc("GET /api/v1/iga/{ispb}", s.handleIGA)
}

func (s *Server) handleSettle(w http.ResponseWriter, r *http.Request) {
	var req SettlementRequest
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, CodeInvalidRequest, err.Error())
		return
	}
	if _, err := pix.ParseEndToEndID(req.EndToEndId); err != nil || req.Amount <= 0 ||
		!pix.ValidISPB(req.DebtorISPB) || !pix.ValidISPB(req.CreditorISPB) || req.DebtorISPB == req.CreditorISPB {
		httpx.WriteError(w, http.StatusBadRequest, CodeInvalidRequest, "endToEndId, ISPBs distintos e valor positivo são obrigatórios")
		return
	}

	res, replay, err := s.settle(r.Context(), req)
	result := "settled"
	switch {
	case errors.Is(err, errInsufficientFunds):
		result = "insufficient_funds"
		httpx.WriteError(w, http.StatusUnprocessableEntity, CodeInsufficientFunds, err.Error())
	case errors.Is(err, errUnknownAccount):
		result = "unknown_account"
		httpx.WriteError(w, http.StatusNotFound, CodeUnknownAccount, err.Error())
	case errors.Is(err, errConflict):
		result = "conflict"
		httpx.WriteError(w, http.StatusConflict, CodeConflict, err.Error())
	case err != nil:
		result = "error"
		slog.ErrorContext(r.Context(), "falha na liquidação", "end_to_end_id", req.EndToEndId, "err", err)
		httpx.WriteError(w, http.StatusInternalServerError, "INTERNAL", "falha na liquidação")
	case replay:
		result = "replay"
		httpx.WriteJSON(w, http.StatusOK, res)
	default:
		httpx.WriteJSON(w, http.StatusCreated, res)
	}
	s.settlements.Add(r.Context(), 1, metric.WithAttributes(attribute.String("result", result)))
}

// settle debita a conta PI do pagador e credita a do recebedor numa única
// transação. Repetir o pedido devolve a liquidação original (replay).
func (s *Server) settle(ctx context.Context, req SettlementRequest) (res SettlementResult, replay bool, err error) {
	res.EndToEndId = req.EndToEndId
	err = store.Transact(ctx, s.client, func(ctx context.Context) error {
		replay = false
		var prev Entry
		err := s.entries.FindOne(ctx, bson.D{{Key: "ispb", Value: req.DebtorISPB}, {Key: "reference", Value: req.EndToEndId}}).Decode(&prev)
		if err == nil {
			if prev.Kind != KindSettlementDebit || prev.Amount != -req.Amount || prev.Counterparty != req.CreditorISPB {
				return errConflict
			}
			replay, res.SettledAt = true, prev.CreatedAt
			return nil
		}
		if !errors.Is(err, mongo.ErrNoDocuments) {
			return err
		}

		now := store.Now()
		debtor, err := s.move(ctx, req.DebtorISPB, -req.Amount, now)
		if err != nil {
			return err
		}
		creditor, err := s.move(ctx, req.CreditorISPB, req.Amount, now)
		if err != nil {
			return err
		}
		_, err = s.entries.InsertMany(ctx, []Entry{
			{ID: bson.NewObjectID(), ISPB: req.DebtorISPB, Kind: KindSettlementDebit, Amount: -req.Amount,
				Reference: req.EndToEndId, Counterparty: req.CreditorISPB, BalanceAfter: debtor.Balance, CreatedAt: now},
			{ID: bson.NewObjectID(), ISPB: req.CreditorISPB, Kind: KindSettlementCredit, Amount: req.Amount,
				Reference: req.EndToEndId, Counterparty: req.DebtorISPB, BalanceAfter: creditor.Balance, CreatedAt: now},
		})
		res.SettledAt = now
		return err
	})
	return res, replay, err
}

// move soma delta ao saldo. Débitos só passam se houver saldo: a condição
// fica no filtro do update, então não há janela entre ler e gravar.
func (s *Server) move(ctx context.Context, ispb string, delta pix.Amount, now time.Time) (Account, error) {
	filter := bson.D{{Key: "_id", Value: ispb}}
	if delta < 0 {
		filter = append(filter, bson.E{Key: "balance", Value: bson.D{{Key: "$gte", Value: -delta}}})
	}
	update := bson.D{
		{Key: "$inc", Value: bson.D{{Key: "balance", Value: delta}}},
		{Key: "$set", Value: bson.D{{Key: "updatedAt", Value: now}}},
	}
	var acc Account
	err := s.accounts.FindOneAndUpdate(ctx, filter, update,
		options.FindOneAndUpdate().SetReturnDocument(options.After)).Decode(&acc)
	if !errors.Is(err, mongo.ErrNoDocuments) {
		return acc, err
	}
	n, err := s.accounts.CountDocuments(ctx, bson.D{{Key: "_id", Value: ispb}})
	switch {
	case err != nil:
		return acc, err
	case n == 0:
		return acc, fmt.Errorf("%w: %s", errUnknownAccount, ispb)
	default:
		return acc, errInsufficientFunds
	}
}

func (s *Server) handleAccount(w http.ResponseWriter, r *http.Request) {
	var acc Account
	err := s.accounts.FindOne(r.Context(), bson.D{{Key: "_id", Value: r.PathValue("ispb")}}).Decode(&acc)
	switch {
	case errors.Is(err, mongo.ErrNoDocuments):
		httpx.WriteError(w, http.StatusNotFound, CodeUnknownAccount, errUnknownAccount.Error())
	case err != nil:
		httpx.WriteError(w, http.StatusInternalServerError, "INTERNAL", err.Error())
	default:
		httpx.WriteJSON(w, http.StatusOK, acc)
	}
}

// Statement é uma página do extrato.
type Statement struct {
	ISPB    string  `json:"ispb"`
	Entries []Entry `json:"entries"`
	Next    string  `json:"next,omitempty"` // cursor para a próxima página (?after=)
}

// handleStatement lista lançamentos em [from, to), em ordem de criação.
func (s *Server) handleStatement(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	now := store.Now()
	from, err1 := parseTime(q.Get("from"), now.Add(-24*time.Hour))
	to, err2 := parseTime(q.Get("to"), now.Add(time.Second))
	limit, err3 := strconv.Atoi(cmp.Or(q.Get("limit"), "1000"))
	if err := errors.Join(err1, err2, err3); err != nil || limit < 1 || limit > 5000 {
		httpx.WriteError(w, http.StatusBadRequest, CodeInvalidRequest, "from/to em RFC 3339 e limit entre 1 e 5000")
		return
	}
	filter := bson.D{
		{Key: "ispb", Value: r.PathValue("ispb")},
		{Key: "createdAt", Value: bson.D{{Key: "$gte", Value: from}, {Key: "$lt", Value: to}}},
	}
	if after := q.Get("after"); after != "" {
		id, err := bson.ObjectIDFromHex(after)
		if err != nil {
			httpx.WriteError(w, http.StatusBadRequest, CodeInvalidRequest, "cursor inválido")
			return
		}
		filter = append(filter, bson.E{Key: "_id", Value: bson.D{{Key: "$gt", Value: id}}})
	}
	cur, err := s.entries.Find(r.Context(), filter,
		options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetLimit(int64(limit)+1))
	if err != nil {
		httpx.WriteError(w, http.StatusInternalServerError, "INTERNAL", err.Error())
		return
	}
	st := Statement{ISPB: r.PathValue("ispb"), Entries: []Entry{}}
	if err := cur.All(r.Context(), &st.Entries); err != nil {
		httpx.WriteError(w, http.StatusInternalServerError, "INTERNAL", err.Error())
		return
	}
	if len(st.Entries) > limit {
		st.Entries = st.Entries[:limit]
		st.Next = st.Entries[limit-1].ID.Hex()
	}
	httpx.WriteJSON(w, http.StatusOK, st)
}

func (s *Server) handleSTRTransfer(w http.ResponseWriter, r *http.Request) {
	var req STRTransfer
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, CodeInvalidRequest, err.Error())
		return
	}
	kind, sign := KindSTRIn, pix.Amount(1)
	switch req.Direction {
	case "IN":
	case "OUT":
		kind, sign = KindSTROut, -1
	default:
		req.Amount = 0 // força a validação abaixo
	}
	if req.TransferId == "" || req.Amount <= 0 {
		httpx.WriteError(w, http.StatusBadRequest, CodeInvalidRequest, "transferId, direction (IN|OUT) e valor positivo são obrigatórios")
		return
	}

	ispb := r.PathValue("ispb")
	var entry Entry
	replay := false
	err := store.Transact(r.Context(), s.client, func(ctx context.Context) error {
		replay = false
		err := s.entries.FindOne(ctx, bson.D{{Key: "ispb", Value: ispb}, {Key: "reference", Value: req.TransferId}}).Decode(&entry)
		if err == nil {
			if entry.Kind != kind || entry.Amount != sign*req.Amount {
				return errConflict
			}
			replay = true
			return nil
		}
		if !errors.Is(err, mongo.ErrNoDocuments) {
			return err
		}
		now := store.Now()
		acc, err := s.move(ctx, ispb, sign*req.Amount, now)
		if err != nil {
			return err
		}
		entry = Entry{ID: bson.NewObjectID(), ISPB: ispb, Kind: kind, Amount: sign * req.Amount,
			Reference: req.TransferId, BalanceAfter: acc.Balance, CreatedAt: now}
		_, err = s.entries.InsertOne(ctx, entry)
		return err
	})
	switch {
	case errors.Is(err, errInsufficientFunds):
		httpx.WriteError(w, http.StatusUnprocessableEntity, CodeInsufficientFunds, err.Error())
	case errors.Is(err, errUnknownAccount):
		httpx.WriteError(w, http.StatusNotFound, CodeUnknownAccount, err.Error())
	case errors.Is(err, errConflict):
		httpx.WriteError(w, http.StatusConflict, CodeConflict, err.Error())
	case err != nil:
		httpx.WriteError(w, http.StatusInternalServerError, "INTERNAL", err.Error())
	case replay:
		httpx.WriteJSON(w, http.StatusOK, entry)
	default:
		httpx.WriteJSON(w, http.StatusCreated, entry)
	}
}

func (s *Server) observeBalances(ctx context.Context, o metric.Float64Observer) error {
	cur, err := s.accounts.Find(ctx, bson.D{})
	if err != nil {
		return err
	}
	var accs []Account
	if err := cur.All(ctx, &accs); err != nil {
		return err
	}
	for _, a := range accs {
		o.Observe(float64(a.Balance)/100, metric.WithAttributes(attribute.String("ispb", a.ISPB)))
	}
	return nil
}

func parseTime(s string, def time.Time) (time.Time, error) {
	if s == "" {
		return def, nil
	}
	return time.Parse(time.RFC3339, s)
}
