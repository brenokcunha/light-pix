// Package dict simula o Diretório de Identificadores de Contas
// Transacionais: vínculos chave Pix → conta, com consulta protegida por rate
// limit por usuário pagador (anti-varredura). Portabilidade, reivindicação e
// MED ficam fora desta fase.
package dict

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/brenokcunha/light-pix/internal/httpx"
	"github.com/brenokcunha/light-pix/internal/pix"
	"github.com/brenokcunha/light-pix/internal/store"
)

// Tipos de titular.
const (
	NaturalPerson = "NATURAL_PERSON"
	LegalPerson   = "LEGAL_PERSON"
)

// Limite de chaves por conta, como no regulamento: 5 para pessoa física e
// 20 para pessoa jurídica.
var keysPerAccount = map[string]int64{NaturalPerson: 5, LegalPerson: 20}

var accountTypes = []string{"CACC", "SVGS", "SLRY", "TRAN"}

// Entry é o vínculo de uma chave a uma conta.
type Entry struct {
	Key              string      `bson:"_id" json:"Key"`
	KeyType          pix.KeyType `bson:"keyType" json:"KeyType"`
	Account          Account     `bson:"account" json:"Account"`
	Owner            Owner       `bson:"owner" json:"Owner"`
	CreationDate     time.Time   `bson:"creationDate" json:"CreationDate"`
	KeyOwnershipDate time.Time   `bson:"keyOwnershipDate" json:"KeyOwnershipDate"`
}

// Account é a conta transacional vinculada.
type Account struct {
	Participant   string    `bson:"participant" json:"Participant"` // ISPB
	Branch        string    `bson:"branch" json:"Branch"`
	AccountNumber string    `bson:"accountNumber" json:"AccountNumber"`
	AccountType   string    `bson:"accountType" json:"AccountType"`
	OpeningDate   time.Time `bson:"openingDate" json:"OpeningDate"`
}

// Owner é o titular da conta.
type Owner struct {
	Type        string `bson:"type" json:"Type"`
	TaxIdNumber string `bson:"taxIdNumber" json:"TaxIdNumber"`
	Name        string `bson:"name" json:"Name"`
	TradeName   string `bson:"tradeName,omitempty" json:"TradeName,omitempty"`
}

// Config parametriza o rate limit da consulta.
type Config struct {
	BucketCapacity  float64 // fichas por usuário pagador
	RefillPerMinute float64 // reposição
	NotFoundCost    float64 // custo de uma consulta a chave inexistente (sucesso custa 1)
	// AnalysisDeadline é o prazo do recebedor para analisar um relato de
	// infração do MED (padrão: 72h).
	AnalysisDeadline time.Duration
}

// Server é o simulador do DICT.
type Server struct {
	entries *mongo.Collection
	limiter *limiter
	cost404 float64
	lookups metric.Int64Counter

	// MED: relatos de infração.
	reports          *mongo.Collection
	analysisDeadline time.Duration
	infractions      metric.Int64Counter
}

// New prepara coleção, índices e métricas.
func New(ctx context.Context, client *mongo.Client, dbName string, cfg Config) (*Server, error) {
	s := &Server{
		entries: client.Database(dbName).Collection("entries"),
		limiter: newLimiter(cfg.BucketCapacity, cfg.RefillPerMinute/60),
		cost404: cfg.NotFoundCost,
	}
	_, err := s.entries.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{
		{Key: "account.participant", Value: 1}, {Key: "account.branch", Value: 1}, {Key: "account.accountNumber", Value: 1},
	}})
	if err != nil {
		return nil, fmt.Errorf("dict: índices: %w", err)
	}
	s.lookups, err = otel.Meter("github.com/brenokcunha/light-pix/internal/sim/dict").Int64Counter("dict_sim.lookups",
		metric.WithDescription("Consultas de chave por resultado"))
	if err != nil {
		return nil, err
	}
	return s, s.initMED(ctx, client.Database(dbName), cfg.AnalysisDeadline)
}

// Seed cria os vínculos que ainda não existem.
func (s *Server) Seed(ctx context.Context, entries []Entry) error {
	now := store.Now()
	for _, e := range entries {
		e.Key = pix.NormalizeKey(e.KeyType, e.Key)
		if err := validate(e); err != nil {
			return fmt.Errorf("dict: seed %s: %w", e.Key, err)
		}
		e.CreationDate, e.KeyOwnershipDate = now, now
		_, err := s.entries.InsertOne(ctx, e)
		if err != nil && !mongo.IsDuplicateKeyError(err) {
			return fmt.Errorf("dict: seed %s: %w", e.Key, err)
		}
	}
	return nil
}

// Routes registra a API.
func (s *Server) Routes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/entries", s.handleCreate)
	mux.HandleFunc("GET /api/v1/entries/{key}", s.handleLookup)
	mux.HandleFunc("DELETE /api/v1/entries/{key}", s.handleDelete)
	mux.HandleFunc("GET /admin/entries", s.handleAdminList)
	s.routesMED(mux)
}

func (s *Server) handleCreate(w http.ResponseWriter, r *http.Request) {
	var e Entry
	if err := httpx.DecodeJSON(w, r, &e); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
		return
	}
	if e.Account.Participant != r.Header.Get(pix.HeaderRequestingParticipant) {
		httpx.WriteError(w, http.StatusForbidden, "FORBIDDEN", "participante só registra chaves das próprias contas")
		return
	}
	if e.KeyType == pix.KeyEVP && e.Key == "" {
		e.Key = pix.NewEVP()
	}
	e.Key = pix.NormalizeKey(e.KeyType, e.Key)
	if err := validate(e); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
		return
	}

	ctx := r.Context()
	n, err := s.entries.CountDocuments(ctx, bson.D{
		{Key: "account.participant", Value: e.Account.Participant},
		{Key: "account.branch", Value: e.Account.Branch},
		{Key: "account.accountNumber", Value: e.Account.AccountNumber},
	})
	if err != nil {
		httpx.WriteInternal(w, r, err)
		return
	}
	if n >= keysPerAccount[e.Owner.Type] {
		httpx.WriteError(w, http.StatusUnprocessableEntity, "ENTRY_LIMIT_EXCEEDED",
			fmt.Sprintf("conta já tem %d chaves", n))
		return
	}

	now := store.Now()
	e.CreationDate, e.KeyOwnershipDate = now, now
	_, err = s.entries.InsertOne(ctx, e)
	switch {
	case mongo.IsDuplicateKeyError(err):
		// No DICT real, isso abriria portabilidade ou reivindicação de posse.
		httpx.WriteError(w, http.StatusConflict, "ENTRY_ALREADY_EXISTS", "chave já vinculada")
	case err != nil:
		httpx.WriteInternal(w, r, err)
	default:
		slog.InfoContext(ctx, "chave registrada", "key_type", e.KeyType, "pix_key", e.Key, "participant", e.Account.Participant)
		httpx.WriteJSON(w, http.StatusCreated, e)
	}
}

// handleLookup resolve a chave. Cada usuário pagador tem um balde de fichas:
// consulta bem-sucedida custa 1 e chave inexistente custa mais, o que torna
// a varredura de chaves cara.
func (s *Server) handleLookup(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	participant := r.Header.Get(pix.HeaderRequestingParticipant)
	payer := r.Header.Get(pix.HeaderPayerID)
	if !pix.ValidISPB(participant) || (!pix.ValidCPF(payer) && !pix.ValidCNPJ(payer)) {
		httpx.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST",
			"headers "+pix.HeaderRequestingParticipant+" (ISPB) e "+pix.HeaderPayerID+" (CPF/CNPJ) são obrigatórios")
		return
	}
	bucket := participant + ":" + payer
	now := time.Now()
	if ok, retry := s.limiter.allow(bucket, now); !ok {
		s.count(ctx, "rate_limited")
		w.Header().Set("Retry-After", fmt.Sprint(int(retry.Seconds())+1))
		httpx.WriteError(w, http.StatusTooManyRequests, "RATE_LIMITED", "limite de consultas do usuário pagador")
		return
	}

	key := r.PathValue("key")
	if strings.Contains(key, "@") {
		key = strings.ToLower(key)
	}
	var e Entry
	err := s.entries.FindOne(ctx, bson.D{{Key: "_id", Value: key}}).Decode(&e)
	switch {
	case errors.Is(err, mongo.ErrNoDocuments):
		s.limiter.charge(bucket, s.cost404, now)
		s.count(ctx, "not_found")
		httpx.WriteError(w, http.StatusNotFound, "ENTRY_NOT_FOUND", "chave não encontrada")
	case err != nil:
		httpx.WriteInternal(w, r, err)
	default:
		s.limiter.charge(bucket, 1, now)
		s.count(ctx, "found")
		httpx.WriteJSON(w, http.StatusOK, e)
	}
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	key := r.PathValue("key")
	if strings.Contains(key, "@") {
		key = strings.ToLower(key)
	}
	res, err := s.entries.DeleteOne(ctx, bson.D{
		{Key: "_id", Value: key},
		{Key: "account.participant", Value: r.Header.Get(pix.HeaderRequestingParticipant)},
	})
	switch {
	case err != nil:
		httpx.WriteInternal(w, r, err)
	case res.DeletedCount == 0:
		// Chave inexistente ou de outro participante: a resposta é a mesma.
		httpx.WriteError(w, http.StatusNotFound, "ENTRY_NOT_FOUND", "chave não encontrada")
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}

func (s *Server) count(ctx context.Context, result string) {
	s.lookups.Add(ctx, 1, metric.WithAttributes(attribute.String("result", result)))
}

// validate confere chave, titular e conta. Chaves CPF/CNPJ precisam ser o
// documento do próprio titular.
func validate(e Entry) error {
	if err := pix.ValidateKey(e.KeyType, e.Key); err != nil {
		return err
	}
	switch e.Owner.Type {
	case NaturalPerson:
		if !pix.ValidCPF(e.Owner.TaxIdNumber) {
			return errors.New("titular pessoa física exige CPF válido")
		}
	case LegalPerson:
		if !pix.ValidCNPJ(e.Owner.TaxIdNumber) {
			return errors.New("titular pessoa jurídica exige CNPJ válido")
		}
	default:
		return fmt.Errorf("Owner.Type deve ser %s ou %s", NaturalPerson, LegalPerson)
	}
	if (e.KeyType == pix.KeyCPF || e.KeyType == pix.KeyCNPJ) && e.Key != e.Owner.TaxIdNumber {
		return errors.New("chave CPF/CNPJ deve ser o documento do titular")
	}
	if e.Owner.Name == "" {
		return errors.New("Owner.Name obrigatório")
	}
	a := e.Account
	if !pix.ValidISPB(a.Participant) || a.Branch == "" || a.AccountNumber == "" || !slices.Contains(accountTypes, a.AccountType) {
		return errors.New("Account exige Participant (ISPB), Branch, AccountNumber e AccountType (CACC, SVGS, SLRY, TRAN)")
	}
	return nil
}
