package psp

import (
	"context"
	"net/http"
	"slices"
	"strconv"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/brenokcunha/light-pix/internal/clients"
	"github.com/brenokcunha/light-pix/internal/httpx"
	"github.com/brenokcunha/light-pix/internal/sim/dict"
)

// Consultas de apoio à interface de testes: o que um cliente (conta) enxerga do
// próprio banco. Nada aqui muda estado.

// ActivityItem é um Pix ou uma devolução que passou pela conta, enviado ou recebido.
type ActivityItem struct {
	Payment
	Kind string `json:"kind"` // PIX | RETURN
}

// ListAccounts lista as contas do PSP com os saldos.
func (s *Service) ListAccounts(ctx context.Context) ([]Account, error) {
	cur, err := s.store.accounts.Find(ctx, bson.D{}, options.Find().SetSort(bson.D{{Key: "number", Value: 1}}))
	if err != nil {
		return nil, err
	}
	out := []Account{}
	return out, cur.All(ctx, &out)
}

// ListActivity devolve os Pix e devoluções da conta, do mais novo para o mais
// velho (limit entre 1 e 200).
func (s *Service) ListActivity(ctx context.Context, branch, number string, limit int) ([]ActivityItem, error) {
	acc, err := s.GetAccount(ctx, branch, number)
	if err != nil {
		return nil, err
	}
	limit = max(1, min(limit, 200))
	filter := bson.D{{Key: "$or", Value: bson.A{
		bson.D{{Key: "debtor.accountId", Value: acc.ID}},
		bson.D{{Key: "creditor.accountId", Value: acc.ID}},
	}}}
	var items []ActivityItem
	for coll, kind := range map[*mongo.Collection]string{s.store.payments: "PIX", s.store.returns: "RETURN"} {
		cur, err := coll.Find(ctx, filter, options.Find().SetSort(bson.D{{Key: "createdAt", Value: -1}}).SetLimit(int64(limit)))
		if err != nil {
			return nil, err
		}
		var batch []Payment
		if err := cur.All(ctx, &batch); err != nil {
			return nil, err
		}
		for _, p := range batch {
			items = append(items, ActivityItem{Payment: p, Kind: kind})
		}
	}
	slices.SortFunc(items, func(a, b ActivityItem) int { return b.CreatedAt.Compare(a.CreatedAt) })
	if len(items) > limit {
		items = items[:limit]
	}
	if items == nil {
		items = []ActivityItem{}
	}
	return items, nil
}

// DirectoryLister lista vínculos do diretório de chaves (só no laboratório).
type DirectoryLister interface {
	ListEntries(ctx context.Context, f clients.EntryFilter) ([]dict.Entry, error)
}

func (a *API) registerLabRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/accounts", a.handleListAccounts)
	mux.HandleFunc("GET /v1/accounts/{branch}/{number}/payments", a.handleAccountActivity)
	mux.HandleFunc("GET /v1/accounts/{branch}/{number}/keys", a.handleAccountKeys)
	mux.HandleFunc("GET /v1/directory/keys", a.handleDirectory)
}

func (a *API) handleListAccounts(w http.ResponseWriter, r *http.Request) {
	list, err := a.svc.ListAccounts(r.Context())
	if err != nil {
		writeServiceError(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"ispb": a.svc.ispb, "accounts": list})
}

func (a *API) handleAccountActivity(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit == 0 {
		limit = 30
	}
	list, err := a.svc.ListActivity(r.Context(), r.PathValue("branch"), r.PathValue("number"), limit)
	if err != nil {
		writeServiceError(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, list)
}

// handleAccountKeys lista as chaves Pix cadastradas para a conta.
func (a *API) handleAccountKeys(w http.ResponseWriter, r *http.Request) {
	acc, err := a.svc.GetAccount(r.Context(), r.PathValue("branch"), r.PathValue("number"))
	if err != nil {
		writeServiceError(w, r, err)
		return
	}
	dir, ok := a.keys.(DirectoryLister)
	if !ok {
		writeServiceError(w, r, newError(http.StatusServiceUnavailable, "DICT_UNAVAILABLE", "listagem de chaves indisponível"))
		return
	}
	entries, err := dir.ListEntries(r.Context(), clients.EntryFilter{Participant: a.svc.ispb, Branch: acc.Branch, Account: acc.Number})
	if err != nil {
		writeServiceError(w, r, mapDictPassthrough(err))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, entries)
}

// handleDirectory busca chaves no diretório pelo trecho da chave ou do nome do
// titular, para o pagador escolher o destino. É conveniência do laboratório: o
// DICT real não lista chaves.
func (a *API) handleDirectory(w http.ResponseWriter, r *http.Request) {
	dir, ok := a.keys.(DirectoryLister)
	if !ok {
		writeServiceError(w, r, newError(http.StatusServiceUnavailable, "DICT_UNAVAILABLE", "listagem de chaves indisponível"))
		return
	}
	entries, err := dir.ListEntries(r.Context(), clients.EntryFilter{Query: r.URL.Query().Get("q"), Limit: 100})
	if err != nil {
		writeServiceError(w, r, mapDictPassthrough(err))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, entries)
}
