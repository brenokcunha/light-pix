package psp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/brenokcunha/light-pix/internal/clients"
	"github.com/brenokcunha/light-pix/internal/httpx"
	"github.com/brenokcunha/light-pix/internal/pix"
	"github.com/brenokcunha/light-pix/internal/sim/dict"
)

// KeyRegistry gerencia vínculos de chave no DICT.
type KeyRegistry interface {
	Register(ctx context.Context, e dict.Entry) (dict.Entry, error)
	Remove(ctx context.Context, key string) error
}

// API é a payment-api: a interface HTTP do PSP para o cliente final.
type API struct {
	svc  *Service
	keys KeyRegistry
}

// NewAPI cria a API sobre o serviço e o registro de chaves.
func NewAPI(svc *Service, keys KeyRegistry) *API { return &API{svc: svc, keys: keys} }

// Routes registra as rotas.
func (a *API) Routes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/pix/payments", a.handleCreatePayment)
	mux.HandleFunc("GET /v1/pix/payments/{endToEndId}", a.handleGetPayment)
	mux.HandleFunc("POST /v1/pix/payments/{endToEndId}/returns", a.handleCreateReturn)
	mux.HandleFunc("GET /v1/pix/payments/{endToEndId}/returns", a.handleListReturns)
	mux.HandleFunc("GET /v1/pix/returns/{returnId}", a.handleGetReturn)
	mux.HandleFunc("POST /v1/pix/payments/{endToEndId}/infraction-reports", a.handleReportInfraction)
	mux.HandleFunc("GET /v1/infraction-reports", a.handleListInfractions)
	mux.HandleFunc("GET /v1/infraction-reports/{id}", a.handleGetInfraction)
	mux.HandleFunc("POST /v1/infraction-reports/{id}/cancel", a.handleCancelInfraction)
	mux.HandleFunc("POST /v1/infraction-reports/{id}/decision", a.handleDecideInfraction)
	mux.HandleFunc("GET /v1/accounts/{branch}/{number}", a.handleGetAccount)
	mux.HandleFunc("POST /v1/keys", a.handleRegisterKey)
	mux.HandleFunc("DELETE /v1/keys/{key}", a.handleRemoveKey)
}

func (a *API) handleCreatePayment(w http.ResponseWriter, r *http.Request) {
	var req CreateRequest
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
		return
	}
	p, created, err := a.svc.CreatePayment(r.Context(), r.Header.Get("Idempotency-Key"), req)
	if err != nil {
		writeServiceError(w, r, err)
		return
	}
	w.Header().Set("Location", "/v1/pix/payments/"+p.EndToEndID)
	if !created {
		w.Header().Set("Idempotent-Replayed", "true")
		httpx.WriteJSON(w, http.StatusOK, p)
		return
	}
	httpx.WriteJSON(w, http.StatusCreated, p)
}

func (a *API) handleCreateReturn(w http.ResponseWriter, r *http.Request) {
	var req ReturnRequest
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
		return
	}
	ret, created, err := a.svc.CreateReturn(r.Context(), r.PathValue("endToEndId"), r.Header.Get("Idempotency-Key"), req)
	if err != nil {
		writeServiceError(w, r, err)
		return
	}
	w.Header().Set("Location", "/v1/pix/returns/"+ret.EndToEndID)
	if !created {
		w.Header().Set("Idempotent-Replayed", "true")
		httpx.WriteJSON(w, http.StatusOK, ret)
		return
	}
	httpx.WriteJSON(w, http.StatusCreated, ret)
}

func (a *API) handleGetReturn(w http.ResponseWriter, r *http.Request) {
	ret, err := a.svc.GetReturn(r.Context(), r.PathValue("returnId"))
	if err != nil {
		writeServiceError(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, ret)
}

func (a *API) handleListReturns(w http.ResponseWriter, r *http.Request) {
	list, err := a.svc.ListReturns(r.Context(), r.PathValue("endToEndId"))
	if err != nil {
		writeServiceError(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, list)
}

func (a *API) handleReportInfraction(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Reason  string `json:"reason"`
		Details string `json:"details"`
	}
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
		return
	}
	inf, err := a.svc.ReportInfraction(r.Context(), r.PathValue("endToEndId"), req.Reason, req.Details)
	if err != nil {
		writeServiceError(w, r, err)
		return
	}
	w.Header().Set("Location", "/v1/infraction-reports/"+inf.ID)
	httpx.WriteJSON(w, http.StatusCreated, inf)
}

func (a *API) handleListInfractions(w http.ResponseWriter, r *http.Request) {
	list, err := a.svc.ListInfractions(r.Context(), r.URL.Query().Get("role"), r.URL.Query().Get("state"))
	if err != nil {
		writeServiceError(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, list)
}

func (a *API) handleGetInfraction(w http.ResponseWriter, r *http.Request) {
	inf, err := a.svc.GetInfraction(r.Context(), r.PathValue("id"))
	if err != nil {
		writeServiceError(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, inf)
}

func (a *API) handleCancelInfraction(w http.ResponseWriter, r *http.Request) {
	inf, err := a.svc.CancelInfraction(r.Context(), r.PathValue("id"))
	if err != nil {
		writeServiceError(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, inf)
}

// handleDecideInfraction é a análise do PSP recebedor: result AGREE devolve o
// valor bloqueado (devolução especial); DISAGREE o libera.
func (a *API) handleDecideInfraction(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Result string `json:"result"`
		Note   string `json:"note"`
	}
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
		return
	}
	if req.Result != "AGREE" && req.Result != "DISAGREE" {
		httpx.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST", "result deve ser AGREE ou DISAGREE")
		return
	}
	inf, err := a.svc.DecideInfraction(r.Context(), r.PathValue("id"), req.Result == "AGREE", req.Note)
	if err != nil {
		writeServiceError(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, inf)
}

func (a *API) handleGetPayment(w http.ResponseWriter, r *http.Request) {
	p, err := a.svc.GetPayment(r.Context(), r.PathValue("endToEndId"))
	if err != nil {
		writeServiceError(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, p)
}

func (a *API) handleGetAccount(w http.ResponseWriter, r *http.Request) {
	acc, err := a.svc.GetAccount(r.Context(), r.PathValue("branch"), r.PathValue("number"))
	if err != nil {
		writeServiceError(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, acc)
}

// registerKeyRequest vincula uma chave a uma conta do PSP.
type registerKeyRequest struct {
	KeyType pix.KeyType `json:"keyType"`
	Key     string      `json:"key,omitempty"` // vazio gera uma chave aleatória (EVP)
	Account AccountRef  `json:"account"`
}

func (a *API) handleRegisterKey(w http.ResponseWriter, r *http.Request) {
	var req registerKeyRequest
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
		return
	}
	acc, err := a.svc.GetAccount(r.Context(), req.Account.Branch, req.Account.Number)
	if err != nil {
		writeServiceError(w, r, err)
		return
	}
	entry, err := a.keys.Register(r.Context(), dict.Entry{
		Key: req.Key, KeyType: req.KeyType,
		Account: dict.Account{Participant: a.svc.ispb, Branch: acc.Branch, AccountNumber: acc.Number,
			AccountType: acc.Type, OpeningDate: acc.CreatedAt},
		Owner: dict.Owner{Type: acc.Holder.Type, TaxIdNumber: acc.Holder.TaxID, Name: acc.Holder.Name},
	})
	if err != nil {
		writeServiceError(w, r, mapDictPassthrough(err))
		return
	}
	httpx.WriteJSON(w, http.StatusCreated, entry)
}

func (a *API) handleRemoveKey(w http.ResponseWriter, r *http.Request) {
	if err := a.keys.Remove(r.Context(), r.PathValue("key")); err != nil {
		writeServiceError(w, r, mapDictPassthrough(err))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// mapDictPassthrough repassa a resposta de erro do DICT ao cliente.
func mapDictPassthrough(err error) error {
	var apiErr *clients.APIError
	if errors.As(err, &apiErr) {
		return &Error{Status: apiErr.Status, Code: apiErr.Code, Message: apiErr.Message}
	}
	return mapDictError(fmt.Errorf("%w: %w", clients.ErrDictUnavailable, err))
}

// writeServiceError responde um *Error de negócio com seu status; qualquer
// outro erro é falha interna, registrada sem detalhes para o cliente.
func writeServiceError(w http.ResponseWriter, r *http.Request, err error) {
	var apiErr *Error
	if !errors.As(err, &apiErr) {
		slog.ErrorContext(r.Context(), "erro interno", "route", r.Pattern, "err", err)
		httpx.WriteError(w, http.StatusInternalServerError, "INTERNAL", "erro interno")
		return
	}
	if apiErr.RetryAfter > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(int(apiErr.RetryAfter.Seconds())+1))
	}
	httpx.WriteError(w, apiErr.Status, apiErr.Code, apiErr.Message)
}
