package clients

import (
	"context"
	"net/http"
	"net/url"
	"time"

	"github.com/brenokcunha/light-pix/internal/pix"
	"github.com/brenokcunha/light-pix/internal/sim/bacen"
)

// Bacen é o cliente do bacen-sim (conta PI).
type Bacen struct{ c httpClient }

// NewBacen cria o cliente com timeout por chamada.
func NewBacen(baseURL string, timeout time.Duration) *Bacen {
	return &Bacen{c: newHTTPClient(baseURL, timeout)}
}

// Settle liquida a transação entre as contas PI (idempotente por EndToEndId).
func (b *Bacen) Settle(ctx context.Context, req bacen.SettlementRequest) (bacen.SettlementResult, error) {
	var res bacen.SettlementResult
	_, err := b.c.do(ctx, http.MethodPost, "/api/v1/settlements", nil, req, &res)
	return res, err
}

// Account consulta o saldo da conta PI.
func (b *Bacen) Account(ctx context.Context, ispb string) (bacen.Account, error) {
	var acc bacen.Account
	_, err := b.c.do(ctx, http.MethodGet, "/api/v1/pi-accounts/"+ispb, nil, nil, &acc)
	return acc, err
}

// STRTransfer faz aporte (IN) ou retirada (OUT) da conta PI.
func (b *Bacen) STRTransfer(ctx context.Context, ispb string, t bacen.STRTransfer) (bacen.Entry, error) {
	var e bacen.Entry
	_, err := b.c.do(ctx, http.MethodPost, "/api/v1/pi-accounts/"+ispb+"/str-transfers", nil, t, &e)
	return e, err
}

// Statement devolve todos os lançamentos em [from, to), seguindo a paginação.
func (b *Bacen) Statement(ctx context.Context, ispb string, from, to time.Time) ([]bacen.Entry, error) {
	var all []bacen.Entry
	after := ""
	for {
		q := url.Values{"from": {from.UTC().Format(time.RFC3339)}, "to": {to.UTC().Format(time.RFC3339)}}
		if after != "" {
			q.Set("after", after)
		}
		var page bacen.Statement
		if _, err := b.c.do(ctx, http.MethodGet, "/api/v1/pi-accounts/"+ispb+"/statement?"+q.Encode(), nil, nil, &page); err != nil {
			return nil, err
		}
		all = append(all, page.Entries...)
		if page.Next == "" {
			return all, nil
		}
		after = page.Next
	}
}

// ReportSPI envia ao bacen-sim os instantes que o SPI mediu de transações
// encerradas (base do IGA).
func (b *Bacen) ReportSPI(ctx context.Context, recs []bacen.IGASPIRecord) error {
	_, err := b.c.do(ctx, http.MethodPost, "/api/v1/iga/spi-records", nil, map[string]any{"records": recs}, nil)
	return err
}

// ReportPSP envia ao bacen-sim os instantes que o PSP ispb mediu de Pix
// encerrados (base do IGA).
func (b *Bacen) ReportPSP(ctx context.Context, ispb string, recs []bacen.IGAPSPRecord) error {
	_, err := b.c.do(ctx, http.MethodPost, "/api/v1/iga/reports",
		http.Header{pix.HeaderRequestingParticipant: {ispb}}, map[string]any{"records": recs}, nil)
	return err
}

// IGA consulta a visão do regulador sobre o PSP na janela.
func (b *Bacen) IGA(ctx context.Context, ispb string, window time.Duration) (bacen.IGAReport, error) {
	var rep bacen.IGAReport
	_, err := b.c.do(ctx, http.MethodGet, "/api/v1/iga/"+ispb+"?window="+url.QueryEscape(window.String()), nil, nil, &rep)
	return rep, err
}
