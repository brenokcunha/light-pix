package clients

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/brenokcunha/light-pix/internal/iso20022"
	"github.com/brenokcunha/light-pix/internal/pix"
	"github.com/brenokcunha/light-pix/internal/sim/spi"
)

// SPI é o cliente do spi-sim.
type SPI struct {
	c    httpClient
	pull httpClient
}

// NewSPI cria o cliente; timeout vale para o envio de mensagens.
func NewSPI(baseURL string, timeout time.Duration) *SPI {
	return &SPI{c: newHTTPClient(baseURL, timeout), pull: newUntracedHTTPClient(baseURL)}
}

// Send entrega uma mensagem ao SPI como participante ispb.
func (s *SPI) Send(ctx context.Context, ispb string, env iso20022.Envelope) error {
	_, err := s.c.do(ctx, http.MethodPost, "/api/v1/in/"+ispb+"/msgs", nil, env, nil)
	return err
}

// Pull faz um long-polling das mensagens destinadas a ispb. next é o valor de
// PI-PULL-NEXT do pull anterior (vazio no primeiro): ele confirma o lote já
// tratado. Devolve as mensagens (nenhuma, se o prazo de wait acabou) e o
// próximo valor de PI-PULL-NEXT.
func (s *SPI) Pull(ctx context.Context, ispb, next string, wait time.Duration) ([]spi.Delivery, string, error) {
	if next == "" {
		next = "/api/v1/out/" + ispb + "/stream"
	}
	sep := "?"
	if u, err := url.Parse(next); err == nil && u.RawQuery != "" {
		sep = "&"
	}
	// O cliente de pull não tem timeout próprio: o limite é o wait mais folga.
	ctx, cancel := context.WithTimeout(ctx, wait+10*time.Second)
	defer cancel()
	var batch spi.Batch
	status, h, err := s.pull.doHeader(ctx, http.MethodGet, next+sep+"wait="+strconv.FormatInt(wait.Milliseconds(), 10)+"ms", nil, nil, &batch)
	if err != nil {
		return nil, "", err
	}
	nextURL := h.Get(pix.HeaderPullNext)
	if status == http.StatusNoContent {
		return nil, nextURL, nil
	}
	return batch.Messages, nextURL, nil
}

// Status consulta a situação de uma transação como participante requester.
func (s *SPI) Status(ctx context.Context, requester, endToEndID string) (iso20022.TxInfAndSts, error) {
	var st iso20022.TxInfAndSts
	_, err := s.c.do(ctx, http.MethodGet, "/api/v1/status/"+endToEndID,
		http.Header{pix.HeaderRequestingParticipant: {requester}}, nil, &st)
	return st, err
}
