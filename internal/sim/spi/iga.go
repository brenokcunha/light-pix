package spi

import (
	"context"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/brenokcunha/light-pix/internal/sim/bacen"
)

const (
	igaQueueSize  = 10_000
	igaBatchSize  = 200
	igaFlushEvery = time.Second
)

// queueIGA registra os instantes da transação encerrada para o envio ao IGA.
// Relê a transação, porque a cópia que o fluxo carrega é anterior às
// mudanças de estado. Fila cheia descarta: medição não pode travar o Pix.
func (s *Server) queueIGA(ctx context.Context, tx Transaction, status, reason string, now time.Time) {
	if s.cfg.IGA == nil {
		return
	}
	var fresh Transaction
	if err := s.txs.FindOne(ctx, bson.D{{Key: "_id", Value: tx.EndToEndId}}).Decode(&fresh); err != nil {
		fresh = tx
	}
	rec := bacen.IGASPIRecord{
		EndToEndID: tx.EndToEndId, DebtorISPB: tx.DebtorISPB, CreditorISPB: tx.CreditorISPB,
		Status: status, Reason: reason,
		ReceivedAt: fresh.ReceivedAt, AcceptedAt: fresh.AcceptedAt, SettledAt: fresh.SettledAt, FinishedAt: now,
	}
	select {
	case s.igaQueue <- rec:
	default:
		s.igaDropped.Add(ctx, 1)
	}
}

// flushIGA envia os registros acumulados em lotes até o contexto terminar e,
// no fim, esvazia o que restou. Falha de envio descarta o lote (e conta).
func (s *Server) flushIGA(ctx context.Context) {
	t := time.NewTicker(igaFlushEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			s.drainIGA(context.WithoutCancel(ctx))
			return
		case <-t.C:
			s.drainIGA(ctx)
		}
	}
}

func (s *Server) drainIGA(ctx context.Context) {
	for {
		var batch []bacen.IGASPIRecord
	collect:
		for len(batch) < igaBatchSize {
			select {
			case r := <-s.igaQueue:
				batch = append(batch, r)
			default:
				break collect
			}
		}
		if len(batch) == 0 {
			return
		}
		sendCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		err := s.cfg.IGA.ReportSPI(sendCtx, batch)
		cancel()
		if err != nil {
			s.igaDropped.Add(ctx, int64(len(batch)))
			slog.WarnContext(ctx, "lote do IGA descartado", "records", len(batch), "err", err)
			return
		}
	}
}
