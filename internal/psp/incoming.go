package psp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/brenokcunha/light-pix/internal/broker"
	"github.com/brenokcunha/light-pix/internal/iso20022"
	"github.com/brenokcunha/light-pix/internal/outbox"
	"github.com/brenokcunha/light-pix/internal/pix"
	"github.com/brenokcunha/light-pix/internal/store"
)

// HandleIncoming trata uma pacs.008 vinda do SPI (fila pix.incoming) e
// responde com pacs.002 pela outbox: ACSP se a conta do recebedor existe e
// está ativa; RJCT AC03 (conta inexistente) ou AC06 (conta bloqueada ou
// encerrada) caso contrário.
//
// Versão mínima, suficiente para o Pix enviado fechar o ciclo: ainda não
// grava o recebimento nem credita a conta (isso vem com o Pix recebido).
// Responder de novo a uma pacs.008 repetida é inofensivo: o SPI ignora.
func (s *Service) HandleIncoming(ctx context.Context, d amqp.Delivery) error {
	var env iso20022.Envelope
	if err := json.Unmarshal(d.Body, &env); err != nil {
		return broker.Permanent(fmt.Errorf("pacs.008 ilegível: %w", err))
	}
	if err := env.Validate(); err != nil || env.FIToFICstmrCdtTrf == nil {
		return broker.Permanent(fmt.Errorf("pacs.008 inválida: %v", err))
	}
	doc := env.FIToFICstmrCdtTrf
	tx := doc.CdtTrfTxInf
	if tx.CdtrAgt.MmbId != s.ispb {
		return broker.Permanent(fmt.Errorf("pacs.008 destinada a %s chegou ao PSP %s", tx.CdtrAgt.MmbId, s.ispb))
	}
	trace.SpanFromContext(ctx).SetAttributes(attribute.String("pix.end_to_end_id", tx.PmtId.EndToEndId))

	now := store.Now()
	st := iso20022.TxInfAndSts{OrgnlEndToEndId: tx.PmtId.EndToEndId, TxSts: iso20022.StatusAccepted, AccptncDtTm: &now}
	acc, err := s.store.FindAccount(ctx, tx.CdtrAcct.Issr, tx.CdtrAcct.Id)
	switch {
	case errors.Is(err, mongo.ErrNoDocuments):
		st = rejection(tx.PmtId.EndToEndId, iso20022.ReasonInvalidAccount)
	case err != nil:
		return err
	case acc.Status != AccountActive:
		st = rejection(tx.PmtId.EndToEndId, iso20022.ReasonBlockedAccount)
	}

	reply := iso20022.NewPacs002Envelope(s.ispb, pix.BCBISPB, doc.GrpHdr.MsgId, st, now)
	body, err := json.Marshal(reply)
	if err != nil {
		return err
	}
	err = store.Transact(ctx, s.store.client, func(ctx context.Context) error {
		return outbox.Add(ctx, s.store.outbox, broker.QueueSPIOut, reply.AppHdr.BizMsgIdr, body)
	})
	if err != nil {
		return err
	}
	slog.InfoContext(ctx, "pacs.008 respondida", "end_to_end_id", tx.PmtId.EndToEndId, "status", st.TxSts)
	return nil
}

func rejection(endToEndID, reason string) iso20022.TxInfAndSts {
	return iso20022.TxInfAndSts{OrgnlEndToEndId: endToEndID, TxSts: iso20022.StatusRejected,
		StsRsnInf: &iso20022.StsRsnInf{Rsn: reason}}
}
