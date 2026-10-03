package spi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/brenokcunha/light-pix/internal/iso20022"
	"github.com/brenokcunha/light-pix/internal/pix"
	"github.com/brenokcunha/light-pix/internal/store"
)

// KindReturn marca, em Transaction.Kind, uma devolução (pacs.004).
const KindReturn = "RETURN"

// DefaultReturnWindow é o prazo para devolver um Pix liquidado.
const DefaultReturnWindow = 90 * 24 * time.Hour

var errReturnExceedsOriginal = errors.New("devolução acima do valor que resta do Pix original")

// receivePacs004 registra a devolução e a encaminha ao pagador original, que a
// trata como um crédito: responde pacs.002 e, aceita, o SPI liquida da conta PI
// de quem devolve para a de quem recebe. Valida no SPI, que é a autoridade:
//
//   - o Pix original existe, está liquidado e dentro do prazo;
//   - quem devolve é o recebedor dele e a devolução vai ao pagador dele;
//   - o total devolvido (ou em devolução) não passa do valor original. A reserva
//     desse valor é um $inc condicional no Pix original, na mesma transação que
//     grava a devolução: duas devoluções simultâneas nunca somam mais que o Pix.
//
// Reenviar a mesma mensagem é inofensivo; reutilizar o identificador com outra
// mensagem é rejeitado.
func (s *Server) receivePacs004(ctx context.Context, env iso20022.Envelope) error {
	doc := env.PmtRtr
	tx := doc.TxInf
	returner, payer := tx.DbtrAgt.MmbId, tx.CdtrAgt.MmbId
	trace.SpanFromContext(ctx).SetAttributes(
		attribute.String("pix.end_to_end_id", tx.RtrId),
		attribute.String("pix.original_end_to_end_id", tx.OrgnlEndToEndId))
	if tx.DbtrAgt.MmbId != env.AppHdr.Fr {
		return newAPIError(http.StatusBadRequest, "INVALID_MESSAGE", "DbtrAgt deve ser o remetente")
	}
	if !s.participant(payer) {
		return newAPIError(http.StatusUnprocessableEntity, "UNKNOWN_PARTICIPANT", "pagador original %s não habilitado", payer)
	}

	// Idempotência antes das validações de valor e prazo: a mesma mensagem
	// repetida volta 202 mesmo que, entre as duas, o Pix tenha sido todo devolvido.
	var prev Transaction
	switch err := s.txs.FindOne(ctx, bson.D{{Key: "_id", Value: tx.RtrId}}).Decode(&prev); {
	case err == nil && prev.MsgId == doc.GrpHdr.MsgId:
		slog.InfoContext(ctx, "pacs.004 repetida ignorada", "return_id", tx.RtrId)
		return nil
	case err == nil:
		return newAPIError(http.StatusConflict, "DUPLICATE_END_TO_END_ID", "identificador de devolução já usado por outra mensagem")
	case !errors.Is(err, mongo.ErrNoDocuments):
		return err
	}

	var orig Transaction
	err := s.txs.FindOne(ctx, bson.D{
		{Key: "_id", Value: tx.OrgnlEndToEndId},
		{Key: "kind", Value: bson.D{{Key: "$exists", Value: false}}},
	}).Decode(&orig)
	switch {
	case errors.Is(err, mongo.ErrNoDocuments):
		return newAPIError(http.StatusNotFound, "UNKNOWN_END_TO_END_ID", "Pix original %s não encontrado", tx.OrgnlEndToEndId)
	case err != nil:
		return err
	case orig.CreditorISPB != returner:
		return newAPIError(http.StatusForbidden, "FORBIDDEN", "só o recebedor do Pix original o devolve")
	case orig.DebtorISPB != payer:
		return newAPIError(http.StatusBadRequest, "INVALID_MESSAGE", "CdtrAgt deve ser o pagador do Pix original")
	case orig.Status != StatusSettled:
		return newAPIError(http.StatusUnprocessableEntity, "ORIGINAL_NOT_SETTLED", "o Pix original não está liquidado (%s)", orig.Status)
	}
	window := s.cfg.ReturnWindow
	if window == 0 {
		window = DefaultReturnWindow
	}
	settled := orig.ReceivedAt
	if orig.SettledAt != nil {
		settled = *orig.SettledAt
	}

	now := store.Now()
	if now.Sub(settled) > window {
		return newAPIError(http.StatusUnprocessableEntity, "RETURN_WINDOW_EXPIRED", "o prazo de devolução de %s venceu", window)
	}
	tc := injectTrace(ctx)
	amount := tx.RtrdIntrBkSttlmAmt.Value
	rec := Transaction{
		EndToEndId:         tx.RtrId,
		MsgId:              doc.GrpHdr.MsgId,
		Kind:               KindReturn,
		OriginalEndToEndID: tx.OrgnlEndToEndId,
		DebtorISPB:         returner,
		CreditorISPB:       payer,
		Amount:             amount,
		Status:             StatusAwaitingCreditor,
		TraceContext:       tc,
		ReceivedAt:         now,
		Deadline:           now.Add(s.cfg.CreditorTimeout),
		UpdatedAt:          now,
	}
	fwd := env
	fwd.AppHdr = iso20022.AppHdr{Fr: pix.BCBISPB, To: payer, BizMsgIdr: pix.NewMsgID(pix.BCBISPB),
		MsgDefIdr: iso20022.MsgPacs004, CreDt: now}

	err = store.Transact(ctx, s.client, func(ctx context.Context) error {
		res, err := s.txs.UpdateOne(ctx,
			bson.D{
				{Key: "_id", Value: orig.EndToEndId},
				{Key: "status", Value: StatusSettled},
				{Key: "$expr", Value: bson.D{{Key: "$lte", Value: bson.A{
					bson.D{{Key: "$add", Value: bson.A{bson.D{{Key: "$ifNull", Value: bson.A{"$returnedAmount", 0}}}, amount}}},
					"$amount",
				}}}},
			},
			bson.D{{Key: "$inc", Value: bson.D{{Key: "returnedAmount", Value: amount}}}})
		if err != nil {
			return err
		}
		if res.MatchedCount == 0 {
			return errReturnExceedsOriginal
		}
		if _, err := s.txs.InsertOne(ctx, rec); err != nil {
			return err
		}
		return s.enqueue(ctx, payer, fwd, tc, now)
	})
	switch {
	case errors.Is(err, errReturnExceedsOriginal):
		return newAPIError(http.StatusUnprocessableEntity, "RETURN_AMOUNT_EXCEEDED",
			"a devolução passaria do valor do Pix original (%s, já devolvido ou em devolução: %s)", orig.Amount, orig.ReturnedAmount)
	case mongo.IsDuplicateKeyError(err):
		var prev Transaction
		if ferr := s.txs.FindOne(ctx, bson.D{{Key: "_id", Value: rec.EndToEndId}}).Decode(&prev); ferr != nil {
			return ferr
		}
		if prev.MsgId == rec.MsgId {
			slog.InfoContext(ctx, "pacs.004 repetida ignorada", "return_id", rec.EndToEndId)
			return nil
		}
		return newAPIError(http.StatusConflict, "DUPLICATE_END_TO_END_ID", "identificador de devolução já usado por outra mensagem")
	case err != nil:
		return err
	}
	s.notifier.notify(payer)
	slog.InfoContext(ctx, "pacs.004 encaminhada", "return_id", rec.EndToEndId, "original", orig.EndToEndId,
		"returner_ispb", returner, "payer_ispb", payer)
	return nil
}
