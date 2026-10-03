package psp

import (
	"context"
	"errors"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/brenokcunha/light-pix/internal/sim/bacen"
)

const (
	igaBatch         = 500 // registros por envio
	igaBatchesPerRun = 20  // lotes por ciclo, para recuperar atraso sem monopolizar o resolver
)

// IGASink recebe os instantes que o PSP mede dos Pix encerrados (o bacen-sim).
type IGASink interface {
	ReportPSP(ctx context.Context, ispb string, recs []bacen.IGAPSPRecord) error
}

// SetIGA liga o envio periódico dos instantes ao regulador. Sem isso o
// resolver não reporta.
func (r *Resolver) SetIGA(sink IGASink) { r.igaSink = sink }

// igaCursor guarda até onde os Pix encerrados já foram reportados, ordenados
// por (finishedAt, _id).
type igaCursor struct {
	FinishedAt time.Time `bson:"finishedAt"`
	ID         string    `bson:"id"`
}

// ReportIGA envia ao regulador os instantes dos Pix encerrados desde o último
// envio, em lotes, e avança o cursor. Reenviar um registro é inofensivo (o
// bacen-sim é idempotente por EndToEndId e papel), então falha no meio só
// repete o último lote. Devolve quantos registros enviou.
func (r *Resolver) ReportIGA(ctx context.Context) (int, error) {
	coll := r.svc.store.igaCursor
	var cur igaCursor
	if err := coll.FindOne(ctx, bson.D{{Key: "_id", Value: "psp"}}).Decode(&cur); err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		return 0, err
	}

	total := 0
	for range igaBatchesPerRun {
		filter := bson.D{
			{Key: "state", Value: bson.D{{Key: "$in", Value: bson.A{StateConfirmed, StateRejected, StateCredited}}}},
			{Key: "$or", Value: bson.A{
				bson.D{{Key: "finishedAt", Value: bson.D{{Key: "$gt", Value: cur.FinishedAt}}}},
				bson.D{{Key: "finishedAt", Value: cur.FinishedAt}, {Key: "_id", Value: bson.D{{Key: "$gt", Value: cur.ID}}}},
			}},
		}
		c, err := r.svc.store.payments.Find(ctx, filter, options.Find().
			SetSort(bson.D{{Key: "finishedAt", Value: 1}, {Key: "_id", Value: 1}}).SetLimit(igaBatch))
		if err != nil {
			return total, err
		}
		var payments []Payment
		if err := c.All(ctx, &payments); err != nil {
			return total, err
		}
		if len(payments) == 0 {
			return total, nil
		}

		recs := make([]bacen.IGAPSPRecord, 0, len(payments))
		for _, p := range payments {
			if p.FinishedAt == nil {
				continue
			}
			rec := bacen.IGAPSPRecord{EndToEndID: p.EndToEndID, ISPB: r.svc.ispb, Role: bacen.RolePayer,
				T0: p.CreatedAt, T6a: *p.FinishedAt, State: p.State, Reason: p.Reason}
			if p.Direction == DirectionIn {
				rec.Role, rec.T1 = bacen.RoleCreditor, p.AcceptedAt
			}
			recs = append(recs, rec)
		}
		if len(recs) > 0 {
			if err := r.igaSink.ReportPSP(ctx, r.svc.ispb, recs); err != nil {
				return total, err
			}
		}
		last := payments[len(payments)-1]
		cur = igaCursor{FinishedAt: *last.FinishedAt, ID: last.EndToEndID}
		if _, err := coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: "psp"}},
			bson.D{{Key: "$set", Value: bson.D{{Key: "finishedAt", Value: cur.FinishedAt}, {Key: "id", Value: cur.ID}}}},
			options.UpdateOne().SetUpsert(true)); err != nil {
			return total, err
		}
		total += len(recs)
		r.igaReported.Add(ctx, int64(len(recs)), metric.WithAttributes(attribute.String("result", "ok")))
		if len(payments) < igaBatch {
			return total, nil
		}
	}
	return total, nil
}
