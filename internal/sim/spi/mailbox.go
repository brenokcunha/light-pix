package spi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/brenokcunha/light-pix/internal/httpx"
	"github.com/brenokcunha/light-pix/internal/iso20022"
	"github.com/brenokcunha/light-pix/internal/pix"
	"github.com/brenokcunha/light-pix/internal/store"
)

// maxBatch limita as mensagens entregues por pull.
const maxBatch = 100

// mailItem é uma mensagem destinada a um participante. A entrega é
// at-least-once: o lote fica reservado (lease) até ser confirmado no pull
// seguinte; sem confirmação, volta a ficar disponível quando o lease vence.
type mailItem struct {
	ID           bson.ObjectID     `bson:"_id"`
	ISPB         string            `bson:"ispb"`
	Envelope     iso20022.Envelope `bson:"envelope"`
	TraceContext map[string]string `bson:"traceContext,omitempty"`
	CreatedAt    time.Time         `bson:"createdAt"`
	LeaseUntil   time.Time         `bson:"leaseUntil"`
	Batch        string            `bson:"batch,omitempty"`
	Acked        bool              `bson:"acked"`
	AckedAt      *time.Time        `bson:"ackedAt,omitempty"`
}

// Delivery é uma mensagem entregue no pull, com o contexto de trace de quem
// a originou (extensão do laboratório para manter um trace ponta a ponta).
type Delivery struct {
	TraceContext map[string]string `json:"traceContext,omitempty"`
	Envelope     iso20022.Envelope `json:"envelope"`
}

// Batch é a resposta de um pull com mensagens.
type Batch struct {
	Messages []Delivery `json:"messages"`
}

func (s *Server) enqueue(ctx context.Context, ispb string, env iso20022.Envelope, tc map[string]string, now time.Time) error {
	_, err := s.mailbox.InsertOne(ctx, mailItem{
		ID: bson.NewObjectID(), ISPB: ispb, Envelope: env, TraceContext: tc, CreatedAt: now,
	})
	return err
}

// handlePull é o long-polling do participante. O parâmetro ack confirma o
// lote anterior (é o que vem na URL de PI-PULL-NEXT); wait encurta a espera.
func (s *Server) handlePull(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ispb := r.PathValue("ispb")
	if !s.participant(ispb) {
		httpx.WriteError(w, http.StatusForbidden, "FORBIDDEN", "participante não habilitado")
		return
	}
	q := r.URL.Query()
	if ack := q.Get("ack"); ack != "" {
		if err := s.ack(ctx, ispb, ack); err != nil {
			httpx.WriteError(w, http.StatusInternalServerError, "INTERNAL", err.Error())
			return
		}
	}
	wait := s.cfg.PollWait
	if v, err := time.ParseDuration(q.Get("wait")); err == nil && v >= 0 && v < wait {
		wait = v
	}
	timeout := time.NewTimer(wait)
	defer timeout.Stop()
	// Leases vencidos não geram notificação; o tick cobre esse caso.
	tick := time.NewTicker(time.Second)
	defer tick.Stop()

	base := "/api/v1/out/" + ispb + "/stream"
	for {
		// Pegar o canal antes de consultar evita perder um aviso entre os dois.
		wake := s.notifier.wait(ispb)
		batch, items, err := s.claim(ctx, ispb)
		if err != nil {
			httpx.WriteError(w, http.StatusInternalServerError, "INTERNAL", err.Error())
			return
		}
		if len(items) > 0 {
			out := Batch{Messages: make([]Delivery, len(items))}
			for i, it := range items {
				out.Messages[i] = Delivery{TraceContext: it.TraceContext, Envelope: it.Envelope}
			}
			trace.SpanFromContext(ctx).SetAttributes(attribute.Int("spi.batch_size", len(items)))
			w.Header().Set(pix.HeaderPullNext, base+"?ack="+batch)
			httpx.WriteJSON(w, http.StatusOK, out)
			return
		}
		select {
		case <-wake:
		case <-tick.C:
		case <-s.stopping:
			// Desligando: responde vazio para o participante reconectar em
			// outra instância em vez de segurar o shutdown.
			w.Header().Set(pix.HeaderPullNext, base)
			w.WriteHeader(http.StatusNoContent)
			return
		case <-timeout.C:
			w.Header().Set(pix.HeaderPullNext, base)
			w.WriteHeader(http.StatusNoContent)
			return
		case <-ctx.Done():
			return
		}
	}
}

// claim reserva até maxBatch mensagens disponíveis para um novo lote. O
// filtro do update repete a condição de disponibilidade, então dois pulls
// concorrentes nunca levam a mesma mensagem.
func (s *Server) claim(ctx context.Context, ispb string) (string, []mailItem, error) {
	now := store.Now()
	available := bson.D{
		{Key: "ispb", Value: ispb},
		{Key: "acked", Value: false},
		{Key: "leaseUntil", Value: bson.D{{Key: "$lte", Value: now}}},
	}
	cur, err := s.mailbox.Find(ctx, available, options.Find().
		SetSort(bson.D{{Key: "_id", Value: 1}}).
		SetLimit(maxBatch).
		SetProjection(bson.D{{Key: "_id", Value: 1}}))
	if err != nil {
		return "", nil, err
	}
	var ids []struct {
		ID bson.ObjectID `bson:"_id"`
	}
	if err := cur.All(ctx, &ids); err != nil || len(ids) == 0 {
		return "", nil, err
	}
	in := make(bson.A, len(ids))
	for i, id := range ids {
		in[i] = id.ID
	}

	batch := newBatchID()
	_, err = s.mailbox.UpdateMany(ctx,
		append(available, bson.E{Key: "_id", Value: bson.D{{Key: "$in", Value: in}}}),
		bson.D{{Key: "$set", Value: bson.D{{Key: "batch", Value: batch}, {Key: "leaseUntil", Value: now.Add(s.cfg.Lease)}}}})
	if err != nil {
		return "", nil, err
	}
	cur, err = s.mailbox.Find(ctx, bson.D{{Key: "batch", Value: batch}}, options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}))
	if err != nil {
		return "", nil, err
	}
	var items []mailItem
	return batch, items, cur.All(ctx, &items)
}

func (s *Server) ack(ctx context.Context, ispb, batch string) error {
	now := store.Now()
	_, err := s.mailbox.UpdateMany(ctx,
		bson.D{{Key: "ispb", Value: ispb}, {Key: "batch", Value: batch}, {Key: "acked", Value: false}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "acked", Value: true}, {Key: "ackedAt", Value: now}}}})
	return err
}

func (s *Server) observePending(ctx context.Context, o metric.Int64Observer) error {
	cur, err := s.mailbox.Aggregate(ctx, bson.A{
		bson.D{{Key: "$match", Value: bson.D{{Key: "acked", Value: false}}}},
		bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$ispb"}, {Key: "n", Value: bson.D{{Key: "$sum", Value: 1}}}}}},
	})
	if err != nil {
		return err
	}
	var rows []struct {
		ISPB string `bson:"_id"`
		N    int64  `bson:"n"`
	}
	if err := cur.All(ctx, &rows); err != nil {
		return err
	}
	pending := map[string]int64{}
	for _, r := range rows {
		pending[r.ISPB] = r.N
	}
	for _, p := range s.cfg.Participants {
		o.Observe(pending[p], metric.WithAttributes(attribute.String("ispb", p)))
	}
	return nil
}

func newBatchID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// notifier acorda os long-pollings de um participante quando chega mensagem
// para ele. Cada espera recebe o canal da vez; notify fecha e troca o canal.
type notifier struct {
	mu    sync.Mutex
	chans map[string]chan struct{}
}

func newNotifier() *notifier { return &notifier{chans: map[string]chan struct{}{}} }

func (n *notifier) wait(ispb string) <-chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()
	ch, ok := n.chans[ispb]
	if !ok {
		ch = make(chan struct{})
		n.chans[ispb] = ch
	}
	return ch
}

func (n *notifier) notify(ispb string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if ch, ok := n.chans[ispb]; ok {
		close(ch)
		delete(n.chans, ispb)
	}
}
