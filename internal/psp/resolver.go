package psp

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/brenokcunha/light-pix/internal/clients"
	"github.com/brenokcunha/light-pix/internal/iso20022"
	"github.com/brenokcunha/light-pix/internal/sim/bacen"
)

// SPIStatus consulta a situação de uma transação no SPI.
type SPIStatus interface {
	Status(ctx context.Context, requester, endToEndID string) (iso20022.TxInfAndSts, error)
}

// PIStatement lê o extrato da conta PI no bacen-sim.
type PIStatement interface {
	Statement(ctx context.Context, ispb string, from, to time.Time) ([]bacen.Entry, error)
}

// ResolverConfig ajusta o pending-resolver. Zeros usam o padrão entre parênteses.
type ResolverConfig struct {
	Interval    time.Duration // período da busca de pendências (10s)
	MinAge      time.Duration // quanto tempo sem mudar para uma pendência ser consultada (40s)
	BatchSize   int           // pendências por ciclo (100)
	DLQInterval time.Duration // período do reprocessamento das DLQs (1min)
	MaxRedrives int           // vezes que uma mensagem sai da DLQ antes de ser estacionada (3)
	ReconcileAt string        // "HH:MM" UTC da reconciliação diária; vazio desliga
	IGAInterval time.Duration // período do envio dos instantes ao regulador (10s)
}

func (c *ResolverConfig) defaults() {
	if c.Interval == 0 {
		c.Interval = 10 * time.Second
	}
	if c.MinAge == 0 {
		c.MinAge = 40 * time.Second
	}
	if c.BatchSize == 0 {
		c.BatchSize = 100
	}
	if c.DLQInterval == 0 {
		c.DLQInterval = time.Minute
	}
	if c.MaxRedrives == 0 {
		c.MaxRedrives = 3
	}
	if c.IGAInterval == 0 {
		c.IGAInterval = 10 * time.Second
	}
}

// Resolver é o pending-resolver: acha pagamentos parados em estado não
// terminal e os resolve pelo que o SPI diz, reprocessa as DLQs e concilia a
// conta PI com o razão. Todas as ações são idempotentes (a mesma escrita
// condicional do caminho normal), então várias réplicas podem rodar juntas.
type Resolver struct {
	svc  *Service
	spi  SPIStatus
	pi   PIStatement
	conn *amqp.Connection // RabbitMQ, para reprocessar as DLQs; nil desliga
	cfg  ResolverConfig

	tracer     trace.Tracer
	checked    metric.Int64Counter
	redriven   metric.Int64Counter
	parked     metric.Int64Counter
	recRuns    metric.Int64Counter
	recDiverge metric.Int64Counter
	lastReport atomic.Pointer[Report]

	igaSink     IGASink
	igaReported metric.Int64Counter
}

// NewResolver prepara o resolver e registra as métricas.
func NewResolver(svc *Service, spi SPIStatus, pi PIStatement, conn *amqp.Connection, cfg ResolverConfig) (*Resolver, error) {
	cfg.defaults()
	r := &Resolver{svc: svc, spi: spi, pi: pi, conn: conn, cfg: cfg,
		tracer: otel.Tracer("github.com/brenokcunha/light-pix/internal/psp")}
	meter := otel.Meter("github.com/brenokcunha/light-pix/internal/psp")
	var err error
	if r.checked, err = meter.Int64Counter("resolver.checked",
		metric.WithDescription("Pendências consultadas no SPI, por sentido e desfecho")); err != nil {
		return nil, err
	}
	if r.redriven, err = meter.Int64Counter("resolver.dlq.redriven",
		metric.WithDescription("Mensagens devolvidas da DLQ à fila de origem")); err != nil {
		return nil, err
	}
	if r.parked, err = meter.Int64Counter("resolver.dlq.parked",
		metric.WithDescription("Mensagens estacionadas em dead_letters depois de esgotar os reenvios")); err != nil {
		return nil, err
	}
	if r.recRuns, err = meter.Int64Counter("reconciliation.runs",
		metric.WithDescription("Reconciliações executadas, por resultado")); err != nil {
		return nil, err
	}
	if r.recDiverge, err = meter.Int64Counter("reconciliation.divergences",
		metric.WithDescription("Divergências achadas pela reconciliação, por tipo")); err != nil {
		return nil, err
	}
	if r.igaReported, err = meter.Int64Counter("resolver.iga.reported",
		metric.WithDescription("Registros enviados ao regulador (IGA)")); err != nil {
		return nil, err
	}
	_, err = meter.Int64ObservableGauge("pix.payments.stuck",
		metric.WithDescription("Pagamentos em estado não terminal há mais que o limiar do resolver"),
		metric.WithInt64Callback(r.observeStuck))
	if err != nil {
		return nil, err
	}
	_, err = meter.Int64ObservableGauge("reconciliation.last.divergences",
		metric.WithDescription("Divergências na última reconciliação"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			if rep := r.lastReport.Load(); rep != nil {
				o.Observe(int64(len(rep.Divergences)))
			}
			return nil
		}))
	if err != nil {
		return nil, err
	}
	_, err = meter.Int64ObservableGauge("reconciliation.last.timestamp", metric.WithUnit("s"),
		metric.WithDescription("Quando a última reconciliação rodou (Unix)"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			if rep := r.lastReport.Load(); rep != nil {
				o.Observe(rep.CreatedAt.Unix())
			}
			return nil
		}))
	return r, err
}

// nonTerminal seleciona pagamentos parados além do limiar.
func (r *Resolver) nonTerminal() bson.D {
	return bson.D{
		{Key: "$or", Value: bson.A{
			bson.D{{Key: "direction", Value: DirectionOut}, {Key: "state", Value: bson.D{{Key: "$in", Value: bson.A{StatePending, StateSent}}}}},
			bson.D{{Key: "direction", Value: DirectionIn}, {Key: "state", Value: StateAccepted}},
		}},
		{Key: "updatedAt", Value: bson.D{{Key: "$lte", Value: time.Now().UTC().Add(-r.cfg.MinAge)}}},
	}
}

func (r *Resolver) observeStuck(ctx context.Context, o metric.Int64Observer) error {
	counts := map[[2]string]int64{}
	for _, coll := range []*mongo.Collection{r.svc.store.payments, r.svc.store.returns} {
		cur, err := coll.Aggregate(ctx, bson.A{
			bson.D{{Key: "$match", Value: r.nonTerminal()}},
			bson.D{{Key: "$group", Value: bson.D{
				{Key: "_id", Value: bson.D{{Key: "direction", Value: "$direction"}, {Key: "state", Value: "$state"}}},
				{Key: "n", Value: bson.D{{Key: "$sum", Value: 1}}},
			}}},
		})
		if err != nil {
			return err
		}
		var rows []struct {
			ID struct{ Direction, State string } `bson:"_id"`
			N  int64                             `bson:"n"`
		}
		if err := cur.All(ctx, &rows); err != nil {
			return err
		}
		for _, row := range rows {
			counts[[2]string{row.ID.Direction, row.ID.State}] += row.N
		}
	}
	seen := map[[2]string]bool{}
	for k, n := range counts {
		seen[k] = true
		o.Observe(n, metric.WithAttributes(attribute.String("direction", k[0]), attribute.String("state", k[1])))
	}
	// Combinações sem pendência precisam aparecer com 0, para o alerta zerar.
	for _, k := range [][2]string{{DirectionOut, StatePending}, {DirectionOut, StateSent}, {DirectionIn, StateAccepted}} {
		if !seen[k] {
			o.Observe(0, metric.WithAttributes(attribute.String("direction", k[0]), attribute.String("state", k[1])))
		}
	}
	return nil
}

// Run executa os ciclos do resolver até o contexto terminar.
func (r *Resolver) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	loop := func(every time.Duration, fn func(context.Context) error, what string) {
		wg.Go(func() {
			t := time.NewTicker(every)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					if err := fn(ctx); err != nil && ctx.Err() == nil {
						slog.ErrorContext(ctx, "falha no ciclo do resolver", "cycle", what, "err", err)
					}
				}
			}
		})
	}
	loop(r.cfg.Interval, func(ctx context.Context) error { _, err := r.ResolvePending(ctx); return err }, "pendências")
	if r.conn != nil {
		loop(r.cfg.DLQInterval, r.RedriveDLQs, "dlq")
	}
	if r.igaSink != nil {
		loop(r.cfg.IGAInterval, func(ctx context.Context) error { _, err := r.ReportIGA(ctx); return err }, "iga")
	}
	if r.cfg.ReconcileAt != "" {
		loop(r.cfg.Interval, r.reconcileIfDue, "reconciliação")
	}
	wg.Wait()
	return nil
}

// ResolvePending consulta o SPI sobre os pagamentos parados e aplica o
// resultado pelo mesmo caminho do worker. Devolve quantos examinou.
func (r *Resolver) ResolvePending(ctx context.Context) (int, error) {
	total := 0
	// Pix e devoluções seguem o mesmo ciclo de vida e a mesma consulta ao SPI.
	for _, coll := range []*mongo.Collection{r.svc.store.payments, r.svc.store.returns} {
		cur, err := coll.Find(ctx, r.nonTerminal(),
			options.Find().SetSort(bson.D{{Key: "updatedAt", Value: 1}}).SetLimit(int64(r.cfg.BatchSize)))
		if err != nil {
			return total, err
		}
		var pending []Payment
		if err := cur.All(ctx, &pending); err != nil {
			return total, err
		}
		for _, p := range pending {
			r.resolveOne(ctx, p)
		}
		total += len(pending)
	}
	return total, nil
}

func (r *Resolver) resolveOne(ctx context.Context, p Payment) {
	ctx, span := r.tracer.Start(ctx, "resolver.resolve", trace.WithAttributes(
		attribute.String("pix.end_to_end_id", p.EndToEndID),
		attribute.String("pix.direction", p.Direction),
		attribute.String("pix.state", p.State)))
	defer span.End()

	outcome := "error"
	defer func() {
		span.SetAttributes(attribute.String("resolver.outcome", outcome))
		r.checked.Add(ctx, 1, metric.WithAttributes(attribute.String("direction", p.Direction), attribute.String("outcome", outcome)))
	}()

	st, err := r.spi.Status(ctx, r.svc.ispb, p.EndToEndID)
	var apiErr *clients.APIError
	switch {
	case errors.As(err, &apiErr) && apiErr.Status == 404:
		// O SPI não conhece a transação: a mensagem ainda não chegou lá. Quem
		// manda é o retry/DLQ; reembolsar daqui arriscaria pagar duas vezes.
		outcome = "not_found"
	case err != nil:
		slog.WarnContext(ctx, "resolver não conseguiu consultar o SPI", "end_to_end_id", p.EndToEndID, "err", err)
		span.SetStatus(codes.Error, err.Error())
	default:
		if outcome, err = r.apply(ctx, p.Direction, p.EndToEndID, st); err != nil {
			slog.ErrorContext(ctx, "resolver não conseguiu aplicar o resultado", "end_to_end_id", p.EndToEndID, "err", err)
			span.SetStatus(codes.Error, err.Error())
			outcome = "error"
		}
	}
}

// apply aplica a situação informada pelo SPI ao pagamento e devolve o desfecho.
func (r *Resolver) apply(ctx context.Context, direction, endToEndID string, st iso20022.TxInfAndSts) (string, error) {
	switch st.TxSts {
	case iso20022.StatusSettled:
		if direction == DirectionIn {
			return "credited", r.svc.FinishIncoming(ctx, endToEndID, StateCredited, "", st.FctvIntrBkSttlmDt)
		}
		return "confirmed", r.svc.Finish(ctx, endToEndID, StateConfirmed, "", st.FctvIntrBkSttlmDt)
	case iso20022.StatusRejected:
		reason := iso20022.ReasonSettlementFailed
		if st.StsRsnInf != nil && st.StsRsnInf.Rsn != "" {
			reason = st.StsRsnInf.Rsn
		}
		if direction == DirectionIn {
			return "rejected", r.svc.FinishIncoming(ctx, endToEndID, StateRejected, reason, nil)
		}
		return "rejected", r.svc.Finish(ctx, endToEndID, StateRejected, reason, nil)
	default:
		return "in_flight", nil // PDNG ou ACSP: o SPI ainda está trabalhando
	}
}
