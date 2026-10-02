package psp_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/brenokcunha/light-pix/internal/broker"
	"github.com/brenokcunha/light-pix/internal/clients"
	"github.com/brenokcunha/light-pix/internal/outbox"
	"github.com/brenokcunha/light-pix/internal/pix"
	"github.com/brenokcunha/light-pix/internal/psp"
	"github.com/brenokcunha/light-pix/internal/sim/bacen"
	"github.com/brenokcunha/light-pix/internal/sim/dict"
	"github.com/brenokcunha/light-pix/internal/sim/spi"
	"github.com/brenokcunha/light-pix/internal/testutil"
)

const (
	ispbA = "11111111"
	ispbB = "22222222"

	accountsSeed = "../../deploy/psp/accounts-seed.json"
	dictSeed     = "../../deploy/sims/dict-seed.json"
)

// spans recebe todos os spans do processo de teste.
var spans = tracetest.NewSpanRecorder()

func TestMain(m *testing.M) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans)))
	os.Exit(m.Run())
}

// world é o ambiente completo: simuladores do BCB e dois PSPs, com MongoDB e
// RabbitMQ reais (um banco e um vhost exclusivos por componente).
type world struct {
	t     *testing.T
	spi   string // URL do spi-sim
	dict  string // URL do dict-sim
	bacen *clients.Bacen
	nodes map[string]*node
}

type worldOpts struct {
	creditorTimeout time.Duration
	// roles de cada PSP que NÃO sobem (para simular componentes fora do ar).
	skip map[string]map[string]bool // ispb → papel (relay, connector, worker)
}

func newWorld(t *testing.T, opts worldOpts) *world {
	t.Helper()
	if opts.creditorTimeout == 0 {
		opts.creditorTimeout = 10 * time.Second
	}
	ctx, cancel := context.WithCancel(context.Background())
	w := &world{t: t, nodes: map[string]*node{}}
	var sims sync.WaitGroup
	servers := []*httptest.Server{}
	serve := func(register func(*http.ServeMux)) string {
		mux := http.NewServeMux()
		register(mux)
		srv := httptest.NewServer(otelhttp.NewHandler(mux, "sim"))
		servers = append(servers, srv)
		return srv.URL
	}

	// bacen-sim
	bClient, bDB := testutil.Mongo(t)
	b, err := bacen.New(ctx, bClient, bDB)
	must(t, err)
	must(t, b.Seed(ctx, map[string]pix.Amount{ispbA: 1_000_000_00, ispbB: 1_000_000_00}))
	bacenURL := serve(b.Routes)
	w.bacen = clients.NewBacen(bacenURL, 5*time.Second)

	// dict-sim
	dClient, dDB := testutil.Mongo(t)
	d, err := dict.New(ctx, dClient, dDB, dict.Config{BucketCapacity: 1000, RefillPerMinute: 600, NotFoundCost: 1})
	must(t, err)
	var entries []dict.Entry
	raw, err := os.ReadFile(dictSeed)
	must(t, err)
	must(t, json.Unmarshal(raw, &entries))
	must(t, d.Seed(ctx, entries))
	w.dict = serve(d.Routes)

	// spi-sim
	sClient, sDB := testutil.Mongo(t)
	s, err := spi.New(ctx, sClient, sDB, spi.Config{
		Participants: []string{ispbA, ispbB}, CreditorTimeout: opts.creditorTimeout, PollWait: 2 * time.Second,
		Lease: 2 * time.Second, SweepInterval: 100 * time.Millisecond, SettleRetry: 500 * time.Millisecond, Settler: w.bacen,
	})
	must(t, err)
	w.spi = serve(s.Routes)
	sims.Go(func() { _ = s.Run(ctx) })

	for _, ispb := range []string{ispbA, ispbB} {
		w.nodes[ispb] = w.newNode(ctx, ispb, opts.skip[ispb])
	}

	// Ordem inversa de criação: PSPs param primeiro (registrados por último),
	// depois o SPI, depois servidores e bancos.
	t.Cleanup(func() {
		cancel()
		sims.Wait()
		for _, srv := range servers {
			srv.Close()
		}
	})
	return w
}

// node é um PSP: serviço, API HTTP e os papéis relay, connector e worker.
type node struct {
	w      *world
	ispb   string
	apiURL string
	client *mongo.Client
	db     string
	conn   *amqp.Connection
	svc    *psp.Service
	store  *psp.Store
	spi    *clients.SPI

	ctx context.Context
	wg  sync.WaitGroup
}

func (w *world) newNode(parent context.Context, ispb string, skip map[string]bool) *node {
	t := w.t
	client, dbName := testutil.Mongo(t)
	rabbitURL := testutil.RabbitMQ(t)
	conn, err := broker.Dial(parent, rabbitURL)
	must(t, err)
	must(t, broker.Declare(conn))

	st, err := psp.NewStore(parent, client, client.Database(dbName))
	must(t, err)
	must(t, st.SeedAccounts(parent, ispb, accountsSeed))
	dictClient := clients.NewDict(w.dict, ispb, clients.DictConfig{Timeout: 2 * time.Second, Attempts: 2})
	svc, err := psp.NewService(ispb, st, dictClient)
	must(t, err)

	mux := http.NewServeMux()
	psp.NewAPI(svc, dictClient).Routes(mux)
	srv := httptest.NewServer(otelhttp.NewHandler(mux, "payment-api"))

	n := &node{w: w, ispb: ispb, apiURL: srv.URL, client: client, db: dbName, conn: conn, svc: svc, store: st,
		spi: clients.NewSPI(w.spi, 5*time.Second)}
	n.ctx = parent // cancelado junto com o mundo
	for role, start := range map[string]func(){"relay": n.startRelay, "connector": n.startConnector, "worker": n.startWorker} {
		if !skip[role] {
			start()
		}
	}
	t.Cleanup(func() {
		srv.Close()
		n.wg.Wait()
		conn.Close()
	})
	return n
}

func (n *node) run(fn func(ctx context.Context) error) {
	n.wg.Go(func() {
		if err := fn(n.ctx); err != nil && n.ctx.Err() == nil {
			n.w.t.Errorf("PSP %s: %v", n.ispb, err)
		}
	})
}

func (n *node) startRelay() {
	pub, err := broker.NewPublisher(n.conn)
	must(n.w.t, err)
	relay, err := outbox.NewRelay(n.ctx, n.client.Database(n.db), pub, outbox.RelayConfig{
		Name: "relay", SweepInterval: 200 * time.Millisecond, SweepMinAge: 500 * time.Millisecond,
	})
	must(n.w.t, err)
	n.run(relay.Run)
}

func (n *node) startConnector() {
	pub, err := broker.NewPublisher(n.conn)
	must(n.w.t, err)
	c := psp.NewConnector(n.ispb, n.svc, n.spi, pub, time.Second)
	n.run(func(ctx context.Context) error {
		return broker.Consume(ctx, n.conn, broker.ConsumeConfig{Queue: broker.QueueSPIOut, Concurrency: 2, RetryDelay: 100 * time.Millisecond}, c.HandleOut)
	})
	n.run(c.Pull)
}

func (n *node) startWorker() {
	n.run(func(ctx context.Context) error {
		return broker.Consume(ctx, n.conn, broker.ConsumeConfig{Queue: broker.QueueResults, Concurrency: 2, RetryDelay: 100 * time.Millisecond}, n.svc.HandleResult)
	})
	n.run(func(ctx context.Context) error {
		return broker.Consume(ctx, n.conn, broker.ConsumeConfig{Queue: broker.QueueIncoming, Concurrency: 2, RetryDelay: 100 * time.Millisecond}, n.svc.HandleIncoming)
	})
}

// --- chamadas HTTP e consultas -------------------------------------------------

type response struct {
	status int
	header http.Header
	body   map[string]any
}

func (r response) str(key string) string { s, _ := r.body[key].(string); return s }

func (n *node) call(method, path, idemKey string, body any) response {
	n.w.t.Helper()
	var buf bytes.Buffer
	if body != nil {
		must(n.w.t, json.NewEncoder(&buf).Encode(body))
	}
	req, _ := http.NewRequest(method, n.apiURL+path, &buf)
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}
	resp, err := http.DefaultClient.Do(req)
	must(n.w.t, err)
	defer resp.Body.Close()
	out := response{status: resp.StatusCode, header: resp.Header, body: map[string]any{}}
	_ = json.NewDecoder(resp.Body).Decode(&out.body)
	return out
}

// pay faz o pedido de Pix de debtor (número da conta) para key.
func (n *node) pay(idemKey, debtor, key string, amount json.Number) response {
	n.w.t.Helper()
	return n.call(http.MethodPost, "/v1/pix/payments", idemKey, map[string]any{
		"debtorAccount": map[string]string{"branch": "0001", "number": debtor},
		"key":           key,
		"amount":        amount,
	})
}

func (n *node) payment(e2e string) response {
	n.w.t.Helper()
	return n.call(http.MethodGet, "/v1/pix/payments/"+e2e, "", nil)
}

// balance devolve disponível e reservado da conta, em centavos.
func (n *node) balance(number string) (available, held pix.Amount) {
	n.w.t.Helper()
	acc, err := n.svc.GetAccount(context.Background(), "0001", number)
	must(n.w.t, err)
	return acc.Balance.Available, acc.Balance.Held
}

func (n *node) setAccountStatus(number, status string) {
	n.w.t.Helper()
	_, err := n.client.Database(n.db).Collection("accounts").UpdateOne(context.Background(),
		bson.D{{Key: "number", Value: number}}, bson.D{{Key: "$set", Value: bson.D{{Key: "status", Value: status}}}})
	must(n.w.t, err)
}

// waitState espera o pagamento chegar ao estado e devolve a resposta.
func (n *node) waitState(e2e, state string, timeout time.Duration) response {
	n.w.t.Helper()
	var last response
	eventually(n.w.t, timeout, "pagamento "+e2e+" em "+state, func() bool {
		last = n.payment(e2e)
		return last.str("state") == state
	})
	return last
}

// checkLedger confere que, em toda conta, o saldo é a soma do razão.
func (n *node) checkLedger() {
	n.w.t.Helper()
	ctx := context.Background()
	db := n.client.Database(n.db)
	cur, err := db.Collection("ledger_entries").Find(ctx, bson.D{})
	must(n.w.t, err)
	var entries []psp.LedgerEntry
	must(n.w.t, cur.All(ctx, &entries))
	sums := map[bson.ObjectID][2]pix.Amount{}
	for _, e := range entries {
		s := sums[e.AccountID]
		sums[e.AccountID] = [2]pix.Amount{s[0] + e.AvailableDelta, s[1] + e.HeldDelta}
	}
	cur, err = db.Collection("accounts").Find(ctx, bson.D{})
	must(n.w.t, err)
	var accs []psp.Account
	must(n.w.t, cur.All(ctx, &accs))
	for _, a := range accs {
		if s := sums[a.ID]; s[0] != a.Balance.Available || s[1] != a.Balance.Held {
			n.w.t.Errorf("conta %s: saldo %s/%s, razão soma %s/%s", a.Number, a.Balance.Available, a.Balance.Held, s[0], s[1])
		}
	}
}

func (n *node) outboxCount(filter bson.D) int64 {
	n.w.t.Helper()
	c, err := n.client.Database(n.db).Collection("outbox").CountDocuments(context.Background(), filter)
	must(n.w.t, err)
	return c
}

func (w *world) piBalance(ispb string) pix.Amount {
	w.t.Helper()
	acc, err := w.bacen.Account(context.Background(), ispb)
	must(w.t, err)
	return acc.Balance
}

func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("tempo esgotado esperando: %s", what)
}

func must(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// httpDo envia o JSON com o header de participante e devolve o status.
func httpDo(t *testing.T, method, url string, body any, participant string) int {
	t.Helper()
	var buf bytes.Buffer
	must(t, json.NewEncoder(&buf).Encode(body))
	req, _ := http.NewRequest(method, url, &buf)
	req.Header.Set(pix.HeaderRequestingParticipant, participant)
	resp, err := http.DefaultClient.Do(req)
	must(t, err)
	resp.Body.Close()
	return resp.StatusCode
}
