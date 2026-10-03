package bacen_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/brenokcunha/light-pix/internal/clients"
	"github.com/brenokcunha/light-pix/internal/pix"
	"github.com/brenokcunha/light-pix/internal/sim/bacen"
	"github.com/brenokcunha/light-pix/internal/testutil"
)

func histogramCount(t *testing.T, rm metricdata.ResourceMetrics, name, ispb string) uint64 {
	t.Helper()
	var total uint64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			for _, dp := range m.Data.(metricdata.Histogram[float64]).DataPoints {
				if v, ok := dp.Attributes.Value("ispb"); ok && v.AsString() == ispb {
					total += dp.Count
				}
			}
		}
	}
	return total
}

// Os relatórios do PSP e do SPI chegam em qualquer ordem e repetidos: cada
// indicador é medido uma única vez, assim que as duas pontas existem.
func TestIGAMeasuresEachIndicatorOnce(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))

	client, db := testutil.Mongo(t)
	s, err := bacen.New(context.Background(), client, db)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	s.Routes(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	c := clients.NewBacen(srv.URL, 5*time.Second)
	ctx := context.Background()

	base := time.Now().UTC().Add(-time.Minute).Truncate(time.Millisecond)
	at := func(d time.Duration) time.Time { return base.Add(d) }
	ptr := func(t time.Time) *time.Time { return &t }
	e2e := pix.NewEndToEndID(ispbA, base)

	payer := bacen.IGAPSPRecord{EndToEndID: e2e, ISPB: ispbA, Role: bacen.RolePayer,
		T0: base, T6a: at(2 * time.Second), State: "CONFIRMED"}
	spi := bacen.IGASPIRecord{EndToEndID: e2e, DebtorISPB: ispbA, CreditorISPB: ispbB, Status: "SETTLED",
		ReceivedAt: at(300 * time.Millisecond), AcceptedAt: ptr(at(900 * time.Millisecond)),
		SettledAt: ptr(at(1500 * time.Millisecond)), FinishedAt: at(1600 * time.Millisecond)}

	// O SPI chega primeiro, o PSP depois, e ambos chegam duas vezes.
	for range 2 {
		if err := c.ReportSPI(ctx, []bacen.IGASPIRecord{spi}); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		if err := c.ReportPSP(ctx, ispbA, []bacen.IGAPSPRecord{payer}); err != nil {
			t.Fatal(err)
		}
	}

	var rm metricdata.ResourceMetrics
	// Ao trocar o provider global, os medidores observáveis de Servers criados por
	// testes anteriores (com o MongoDB já desconectado) passam a rodar aqui e
	// devolvem erro; as medições deste teste vêm mesmo assim.
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Logf("coleta com erros de medidores de outros testes: %v", err)
	}
	for name, ispb := range map[string]string{
		"iga.payer_experience.duration":  ispbA,
		"iga.initiation.duration":        ispbA,
		"iga.settlement.duration":        ispbA,
		"iga.creditor_response.duration": ispbB,
	} {
		if got := histogramCount(t, rm, name, ispb); got != 1 {
			t.Errorf("%s{ispb=%s}: %d medições, quer 1", name, ispb, got)
		}
	}

	rep, err := c.IGA(ctx, ispbA, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Transactions != 1 || rep.Settled != 1 ||
		rep.PayerExperience.P50 != 2 || rep.Initiation.P50 != 0.3 || rep.Settlement.P50 != 1.2 {
		t.Fatalf("relatório de A: %+v", rep)
	}
	repB, err := c.IGA(ctx, ispbB, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if repB.CreditorResponse.P50 != 0.6 || repB.CreditorResponse.Count != 1 {
		t.Fatalf("relatório de B: %+v", repB)
	}
}

func TestIGARejectsInvalidReports(t *testing.T) {
	client, db := testutil.Mongo(t)
	s, err := bacen.New(context.Background(), client, db)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	s.Routes(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	c := clients.NewBacen(srv.URL, 5*time.Second)
	now := time.Now().UTC()
	e2e := pix.NewEndToEndID(ispbA, now)

	// Um PSP não reporta transações de outro.
	err = c.ReportPSP(context.Background(), ispbB, []bacen.IGAPSPRecord{{EndToEndID: e2e, ISPB: ispbA, Role: bacen.RolePayer, T0: now, T6a: now}})
	wantCode(t, err, http.StatusForbidden, "FORBIDDEN")
	err = c.ReportPSP(context.Background(), ispbA, []bacen.IGAPSPRecord{{EndToEndID: e2e, ISPB: ispbA, Role: "OUTRO", T0: now, T6a: now}})
	wantCode(t, err, http.StatusBadRequest, bacen.CodeInvalidRequest)
	err = c.ReportSPI(context.Background(), []bacen.IGASPIRecord{{EndToEndID: "x", DebtorISPB: ispbA, CreditorISPB: ispbB, Status: "SETTLED", ReceivedAt: now, FinishedAt: now}})
	wantCode(t, err, http.StatusBadRequest, bacen.CodeInvalidRequest)
}
