package psp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/brenokcunha/light-pix/internal/broker"
	"github.com/brenokcunha/light-pix/internal/clients"
	"github.com/brenokcunha/light-pix/internal/iso20022"
	"github.com/brenokcunha/light-pix/internal/sim/spi"
)

// Connector faz a ponte entre o PSP e o SPI: envia o que está na fila
// spi.out e traz para o PSP as mensagens destinadas a ele.
type Connector struct {
	ispb     string
	svc      *Service
	spi      *clients.SPI
	pub      *broker.Publisher
	pollWait time.Duration
	tracer   trace.Tracer

	// Vazamento de memória injetado (cenário de incidente): a cada mensagem
	// enviada, leakKB KiB ficam retidos para sempre.
	leakKB int
	leakMu sync.Mutex
	leaked [][]byte
}

// SetLeak liga o vazamento de memória de mentira: kb KiB retidos por
// mensagem enviada ao SPI. É a falha do cenário "leak no connector"; com um
// limite de memória no container, termina em OOMKilled.
func (c *Connector) SetLeak(kb int) { c.leakKB = kb }

func (c *Connector) leak() {
	if c.leakKB <= 0 {
		return
	}
	b := make([]byte, c.leakKB*1024)
	for i := range b { // escreve em todas as páginas para elas contarem na memória residente
		b[i] = 1
	}
	c.leakMu.Lock()
	c.leaked = append(c.leaked, b)
	c.leakMu.Unlock()
}

// NewConnector cria o conector. pollWait é a espera de cada long-polling.
func NewConnector(ispb string, svc *Service, spiClient *clients.SPI, pub *broker.Publisher, pollWait time.Duration) *Connector {
	return &Connector{ispb: ispb, svc: svc, spi: spiClient, pub: pub, pollWait: pollWait,
		tracer: otel.Tracer("github.com/brenokcunha/light-pix/internal/psp")}
}

// HandleOut envia ao SPI uma mensagem da fila spi.out (pacs.008 do Pix
// enviado ou pacs.002 de resposta do lado recebedor).
//
//   - SPI aceitou (202): pacs.008 leva o pagamento a SENT.
//   - SPI recusou de forma definitiva (4xx): a pacs.008 vira REJECTED; o
//     pagador não fica com a reserva presa.
//   - Falha transitória (rede, 5xx, 429): devolve o erro; a mensagem volta à
//     fila. Reenviar é seguro porque o SPI deduplica por EndToEndId.
func (c *Connector) HandleOut(ctx context.Context, d amqp.Delivery) error {
	var env iso20022.Envelope
	if err := json.Unmarshal(d.Body, &env); err != nil {
		return broker.Permanent(fmt.Errorf("mensagem ilegível: %w", err))
	}
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attribute.String("pix.msg_type", env.AppHdr.MsgDefIdr))

	err := c.spi.Send(ctx, c.ispb, env)
	if err == nil {
		c.leak()
		if env.AppHdr.MsgDefIdr == iso20022.MsgPacs008 && env.FIToFICstmrCdtTrf != nil {
			return c.svc.MarkSent(ctx, env.FIToFICstmrCdtTrf.CdtTrfTxInf.PmtId.EndToEndId)
		}
		return nil
	}

	var apiErr *clients.APIError
	if !errors.As(err, &apiErr) || apiErr.Temporary() {
		return err
	}
	// O SPI disse não, e dizer de novo não muda a resposta.
	slog.ErrorContext(ctx, "SPI recusou a mensagem", "msg_type", env.AppHdr.MsgDefIdr, "status", apiErr.Status, "code", apiErr.Code, "message", apiErr.Message)
	span.SetStatus(codes.Error, apiErr.Error())
	if env.AppHdr.MsgDefIdr != iso20022.MsgPacs008 || env.FIToFICstmrCdtTrf == nil {
		return nil // pacs.002 recusada: nada a desfazer neste PSP
	}
	e2e := env.FIToFICstmrCdtTrf.CdtTrfTxInf.PmtId.EndToEndId
	if apiErr.Status == http.StatusConflict {
		// EndToEndId já em uso por outra mensagem: não tocar no pagamento.
		return nil
	}
	reason := iso20022.ReasonSettlementFailed
	if apiErr.Code == "UNKNOWN_PARTICIPANT" {
		reason = iso20022.ReasonInvalidAccount
	}
	return c.svc.Finish(ctx, e2e, StateRejected, reason, nil)
}

// Pull traz do SPI as mensagens destinadas a este PSP e as publica no
// RabbitMQ (pacs.008 em pix.incoming, pacs.002 em pix.results) até o contexto
// terminar. O lote só é confirmado ao SPI no pull seguinte, depois de todas
// as mensagens terem sido publicadas com confirm: se o processo cair no meio,
// o SPI reentrega o lote (at-least-once).
func (c *Connector) Pull(ctx context.Context) error {
	next := ""
	backoff := time.Second
	for ctx.Err() == nil {
		msgs, nextURL, err := c.spi.Pull(ctx, c.ispb, next, c.pollWait)
		if err == nil {
			err = c.forward(ctx, msgs)
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.WarnContext(ctx, "falha ao buscar mensagens do SPI", "err", err, "retry_in", backoff)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, 10*time.Second)
			continue // sem avançar o ack: o SPI reentrega quando o lease vencer
		}
		backoff = time.Second
		next = nextURL
	}
	return nil
}

func (c *Connector) forward(ctx context.Context, msgs []spi.Delivery) error {
	for _, m := range msgs {
		if err := c.forwardOne(ctx, m); err != nil {
			return err
		}
	}
	return nil
}

// forwardOne publica a mensagem continuando o trace de quem a originou no
// SPI (traceContext), para o trace seguir do SPI até o worker.
func (c *Connector) forwardOne(ctx context.Context, m spi.Delivery) error {
	ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(m.TraceContext))
	key := broker.QueueResults
	if m.Envelope.AppHdr.MsgDefIdr == iso20022.MsgPacs008 {
		key = broker.QueueIncoming
	}
	ctx, span := c.tracer.Start(ctx, "spi.deliver "+m.Envelope.AppHdr.MsgDefIdr, trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(attribute.String("messaging.destination.name", key)))
	defer span.End()

	body, err := json.Marshal(m.Envelope)
	if err != nil {
		return err
	}
	err = c.pub.Publish(ctx, broker.ExchangePix, key, broker.Message{ID: m.Envelope.AppHdr.BizMsgIdr, Body: body})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return err
}
