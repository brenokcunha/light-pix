package pix

import (
	"crypto/rand"
	"fmt"
	"regexp"
	"time"
)

// ISPB do Banco Central, destinatário/remetente das mensagens do SPI.
const BCBISPB = "00038166"

var ispbRe = regexp.MustCompile(`^\d{8}$`)

// ValidISPB verifica o formato do ISPB (8 dígitos).
func ValidISPB(s string) bool { return ispbRe.MatchString(s) }

// O EndToEndId tem 32 caracteres: "E" + ISPB do pagador + data/hora UTC
// (AAAAMMDDHHMM) + 11 caracteres alfanuméricos únicos no minuto.
var e2eRe = regexp.MustCompile(`^E(\d{8})(\d{12})([a-zA-Z0-9]{11})$`)

const alnum = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

// NewEndToEndID gera o identificador da transação do lado do pagador.
func NewEndToEndID(ispb string, now time.Time) string {
	return "E" + ispb + now.UTC().Format("200601021504") + randomAlnum(11)
}

// NewMsgID gera o identificador de mensagem (MsgId/BizMsgIdr): "M" + ISPB do
// emissor + 23 caracteres alfanuméricos.
func NewMsgID(ispb string) string { return "M" + ispb + randomAlnum(23) }

func randomAlnum(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = alnum[randIndex(len(alnum))]
	}
	return string(b)
}

// ParseEndToEndID valida o formato e devolve o ISPB do pagador.
func ParseEndToEndID(id string) (ispb string, err error) {
	m := e2eRe.FindStringSubmatch(id)
	if m == nil {
		return "", fmt.Errorf("pix: EndToEndId inválido: %q", id)
	}
	if _, err := time.Parse("200601021504", m[2]); err != nil {
		return "", fmt.Errorf("pix: EndToEndId com data inválida: %q", id)
	}
	return m[1], nil
}

// randIndex sorteia em [0, n) sem viés (rejeita bytes acima do maior múltiplo de n).
func randIndex(n int) int {
	limit := 256 - 256%n
	var b [1]byte
	for {
		_, _ = rand.Read(b[:])
		if int(b[0]) < limit {
			return int(b[0]) % n
		}
	}
}
