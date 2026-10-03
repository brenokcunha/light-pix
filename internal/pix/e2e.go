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

// O identificador da devolução tem o mesmo formato do EndToEndId, com "D" no
// lugar do "E" e o ISPB de quem devolve (o recebedor do Pix original).
var returnRe = regexp.MustCompile(`^D(\d{8})(\d{12})([a-zA-Z0-9]{11})$`)

const alnum = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

// NewEndToEndID gera o identificador da transação do lado do pagador.
func NewEndToEndID(ispb string, now time.Time) string {
	return "E" + ispb + now.UTC().Format("200601021504") + randomAlnum(11)
}

// NewReturnID gera o identificador de uma devolução, do lado de quem devolve.
func NewReturnID(ispb string, now time.Time) string {
	return "D" + ispb + now.UTC().Format("200601021504") + randomAlnum(11)
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

// ParseReturnID valida o formato do identificador de devolução e devolve o
// ISPB de quem devolve.
func ParseReturnID(id string) (ispb string, err error) {
	m := returnRe.FindStringSubmatch(id)
	if m == nil {
		return "", fmt.Errorf("pix: identificador de devolução inválido: %q", id)
	}
	if _, err := time.Parse("200601021504", m[2]); err != nil {
		return "", fmt.Errorf("pix: identificador de devolução com data inválida: %q", id)
	}
	return m[1], nil
}

// IsReturnID diz se o identificador é de uma devolução (começa com "D").
func IsReturnID(id string) bool { return len(id) > 0 && id[0] == 'D' }

// ParseTransactionID aceita o EndToEndId de um Pix ("E...") ou o identificador
// de uma devolução ("D...") e devolve o ISPB de quem originou.
func ParseTransactionID(id string) (ispb string, isReturn bool, err error) {
	if IsReturnID(id) {
		ispb, err = ParseReturnID(id)
		return ispb, true, err
	}
	ispb, err = ParseEndToEndID(id)
	return ispb, false, err
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
