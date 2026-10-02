// Package pix reúne as regras de domínio do Pix usadas por PSPs e simuladores:
// valores, EndToEndId, chaves e participantes.
package pix

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Amount é um valor em centavos. Dinheiro nunca passa por float: no JSON ele
// aparece como número decimal com até duas casas (12.34) e é convertido
// direto do texto.
type Amount int64

// ErrInvalidAmount indica um valor mal formado (mais de duas casas, texto...).
var ErrInvalidAmount = errors.New("pix: valor inválido")

func (a Amount) String() string {
	sign, v := "", int64(a)
	if v < 0 {
		sign, v = "-", -v
	}
	return fmt.Sprintf("%s%d.%02d", sign, v/100, v%100)
}

// MarshalJSON escreve o valor como número decimal.
func (a Amount) MarshalJSON() ([]byte, error) { return []byte(a.String()), nil }

// UnmarshalJSON aceita número JSON (ou texto) com até duas casas decimais.
func (a *Amount) UnmarshalJSON(b []byte) error {
	v, err := ParseAmount(strings.Trim(string(b), `"`))
	if err != nil {
		return err
	}
	*a = v
	return nil
}

// ParseAmount converte "12", "12.3" ou "12.34" em centavos.
func ParseAmount(s string) (Amount, error) {
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	whole, frac, hasFrac := strings.Cut(s, ".")
	if whole == "" || len(frac) > 2 || (hasFrac && frac == "") || !digits(whole) || !digits(frac) {
		return 0, fmt.Errorf("%w: %q", ErrInvalidAmount, s)
	}
	frac += strings.Repeat("0", 2-len(frac))
	v, err := strconv.ParseInt(whole+frac, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: %q", ErrInvalidAmount, s)
	}
	if neg {
		v = -v
	}
	return Amount(v), nil
}

func digits(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
