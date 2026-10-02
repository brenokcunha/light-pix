package pix

import (
	"crypto/rand"
	"fmt"
	"regexp"
	"strings"
)

// KeyType é o tipo de chave Pix, com os nomes usados no DICT.
type KeyType string

const (
	KeyCPF   KeyType = "CPF"
	KeyCNPJ  KeyType = "CNPJ"
	KeyEmail KeyType = "EMAIL"
	KeyPhone KeyType = "PHONE"
	KeyEVP   KeyType = "EVP"
)

var (
	emailRe = regexp.MustCompile(`^[a-z0-9.!#$&'*+/=?^_` + "`" + `{|}~-]+@[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)*$`)
	phoneRe = regexp.MustCompile(`^\+[1-9]\d{1,14}$`)
	evpRe   = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
)

// ValidateKey confere se a chave tem o formato do seu tipo. E-mail é
// comparado em minúsculas, como no DICT.
func ValidateKey(t KeyType, key string) error {
	ok := false
	switch t {
	case KeyCPF:
		ok = ValidCPF(key)
	case KeyCNPJ:
		ok = ValidCNPJ(key)
	case KeyEmail:
		ok = len(key) <= 77 && emailRe.MatchString(key)
	case KeyPhone:
		ok = phoneRe.MatchString(key)
	case KeyEVP:
		ok = evpRe.MatchString(key)
	default:
		return fmt.Errorf("pix: tipo de chave desconhecido: %q", t)
	}
	if !ok {
		return fmt.Errorf("pix: chave %s inválida", t)
	}
	return nil
}

// NormalizeKey aplica a forma canônica (hoje, só e-mail em minúsculas).
func NormalizeKey(t KeyType, key string) string {
	if t == KeyEmail {
		return strings.ToLower(key)
	}
	return key
}

// NewEVP gera uma chave aleatória (UUID v4).
func NewEVP() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// ValidCPF confere formato (11 dígitos) e dígitos verificadores.
func ValidCPF(s string) bool {
	if len(s) != 11 || !digits(s) || allSame(s) {
		return false
	}
	return checkDigit(s[:9], weights(10, 9)) == s[9] && checkDigit(s[:10], weights(11, 10)) == s[10]
}

// ValidCNPJ confere formato (14 dígitos) e dígitos verificadores. O CNPJ
// alfanumérico (a partir de 2026) fica fora do laboratório.
func ValidCNPJ(s string) bool {
	if len(s) != 14 || !digits(s) || allSame(s) {
		return false
	}
	w1 := []int{5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2}
	w2 := append([]int{6}, w1...)
	return checkDigit(s[:12], w1) == s[12] && checkDigit(s[:13], w2) == s[13]
}

// checkDigit calcula o dígito verificador módulo 11 usado por CPF e CNPJ.
func checkDigit(s string, w []int) byte {
	sum := 0
	for i := range s {
		sum += int(s[i]-'0') * w[i]
	}
	d := 11 - sum%11
	if d >= 10 {
		d = 0
	}
	return byte('0' + d)
}

// weights devolve n pesos decrescentes a partir de start (CPF: 10..2, 11..2).
func weights(start, n int) []int {
	w := make([]int, n)
	for i := range w {
		w[i] = start - i
	}
	return w
}

func allSame(s string) bool { return strings.Count(s, s[:1]) == len(s) }
