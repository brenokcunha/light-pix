package pix

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestParseAmount(t *testing.T) {
	ok := map[string]Amount{"0": 0, "12": 1200, "12.3": 1230, "12.34": 1234, "0.01": 1, "-5.5": -550}
	for in, want := range ok {
		got, err := ParseAmount(in)
		if err != nil || got != want {
			t.Errorf("ParseAmount(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"", "1.234", "1.", ".5", "1e3", "abc", "1,5"} {
		if _, err := ParseAmount(in); err == nil {
			t.Errorf("ParseAmount(%q) deveria falhar", in)
		}
	}
}

func TestAmountJSON(t *testing.T) {
	var v struct{ Value Amount }
	if err := json.Unmarshal([]byte(`{"Value": 100.5}`), &v); err != nil || v.Value != 10050 {
		t.Fatalf("unmarshal = %d, %v", v.Value, err)
	}
	b, _ := json.Marshal(v)
	if string(b) != `{"Value":100.50}` {
		t.Fatalf("marshal = %s", b)
	}
}

func TestEndToEndID(t *testing.T) {
	id := NewEndToEndID("11111111", time.Date(2026, 10, 2, 15, 4, 0, 0, time.UTC))
	if len(id) != 32 || !strings.HasPrefix(id, "E111111112026100215") {
		t.Fatalf("id = %s", id)
	}
	ispb, err := ParseEndToEndID(id)
	if err != nil || ispb != "11111111" {
		t.Fatalf("ParseEndToEndID = %q, %v", ispb, err)
	}
	for _, bad := range []string{"", "X11111111202610021504abcdefghijk", "E11111111202613021504abcdefghijk", "E1111111120261002150abcdefghijk"} {
		if _, err := ParseEndToEndID(bad); err == nil {
			t.Errorf("ParseEndToEndID(%q) deveria falhar", bad)
		}
	}
}

func TestValidateKey(t *testing.T) {
	ok := []struct {
		t   KeyType
		key string
	}{
		{KeyCPF, "52998224725"},
		{KeyCNPJ, "11222333000181"},
		{KeyEmail, "alice@example.com"},
		{KeyPhone, "+5511999990001"},
		{KeyEVP, NewEVP()},
	}
	for _, c := range ok {
		if err := ValidateKey(c.t, c.key); err != nil {
			t.Errorf("ValidateKey(%s, %q) = %v", c.t, c.key, err)
		}
	}
	bad := []struct {
		t   KeyType
		key string
	}{
		{KeyCPF, "52998224724"},
		{KeyCPF, "11111111111"},
		{KeyCNPJ, "11222333000180"},
		{KeyEmail, "Alice@Example.com"},
		{KeyPhone, "11999990001"},
		{KeyEVP, "not-a-uuid"},
		{"PIX", "x"},
	}
	for _, c := range bad {
		if err := ValidateKey(c.t, c.key); err == nil {
			t.Errorf("ValidateKey(%s, %q) deveria falhar", c.t, c.key)
		}
	}
}

func TestReturnID(t *testing.T) {
	now := time.Date(2026, 10, 2, 15, 4, 0, 0, time.UTC)
	id := NewReturnID("22222222", now)
	if len(id) != 32 || id[0] != 'D' {
		t.Fatalf("id = %q", id)
	}
	if ispb, err := ParseReturnID(id); err != nil || ispb != "22222222" {
		t.Fatalf("ParseReturnID = %q, %v", ispb, err)
	}
	if _, err := ParseReturnID(NewEndToEndID("22222222", now)); err == nil {
		t.Fatal("um EndToEndId não é identificador de devolução")
	}
	if _, err := ParseEndToEndID(id); err == nil {
		t.Fatal("um identificador de devolução não é EndToEndId")
	}
	for _, c := range []struct {
		id     string
		isRet  bool
		wantOK bool
	}{
		{id, true, true},
		{NewEndToEndID("11111111", now), false, true},
		{"X123", false, false},
		{"D22222222999999999999abcdefghijk", true, false}, // data inválida
	} {
		ispb, isRet, err := ParseTransactionID(c.id)
		if (err == nil) != c.wantOK || (c.wantOK && (isRet != c.isRet || ispb == "")) {
			t.Errorf("ParseTransactionID(%q) = %q, %v, %v", c.id, ispb, isRet, err)
		}
	}
}
