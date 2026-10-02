// Comando dict-sim: simula o DICT (vínculos de chave Pix e consulta com
// rate limit por usuário pagador).
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/brenokcunha/light-pix/internal/app"
	"github.com/brenokcunha/light-pix/internal/config"
	"github.com/brenokcunha/light-pix/internal/sim/dict"
)

// version é preenchida no build via -ldflags.
var version = "dev"

func main() {
	app.Main(app.Sim{Name: "dict-sim", Version: version, DB: "dict", Setup: setup})
}

func setup(ctx context.Context, client *mongo.Client, db string, mux *http.ServeMux) (func(context.Context) error, error) {
	var cfg dict.Config
	var err error
	for _, f := range []struct {
		dst *float64
		key string
		def float64
	}{
		{&cfg.BucketCapacity, "DICT_PAYER_BUCKET_CAPACITY", 100},
		{&cfg.RefillPerMinute, "DICT_PAYER_REFILL_PER_MIN", 60},
		{&cfg.NotFoundCost, "DICT_NOT_FOUND_COST", 20},
	} {
		if *f.dst, err = config.Float(f.key, f.def); err != nil {
			return nil, err
		}
	}
	s, err := dict.New(ctx, client, db, cfg)
	if err != nil {
		return nil, err
	}
	if path := config.String("DICT_SEED_FILE", ""); path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		var entries []dict.Entry
		if err := json.Unmarshal(b, &entries); err != nil {
			return nil, fmt.Errorf("DICT_SEED_FILE: %w", err)
		}
		if err := s.Seed(ctx, entries); err != nil {
			return nil, err
		}
	}
	s.Routes(mux)
	return nil, nil
}
