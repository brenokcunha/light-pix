package psp

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/brenokcunha/light-pix/internal/outbox"
	"github.com/brenokcunha/light-pix/internal/pix"
	"github.com/brenokcunha/light-pix/internal/store"
)

// idempotencyTTL é por quanto tempo uma Idempotency-Key é lembrada.
const idempotencyTTL = 24 * time.Hour

// Store reúne as coleções do banco do PSP (um database por PSP).
type Store struct {
	client   *mongo.Client
	accounts *mongo.Collection
	ledger   *mongo.Collection
	payments *mongo.Collection
	idem     *mongo.Collection
	outbox   *mongo.Collection
}

// NewStore prepara as coleções e cria os índices.
func NewStore(ctx context.Context, client *mongo.Client, db *mongo.Database) (*Store, error) {
	s := &Store{
		client:   client,
		accounts: db.Collection("accounts"),
		ledger:   db.Collection("ledger_entries"),
		payments: db.Collection("payments"),
		idem:     db.Collection("idempotency_keys"),
		outbox:   outbox.Collection(db),
	}
	idx := []struct {
		coll   *mongo.Collection
		models []mongo.IndexModel
	}{
		{s.accounts, []mongo.IndexModel{
			{Keys: bson.D{{Key: "branch", Value: 1}, {Key: "number", Value: 1}}, Options: options.Index().SetUnique(true)},
		}},
		{s.ledger, []mongo.IndexModel{
			{Keys: bson.D{{Key: "accountId", Value: 1}, {Key: "endToEndId", Value: 1}, {Key: "kind", Value: 1}}, Options: options.Index().SetUnique(true)},
			{Keys: bson.D{{Key: "accountId", Value: 1}, {Key: "createdAt", Value: 1}}},
		}},
		{s.payments, []mongo.IndexModel{
			// Para o pending-resolver achar os não terminais parados.
			{Keys: bson.D{{Key: "state", Value: 1}, {Key: "updatedAt", Value: 1}}},
		}},
		{s.idem, []mongo.IndexModel{
			{Keys: bson.D{{Key: "scope", Value: 1}, {Key: "key", Value: 1}}, Options: options.Index().SetUnique(true)},
			{Keys: bson.D{{Key: "createdAt", Value: 1}}, Options: options.Index().SetExpireAfterSeconds(int32(idempotencyTTL.Seconds()))},
		}},
	}
	for _, i := range idx {
		if _, err := i.coll.Indexes().CreateMany(ctx, i.models); err != nil {
			return nil, fmt.Errorf("psp: índices de %s: %w", i.coll.Name(), err)
		}
	}
	if err := outbox.EnsureIndexes(ctx, s.outbox); err != nil {
		return nil, err
	}
	return s, nil
}

// SeedAccount descreve uma conta da carga inicial.
type SeedAccount struct {
	ISPB    string     `json:"ispb"`
	Branch  string     `json:"branch"`
	Number  string     `json:"number"`
	Type    string     `json:"type"`
	Holder  Holder     `json:"holder"`
	Balance pix.Amount `json:"balance"`
}

// SeedAccounts cria as contas de ispb que ainda não existem, com o saldo
// inicial lançado no razão (SEED). Repetir não muda nada.
func (s *Store) SeedAccounts(ctx context.Context, ispb, path string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var seeds []SeedAccount
	if err := json.Unmarshal(b, &seeds); err != nil {
		return fmt.Errorf("psp: carga de contas %s: %w", path, err)
	}
	for _, sa := range seeds {
		if sa.ISPB != ispb {
			continue
		}
		now := store.Now()
		acc := Account{
			ID: bson.NewObjectID(), Branch: sa.Branch, Number: sa.Number, Type: sa.Type, Holder: sa.Holder,
			Balance: Balance{Available: sa.Balance}, Status: AccountActive, CreatedAt: now, UpdatedAt: now,
		}
		err := store.Transact(ctx, s.client, func(ctx context.Context) error {
			if _, err := s.accounts.InsertOne(ctx, acc); err != nil {
				return err
			}
			_, err := s.ledger.InsertOne(ctx, LedgerEntry{
				ID: bson.NewObjectID(), AccountID: acc.ID, EndToEndID: "seed", Kind: EntrySeed,
				AvailableDelta: sa.Balance, CreatedAt: now,
			})
			return err
		})
		switch {
		case mongo.IsDuplicateKeyError(err):
			continue
		case err != nil:
			return fmt.Errorf("psp: seed da conta %s/%s: %w", sa.Branch, sa.Number, err)
		}
		slog.InfoContext(ctx, "conta criada", "branch", sa.Branch, "number", sa.Number, "balance", sa.Balance.String())
	}
	return nil
}

// FindAccount busca a conta por agência e número.
func (s *Store) FindAccount(ctx context.Context, branch, number string) (Account, error) {
	var acc Account
	err := s.accounts.FindOne(ctx, bson.D{{Key: "branch", Value: branch}, {Key: "number", Value: number}}).Decode(&acc)
	return acc, err
}

// FindPayment busca o pagamento pelo EndToEndId.
func (s *Store) FindPayment(ctx context.Context, endToEndID string) (Payment, error) {
	var p Payment
	err := s.payments.FindOne(ctx, bson.D{{Key: "_id", Value: endToEndID}}).Decode(&p)
	return p, err
}
