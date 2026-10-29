package dict

import (
	"net/http"
	"regexp"
	"strconv"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/brenokcunha/light-pix/internal/httpx"
)

// handleAdminList lista os vínculos do diretório. É uma conveniência só do
// laboratório (a interface de testes monta a lista de chaves a partir dela): o
// DICT real não permite listar chaves, exatamente para impedir varredura. Fica
// sob /admin, junto com /admin/faults: sem autenticação e isento de falhas.
//
//	q            trecho da chave ou do nome do titular (sem diferenciar maiúsculas)
//	participant  ISPB do PSP da conta
//	branch, account  agência e número da conta
//	limit        no máximo 500 (padrão 200)
func (s *Server) handleAdminList(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	filter := bson.D{}
	if t := q.Get("q"); t != "" {
		re := bson.Regex{Pattern: regexp.QuoteMeta(t), Options: "i"}
		filter = append(filter, bson.E{Key: "$or", Value: bson.A{
			bson.D{{Key: "_id", Value: re}},
			bson.D{{Key: "owner.name", Value: re}},
		}})
	}
	for param, field := range map[string]string{
		"participant": "account.participant", "branch": "account.branch", "account": "account.accountNumber",
	} {
		if v := q.Get(param); v != "" {
			filter = append(filter, bson.E{Key: field, Value: v})
		}
	}
	limit := int64(200)
	if v, err := strconv.Atoi(q.Get("limit")); err == nil && v > 0 {
		limit = int64(min(v, 500))
	}
	cur, err := s.entries.Find(r.Context(), filter, options.Find().
		SetSort(bson.D{{Key: "owner.name", Value: 1}, {Key: "_id", Value: 1}}).SetLimit(limit))
	if err != nil {
		httpx.WriteInternal(w, r, err)
		return
	}
	out := []Entry{}
	if err := cur.All(r.Context(), &out); err != nil {
		httpx.WriteInternal(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, out)
}
