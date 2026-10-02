package pix

// Headers HTTP das interfaces do Bacen (DICT e SPI).
const (
	// HeaderRequestingParticipant identifica o ISPB de quem chama. No ambiente
	// real a identidade vem do certificado mTLS; aqui, do header.
	HeaderRequestingParticipant = "PI-RequestingParticipant"
	// HeaderPayerID é o CPF/CNPJ do usuário pagador na consulta ao DICT, base
	// do rate limit por usuário.
	HeaderPayerID = "PI-PayerId"
	// HeaderEndToEndID liga a consulta ao DICT ao pagamento que ela inicia.
	HeaderEndToEndID = "PI-EndToEndId"
	// HeaderPullNext devolve, no stream do SPI, a URL do próximo pull. Usá-la
	// confirma o recebimento do lote anterior.
	HeaderPullNext = "PI-PULL-NEXT"
)
