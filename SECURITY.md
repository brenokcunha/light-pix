# Política de segurança

O light-pix é um laboratório de estudo. Ele não é um PSP real e não deve ser
usado em produção nem com dados reais.

## Limitações conhecidas (por projeto)

- Sem autenticação nem autorização nas APIs, inclusive nas rotas `/admin/*`.
- Sem TLS entre os serviços, o MongoDB e o RabbitMQ.
- MongoDB sem autenticação; RabbitMQ e Grafana com a senha de demonstração `lightpix`.
- Rate limit apenas na consulta de chaves do DICT simulado.
- Os pods do chart Helm rodam sem `securityContext` (root, sistema de arquivos
  gravável), o que o Trivy aponta como KSV-0014 e KSV-0118.
- A interface de testes não envia cabeçalhos de segurança (CSP, X-Frame-Options etc.).

Se for expor qualquer parte fora da sua máquina, resolva isso antes.

## Reportar uma vulnerabilidade

Para falhas fora da lista acima, abra um
[security advisory privado](../../security/advisories/new) no GitHub, em vez de
uma issue pública. Respondo assim que possível.
