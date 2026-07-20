package model

import "time"

// WorklistItem é UMA nota que o agent já viu chegar e que ainda NÃO foi
// sincronizada (arrived_at ∧ ¬synced_at no tracker) — a "lista de separação" que
// o syncer executa SEM varrer o filesystem. file_path vem da observação de
// chegada (stage=arrival) gravada pelo agent. É o contrato compartilhado entre o
// store (produz), a API (serve) e o syncer (consome), então os json tags são a
// interface de rede: não renomeie sem casar os três lados.
type WorklistItem struct {
	Chave         string `json:"chave"`
	FilePath      string `json:"file_path"`
	CodigoEmpresa int    `json:"codigo_empresa"`
	CodigoFilial  int    `json:"codigo_filial"`
	DataEmissao   string `json:"data_emissao"`
}

// WorklistQuery são os filtros de uma consulta de worklist. Roots são CNPJ-base
// (8 primeiros dígitos): filtramos por CNPJ, NÃO por codigo_empresa, que é
// poluída pelo fan-out do SIEG (reflete a empresa que o poller escolheu, não a
// dona real). FilialMax>0 limita codigo_filial<=N; Since é o piso de emissão.
type WorklistQuery struct {
	Roots     []string  `json:"roots"`
	FilialMax int       `json:"filial_max"`
	Since     time.Time `json:"since"`
	Limit     int       `json:"limit"`
}
