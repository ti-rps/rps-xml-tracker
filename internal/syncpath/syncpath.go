// Package syncpath deriva o caminho relativo (coluna URL da TABLISTACHAVEACESSO)
// onde o XML de uma nota vive dentro do "XML SINCRONIZADO". É uma FUNÇÃO PURA —
// nenhum I/O — para ser validada em massa contra URLs reais (repoll --check-path)
// antes de o syncer usá-la para escrever de verdade (F1).
//
// Hipótese do padrão (do exemplo real observado):
//
//	\<NOME_EMPRESA>\<CNPJ_FILIAL_14>\<TIPODOC>\<ENTRADA|SAIDA>\<AAAAMM>\<CHAVE>.xml
//
// O 2º segmento é o CNPJ da FILIAL DONA da participação (não o do emitente): numa
// saída eles coincidem porque a própria empresa emitiu; é a ENTRADA que os separa.
// O 5º segmento assume AAAAMM da DATA DE EMISSÃO — o --check-path mede emissão vs
// inclusão e decide (casos de virada de mês).
package syncpath

import (
	"fmt"
	"strings"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
)

// Input são os fatos de UMA PARTICIPAÇÃO (chave + empresa) necessários para a
// derivação. Vêm do parse do XML + resolução no Firebird (TABEMPRESAS/TABFILIAL).
type Input struct {
	NomeEmpresa string        // TABEMPRESAS.NOME da empresa cliente (dona da pasta)
	CnpjFilial  string        // CNPJ da filial dona da participação; não-dígitos são ignorados
	DocType     model.DocType // classificação do tracker (NFe/NFCe/CTe/NFS)
	Direction   string        // model.DirEntrada | model.DirSaida (lado da EMPRESA, não do documento)
	DataEmissao string        // yyyy-mm-dd (dhEmi/dEmi do XML ou DATAEMISSAO da linha)
	Chave       string        // 44 dígitos
}

// SegmentNames nomeia as posições do caminho, na ordem — usado nos relatórios do
// --check-path para dizer QUAL segmento divergiu.
var SegmentNames = []string{"empresa", "cnpj_filial", "tipo_doc", "direcao", "competencia", "arquivo"}

// Derive monta o caminho relativo no formato das URLs reais: prefixo "\" e
// separador "\" (é um caminho Windows relativo à raiz do SINCRONIZADO). Erro em
// qualquer campo fora do padrão — o piloto NÃO chuta: nota que não deriva
// limpo não é sincronizada.
func Derive(in Input) (string, error) {
	nome := SanitizeSegment(in.NomeEmpresa)
	if nome == "" {
		return "", fmt.Errorf("syncpath: nome da empresa vazio (ou só caracteres inválidos NTFS): %q", in.NomeEmpresa)
	}
	cnpj := digits(in.CnpjFilial)
	if len(cnpj) != 14 {
		return "", fmt.Errorf("syncpath: CNPJ da filial precisa ter 14 dígitos: %q", in.CnpjFilial)
	}
	doc, err := DocSegment(in.DocType)
	if err != nil {
		return "", err
	}
	dir, err := DirSegment(in.Direction)
	if err != nil {
		return "", err
	}
	comp, err := Competencia(in.DataEmissao)
	if err != nil {
		return "", err
	}
	if !chave44(in.Chave) {
		return "", fmt.Errorf("syncpath: chave precisa ter 44 dígitos: %q", in.Chave)
	}
	return `\` + strings.Join([]string{nome, cnpj, doc, dir, comp, in.Chave + ".xml"}, `\`), nil
}

// DocSegment mapeia a classificação do tracker para o segmento de diretório
// observado nas URLs reais. EVENTO/UNKNOWN dão erro: o padrão de eventos/CCe
// nas URLs é outro (a confirmar no --check-path) e o piloto não os cobre.
func DocSegment(dt model.DocType) (string, error) {
	switch dt {
	case model.DocNFe:
		return "NFe", nil
	case model.DocNFCe:
		return "NFCe", nil
	case model.DocCTe:
		return "CTe", nil
	case model.DocNFS:
		return "NFSe", nil
	}
	return "", fmt.Errorf("syncpath: doc_type %q fora do escopo da derivação", dt)
}

// DirSegment mapeia a direção do tracker para o segmento ENTRADA/SAIDA.
func DirSegment(direction string) (string, error) {
	switch direction {
	case model.DirEntrada:
		return "ENTRADA", nil
	case model.DirSaida:
		return "SAIDA", nil
	}
	return "", fmt.Errorf("syncpath: direção indeterminada (%q) — sem lado da empresa não há pasta", direction)
}

// Competencia converte yyyy-mm-dd em AAAAMM.
func Competencia(dataEmissao string) (string, error) {
	if len(dataEmissao) < 7 || dataEmissao[4] != '-' {
		return "", fmt.Errorf("syncpath: data de emissão inválida (esperado yyyy-mm-dd): %q", dataEmissao)
	}
	comp := dataEmissao[:4] + dataEmissao[5:7]
	if digits(comp) != comp {
		return "", fmt.Errorf("syncpath: data de emissão inválida (esperado yyyy-mm-dd): %q", dataEmissao)
	}
	return comp, nil
}

// SanitizeSegment reproduz como o DownloadXML monta o nome da pasta da empresa a
// partir de TABEMPRESAS.NOME. Regras confirmadas empiricamente no repoll
// --check-path (5000 URLs reais, jul/2026):
//   - "&" vira "e" (ex.: NOME "J MARCOS ALVES TRINDADE & CIA LTDA" -> pasta
//     "...TRINDADE e CIA LTDA");
//   - caracteres reservados do NTFS (< > : " / \ | ? *) e controles (0x00-0x1F)
//     são removidos;
//   - ponto/espaço finais são cortados (NTFS não os mantém).
//
// Divergências residuais (~1-2%) são notas cuja pasta foi criada com um NOME que
// depois foi editado na TABEMPRESAS — não são erro de derivação (go-forward casa
// o NOME vigente). Se o --check-path revelar outra transformação, ela entra aqui
// com um caso de teste usando a URL real.
func SanitizeSegment(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r == '&':
			b.WriteRune('e')
		case r < 0x20:
		case strings.ContainsRune(`<>:"/\|?*`, r):
		default:
			b.WriteRune(r)
		}
	}
	return strings.TrimRight(strings.TrimSpace(b.String()), ". ")
}

// Segments quebra uma URL real (ou derivada) nos segmentos, ignorando o "\"
// inicial. Não valida o tamanho — o chamador compara com SegmentNames.
func Segments(p string) []string {
	p = strings.TrimPrefix(strings.TrimSpace(p), `\`)
	if p == "" {
		return nil
	}
	return strings.Split(p, `\`)
}

func digits(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func chave44(s string) bool {
	return len(s) == 44 && digits(s) == s
}
