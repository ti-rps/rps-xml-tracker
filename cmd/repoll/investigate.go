package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/EnzzoHosaki/rps-xml-tracker/internal/firebird"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/model"
	"github.com/EnzzoHosaki/rps-xml-tracker/internal/syncpath"
)

const investTable = "TABLISTACHAVEACESSO"

// investSince resolve a janela dos modos de investigação: --since YYYY-MM-DD ou,
// vazio, os últimos `defDays` dias.
func investSince(since string, defDays int) time.Time {
	if since == "" {
		return time.Now().AddDate(0, 0, -defDays)
	}
	return mustDate(since, "since")
}

// runProfileInsert é o §5/§6 do design: perfila a TABLISTACHAVEACESSO para revelar
// o INSERT mínimo compatível com o DownloadXML, o mecanismo da PK, a prevalência do
// multi-participação e a janela do robô. Só imprime — nada é escrito.
func runProfileInsert(ctx context.Context, rd *firebird.Reader, since string, sample int) {
	from := investSince(since, 7)
	log.Printf("=== profile-insert (READ-ONLY) — janela DATAINCLUSAO >= %s ===", from.Format("2006-01-02"))

	cols, err := rd.TableColumns(ctx, investTable)
	if err != nil {
		log.Fatalf("profile-insert: DDL: %v", err)
	}
	present := map[string]bool{}
	names := make([]string, len(cols))
	for i, c := range cols {
		present[c.Name] = true
		names[i] = c.Name
	}
	log.Printf("--- DDL efetivo (%d colunas) ---", len(cols))
	for _, c := range cols {
		nn := ""
		if c.NotNull {
			nn = " NOT NULL"
		}
		log.Printf("  %-28s %s%s", c.Name, c.Type, nn)
	}

	reportFill(ctx, rd, names, from, "")
	if present["TIPODOCUMENTO"] {
		reportFill(ctx, rd, names, from, "TIPODOCUMENTO")
	}
	if present["TIPO"] {
		reportFill(ctx, rd, names, from, "TIPO")
	}

	reportPKMechanism(ctx, rd)
	reportMultiPart(ctx, rd, from, sample)
	reportImportWindow(ctx, rd)
	reportPendingVsImported(ctx, rd, present)
	reportDistincts(ctx, rd, present, from)
	log.Printf("=== fim do profile-insert ===")
}

// reportFill imprime o fill-rate por coluna (global ou quebrado por groupBy),
// classificando em SEMPRE (candidata ao INSERT mínimo) / ÀS VEZES / NUNCA.
func reportFill(ctx context.Context, rd *firebird.Reader, cols []string, from time.Time, groupBy string) {
	groups, err := rd.FillProfile(ctx, investTable, cols, from, groupBy)
	if err != nil {
		log.Printf("fill-profile (groupBy=%q): ERRO %v", groupBy, err)
		return
	}
	if groupBy == "" {
		log.Printf("--- fill-rate global ---")
	} else {
		log.Printf("--- fill-rate por %s ---", groupBy)
	}
	for _, g := range groups {
		label := "GLOBAL"
		if groupBy != "" {
			label = g.Group
			if label == "" {
				label = "(NULL)"
			}
		}
		if g.Total == 0 {
			log.Printf("  [%s] 0 linhas", label)
			continue
		}
		var always, sometimes, never []string
		for _, c := range cols {
			f := g.Filled[c]
			switch {
			case f == g.Total:
				always = append(always, c)
			case f == 0:
				never = append(never, c)
			default:
				sometimes = append(sometimes, colPct(c, f, g.Total))
			}
		}
		sort.Strings(always)
		sort.Strings(never)
		sort.Strings(sometimes)
		log.Printf("  [%s] total=%d", label, g.Total)
		log.Printf("    SEMPRE (%d): %s", len(always), strings.Join(always, ", "))
		if len(sometimes) > 0 {
			log.Printf("    ÀS VEZES (%d): %s", len(sometimes), strings.Join(sometimes, ", "))
		}
		if len(never) > 0 {
			log.Printf("    NUNCA (%d): %s", len(never), strings.Join(never, ", "))
		}
	}
}

func colPct(col string, filled, total int64) string {
	if total == 0 {
		return col + " 0%"
	}
	return col + " " + strconv.FormatInt(filled*100/total, 10) + "%"
}

// reportPKMechanism descobre como a PK CODIGO_CHAVEACESSO é gerada: trigger
// BEFORE INSERT com GEN_ID, ou app client-side (MAX(PK) ~= valor do generator).
func reportPKMechanism(ctx context.Context, rd *firebird.Reader) {
	log.Printf("--- mecanismo da PK / generator ---")
	trigs, err := rd.TableTriggers(ctx, investTable)
	if err != nil {
		log.Printf("  triggers: ERRO %v", err)
	} else if len(trigs) == 0 {
		log.Printf("  nenhuma trigger de usuário na tabela")
	} else {
		for _, t := range trigs {
			state := "ATIVA"
			if t.Inactive {
				state = "inativa"
			}
			log.Printf("  trigger %s [%s]:", t.Name, state)
			for _, line := range strings.Split(t.Source, "\n") {
				if s := strings.TrimRight(line, "\r "); s != "" {
					log.Printf("      %s", s)
				}
			}
		}
	}
	gens, err := rd.Generators(ctx, "CHAVE")
	if err != nil {
		log.Printf("  generators: ERRO %v", err)
	}
	for _, g := range gens {
		v, err := rd.GeneratorValue(ctx, g)
		if err != nil {
			log.Printf("  generator %s: valor ERRO %v", g, err)
			continue
		}
		log.Printf("  generator %s = %d (valor atual, sem consumir)", g, v)
	}
	if mx, err := rd.MaxBigint(ctx, investTable, "CODIGO_CHAVEACESSO"); err != nil {
		log.Printf("  MAX(CODIGO_CHAVEACESSO): ERRO %v", err)
	} else if mx != nil {
		log.Printf("  MAX(CODIGO_CHAVEACESSO) = %d (compare com o generator p/ ver se batem)", *mx)
	}
}

func reportMultiPart(ctx context.Context, rd *firebird.Reader, from time.Time, sample int) {
	log.Printf("--- multi-participação (chaves com 2+ empresas) ---")
	st, err := rd.MultiParticipacao(ctx, from)
	if err != nil {
		log.Printf("  ERRO %v", err)
		return
	}
	share := int64(0)
	if st.ChavesTotal > 0 {
		share = st.ChavesMulti * 100 / st.ChavesTotal
	}
	log.Printf("  chaves na janela=%d, multi-empresa=%d (%d%%)", st.ChavesTotal, st.ChavesMulti, share)
	type kv struct {
		k string
		v int64
	}
	combos := make([]kv, 0, len(st.Combos))
	for k, v := range st.Combos {
		combos = append(combos, kv{k, v})
	}
	sort.Slice(combos, func(i, j int) bool { return combos[i].v > combos[j].v })
	for _, c := range combos {
		log.Printf("    %-40s %d chaves", c.k, c.v)
	}
	urls, err := rd.MultiParticipacaoURLs(ctx, from, sample/10)
	if err != nil {
		log.Printf("  URLs irmãs: ERRO %v", err)
		return
	}
	log.Printf("  amostra de URLs por chave multi-empresa (as URLs divergem? = 1 cópia física por empresa):")
	shown, lastChave := 0, ""
	for _, u := range urls {
		if u.Chave != lastChave {
			if shown >= 12 {
				break
			}
			lastChave = u.Chave
			shown++
			log.Printf("    chave %s:", u.Chave)
		}
		log.Printf("      emp %-6s tipo %-2s url=%s", derefIntStr(u.CodigoEmpresa), u.Tipo, u.URL)
	}
}

func reportImportWindow(ctx context.Context, rd *firebird.Reader) {
	log.Printf("--- janela do AthenasHorse (meses emissão→importação, importadas últimos 180d) ---")
	buckets, err := rd.ImportWindowMeses(ctx, time.Now().AddDate(0, 0, -180))
	if err != nil {
		log.Printf("  ERRO %v", err)
		return
	}
	for _, b := range buckets {
		m := "(emissão NULL)"
		if b.Meses != nil {
			m = strconv.Itoa(*b.Meses) + " mês(es)"
		}
		log.Printf("    %-16s %d", m, b.Count)
	}
	log.Printf("  (tese fiscal: só 0 e 1 mês; caudas indicam exceções ou emissão retroativa)")
}

func reportPendingVsImported(ctx context.Context, rd *firebird.Reader, present map[string]bool) {
	for _, c := range firebird.PendingVsImportedCols {
		if !present[c] {
			log.Printf("--- pendente×importada: coluna %s ausente nesta versão, pulando ---", c)
			return
		}
	}
	log.Printf("--- pendente×importada dentro da janela (emissão últimos 60d, não-ignoradas) ---")
	combos, err := rd.PendingVsImported(ctx, time.Now().AddDate(0, 0, -60))
	if err != nil {
		log.Printf("  ERRO %v", err)
		return
	}
	log.Printf("  IMPORTADO | %s | count", strings.Join(firebird.PendingVsImportedCols, " | "))
	for i, cc := range combos {
		if i >= 40 {
			log.Printf("  ... e mais %d combinações", len(combos)-40)
			break
		}
		vals := make([]string, len(cc.Values))
		for j, v := range cc.Values {
			vals[j] = derefStr(v)
		}
		log.Printf("    %d | %s | %d", cc.Importado, strings.Join(vals, " | "), cc.Count)
	}
}

func reportDistincts(ctx context.Context, rd *firebird.Reader, present map[string]bool, from time.Time) {
	log.Printf("--- valores distintos de colunas de semântica desconhecida ---")
	for _, col := range []string{"ORIGEM", "SITUACAO", "DOWNLOAD", "TIPO", "TIPODOCUMENTO", "CODIGOTIPOMOVIMENTO", "ORDEMATHENAS"} {
		if !present[col] {
			continue
		}
		vals, err := rd.DistinctCounts(ctx, investTable, col, from)
		if err != nil {
			log.Printf("  %s: ERRO %v", col, err)
			continue
		}
		log.Printf("  %s:", col)
		for i, v := range vals {
			if i >= 20 {
				log.Printf("    ... e mais %d valores", len(vals)-20)
				break
			}
			log.Printf("    %-24s %d", derefStr(v.Value), v.Count)
		}
	}
}

// runWatchChave faz polling de uma chave e imprime a linha inteira quando aparece
// e o diff coluna-a-coluna a cada mudança — captura EXATAMENTE o que o DownloadXML
// grava no INSERT e o que o AthenasHorse muda depois (IMPORTADO 0→1, etc.).
func runWatchChave(ctx context.Context, rd *firebird.Reader, chave string, intervalSec int) {
	chave = strings.TrimSpace(chave)
	if len(chave) != 44 || digitsOnly(chave) != chave {
		log.Fatalf("watch-chave: chave precisa ter 44 dígitos: %q", chave)
	}
	if intervalSec < 5 {
		intervalSec = 5
	}
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	cols, err := rd.TableColumns(ctx, investTable)
	if err != nil {
		log.Fatalf("watch-chave: DDL: %v", err)
	}
	colNames := make([]string, len(cols))
	for i, c := range cols {
		colNames[i] = c.Name
	}
	log.Printf("=== watch-chave %s — polling a cada %ds (Ctrl-C p/ sair) ===", chave, intervalSec)

	prev := map[string]map[string]string{} // rowKey -> col -> value
	tick := time.NewTicker(time.Duration(intervalSec) * time.Second)
	defer tick.Stop()
	poll := func() {
		rows, err := rd.RawRowsByChave(ctx, investTable, colNames, chave)
		if err != nil {
			log.Printf("  poll: ERRO %v", err)
			return
		}
		if len(rows) == 0 {
			return
		}
		for _, row := range rows {
			cur := map[string]string{}
			for i, c := range row.Columns {
				if row.Values[i] != nil {
					cur[c] = *row.Values[i]
				}
			}
			key := cur["CODIGOEMPRESA"] + "/" + cur["CODIGOFILIAL"]
			old, seen := prev[key]
			if !seen {
				log.Printf("  [empresa %s] LINHA NOVA:", key)
				keys := make([]string, 0, len(cur))
				for k := range cur {
					keys = append(keys, k)
				}
				sort.Strings(keys)
				for _, k := range keys {
					log.Printf("      %-28s = %s", k, cur[k])
				}
				prev[key] = cur
				continue
			}
			changed := diffMaps(old, cur)
			if len(changed) > 0 {
				log.Printf("  [empresa %s] MUDOU:", key)
				for _, ch := range changed {
					log.Printf("      %s", ch)
				}
				prev[key] = cur
			}
		}
	}

	poll()
	for {
		select {
		case <-ctx.Done():
			log.Printf("=== watch-chave encerrado ===")
			return
		case <-tick.C:
			poll()
		}
	}
}

// diffMaps retorna as mudanças (col: antes -> depois) entre dois snapshots.
func diffMaps(old, cur map[string]string) []string {
	seen := map[string]bool{}
	var out []string
	for k, v := range cur {
		seen[k] = true
		if ov, ok := old[k]; !ok {
			out = append(out, k+": (NULL) -> "+v)
		} else if ov != v {
			out = append(out, k+": "+ov+" -> "+v)
		}
	}
	for k, ov := range old {
		if !seen[k] {
			out = append(out, k+": "+ov+" -> (NULL)")
		}
	}
	sort.Strings(out)
	return out
}

// segStat acumula a validação de um segmento da URL.
type segStat struct {
	exact, ci, total int
	examples         []string
}

func (s *segStat) add(derived, real string) {
	s.total++
	switch {
	case derived == real:
		s.exact++
		s.ci++
	case strings.EqualFold(derived, real):
		s.ci++
		s.addEx(derived, real)
	default:
		s.addEx(derived, real)
	}
}

func (s *segStat) addEx(derived, real string) {
	if len(s.examples) < 8 {
		s.examples = append(s.examples, "derivado="+derived+" | real="+real)
	}
}

// runCheckPath valida internal/syncpath contra URLs reais, segmento a segmento
// (§7 do design). Responde empiricamente as perguntas em aberto: qual campo é o
// 1º segmento, se o CNPJ é da filial (não do emitente), se a competência é da
// emissão ou da inclusão, e o mapa TIPODOCUMENTO→segmento de tipo.
func runCheckPath(ctx context.Context, rd *firebird.Reader, since string, sample int) {
	from := investSince(since, 30)
	log.Printf("=== check-path (READ-ONLY) — URLs com DATAINCLUSAO >= %s, amostra=%d ===", from.Format("2006-01-02"), sample)

	cols, err := rd.TableColumns(ctx, investTable)
	if err != nil {
		log.Fatalf("check-path: DDL: %v", err)
	}
	present := map[string]bool{}
	for _, c := range cols {
		present[c.Name] = true
	}
	rows, err := rd.RecentURLRows(ctx, from, sample, present["TPEVENTO"], present["CHAVEACESSOSUBS"])
	if err != nil {
		log.Fatalf("check-path: %v", err)
	}
	log.Printf("  %d linhas com URL preenchida", len(rows))

	stats := map[string]*segStat{}
	for _, n := range syncpath.SegmentNames {
		stats[n] = &segStat{}
	}
	compInclusao := &segStat{}       // competência derivada da DATAINCLUSAO (alternativa)
	cnpjEmitente := &segStat{}       // 2º segmento seria o CNPJ do EMITENTE? (deve falhar em entradas)
	docByFb := map[string][]string{} // TIPODOCUMENTO -> segmentos reais de tipo (aprende o mapa)
	var eventos, substitutas, formatoInesperado, semSegmentos int

	for _, r := range rows {
		if r.TpEvento != "" {
			eventos++
			continue
		}
		if r.ChaveSubs != "" {
			substitutas++
			continue
		}
		segs := syncpath.Segments(r.URL)
		if len(segs) == 0 {
			semSegmentos++
			continue
		}
		if len(segs) != len(syncpath.SegmentNames) {
			formatoInesperado++
			if formatoInesperado <= 8 {
				log.Printf("    formato inesperado (%d segmentos): %s", len(segs), r.URL)
			}
			continue
		}
		// 0: empresa
		stats["empresa"].add(syncpath.SanitizeSegment(r.NomeEmpresa), segs[0])
		// 1: cnpj da filial (tese) vs cnpj do emitente (contraprova)
		stats["cnpj_filial"].add(digitsOnly(r.CnpjFilial), segs[1])
		cnpjEmitente.add(digitsOnly(r.CnpjEmitente), segs[1])
		// 2: tipo de documento — aprende o mapa em vez de chutar
		docByFb[r.TipoDocumento] = appendDistinct(docByFb[r.TipoDocumento], segs[2])
		// 3: direção derivada dos CNPJs
		if dir := model.DirectionFromCNPJs(r.CnpjFilial, r.CnpjEmitente, r.CnpjDestinatario); dir != "" {
			if seg, err := syncpath.DirSegment(dir); err == nil {
				stats["direcao"].add(seg, segs[3])
			}
		}
		// 4: competência — emissão (tese) e inclusão (alternativa)
		if c, err := syncpath.Competencia(r.DataEmissao); err == nil {
			stats["competencia"].add(c, segs[4])
		}
		if r.DataInclusao != nil {
			compInclusao.add(r.DataInclusao.Format("200601"), segs[4])
		}
		// 5: arquivo
		stats["arquivo"].add(r.Chave+".xml", segs[5])
	}

	log.Printf("--- acerto por segmento (exato / case-insensitive / total) ---")
	for _, n := range syncpath.SegmentNames {
		s := stats[n]
		log.Printf("  %-12s %s exato, %s ci  (%d amostras)", n, ratio(s.exact, s.total), ratio(s.ci, s.total), s.total)
		for _, ex := range s.examples {
			log.Printf("      ✗ %s", ex)
		}
	}
	log.Printf("  [contraprova] cnpj_filial usando CNPJ do EMITENTE: %s exato (%d) — deve cair nas ENTRADAS", ratio(cnpjEmitente.exact, cnpjEmitente.total), cnpjEmitente.total)
	log.Printf("  [alternativa] competência da DATAINCLUSAO: %s exato (%d) vs emissão %s", ratio(compInclusao.exact, compInclusao.total), compInclusao.total, ratio(stats["competencia"].exact, stats["competencia"].total))

	log.Printf("--- mapa TIPODOCUMENTO (Firebird) -> segmento de tipo real na URL ---")
	fbKeys := make([]string, 0, len(docByFb))
	for k := range docByFb {
		fbKeys = append(fbKeys, k)
	}
	sort.Strings(fbKeys)
	for _, k := range fbKeys {
		log.Printf("    %-16s -> %s", derefStrRaw(k), strings.Join(docByFb[k], ", "))
	}

	log.Printf("--- fora do escopo do piloto ---")
	log.Printf("  eventos (TPEVENTO): %d | substitutas (CHAVEACESSOSUBS): %d | URL vazia após split: %d | formato inesperado: %d",
		eventos, substitutas, semSegmentos, formatoInesperado)
	log.Printf("=== fim do check-path ===")
}

func ratio(n, d int) string {
	if d == 0 {
		return "n/a"
	}
	return strconv.Itoa(n*100/d) + "%"
}

func digitsOnly(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func appendDistinct(xs []string, v string) []string {
	for _, x := range xs {
		if x == v {
			return xs
		}
	}
	return append(xs, v)
}

func derefStr(s *string) string {
	if s == nil {
		return "(NULL)"
	}
	return *s
}

func derefStrRaw(s string) string {
	if s == "" {
		return "(vazio)"
	}
	return s
}

func derefIntStr(i *int) string {
	if i == nil {
		return "?"
	}
	return strconv.Itoa(*i)
}
