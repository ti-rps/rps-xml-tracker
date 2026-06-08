# Phase 0 — Investigação (read-only, descartável)

Ferramentas para **provar os sinais antes de construir** o rps-xml-tracker.
Tudo aqui é **somente-leitura**: nunca move/apaga/trava arquivo, nunca escreve no Firebird.
Módulo Go isolado (`phase0/`) — não faz parte do serviço final.

Plano completo: `~/.claude/plans/enumerated-moseying-goose.md`.

---

## `fsscan` — F0.2 (volume/watch) + F0.3 (taxonomia)

Varre uma pasta do fluxo de XML e reporta volume, profundidade, tipos de documento
(NFe / nfeProc / resNFe / eventos / NFCe / ...), quantos arquivos rendem uma chave de 44
dígitos e como, e o custo de parsear (p50/p95/max) — para dimensionar o agente.

```bash
# No srvdoc01 (Windows) — contra o disco LOCAL:
go run ./fsscan -root "C:\xml_asincronizar" -json asinc.json -show-unknown
go run ./fsscan -root "C:\xml_sincronizado"  -json sinc.json

# amostra rápida (parar após N arquivos):
go run ./fsscan -root "C:\xml_asincronizar" -sample 5000
```

O que olhar no resultado:
- **% identifiable** — qual fração dos XML rende chave (alvo: ~100% para NFe/NFCe).
- **Document types / Root elements** — confirma a taxonomia e o que fica fora do MVP (eventos, resumo).
- **Chave found via** — `infNFe@Id` vs `chNFe` (regras de extração a implementar no agente).
- **Parse cost p95** — viabilidade de parsear no ritmo de chegada.
- **Max depth / Total files** — decide watch recursivo e mono-processo vs containers.

## `fbinspect` — F0.1 (definir `imported_at`)

Conecta no Firebird do Athenas (read-only) e responde: dá para usar `DATADEENTRADA` como
`imported_at`, ou precisamos detectar a transição `IMPORTADO 0→1` por polling?

```bash
export FIREBIRD_DSN="USUARIO_RO:senha@HOST:3050//caminho/para/ATHENAS.FDB"
go run ./fbinspect
# ou por partes:
go run ./fbinspect -host 10.0.0.5 -user SYSDBA -pass '***' -db 'C:\Athenas\BASE.FDB'
```

**Critério de decisão** (impresso no fim): se `IMPORTADO=1 AND DATADEENTRADA IS NULL` ≈ 0 e o
range/recência fazem sentido → usar `DATADEENTRADA`. Caso contrário → polling da transição 0→1
(latência ≈ intervalo do poll).

### Fallback sem Go — SQL puro (rodar em isql / IBExpert, somente SELECT)

```sql
-- volume e flag de importado
SELECT COUNT(*) FROM TABLISTACHAVEACESSO;
SELECT IMPORTADO, COUNT(*) FROM TABLISTACHAVEACESSO GROUP BY IMPORTADO;

-- a pergunta-chave do imported_at:
SELECT COUNT(*) FROM TABLISTACHAVEACESSO WHERE IMPORTADO = 1 AND DATADEENTRADA IS NULL;
SELECT COUNT(*) FROM TABLISTACHAVEACESSO WHERE IMPORTADO = 1 AND DATADEENTRADA IS NOT NULL;
SELECT MIN(DATADEENTRADA), MAX(DATADEENTRADA) FROM TABLISTACHAVEACESSO;

-- recência (cadência de poll):
SELECT COUNT(*) FROM TABLISTACHAVEACESSO WHERE CAST(DATADEENTRADA AS DATE) = CURRENT_DATE;

-- estados terminais:
SELECT SITUACAO, COUNT(*) FROM TABLISTACHAVEACESSO GROUP BY SITUACAO;
SELECT IMPORTACAOIGNORADA, COUNT(*) FROM TABLISTACHAVEACESSO GROUP BY IMPORTACAOIGNORADA;

-- amostra recente para conferir DATADEENTRADA vs chave:
SELECT FIRST 15 CHAVEACESSO, IMPORTADO, DATADEENTRADA, SITUACAO, IMPORTACAOIGNORADA
  FROM TABLISTACHAVEACESSO WHERE DATADEENTRADA IS NOT NULL ORDER BY DATADEENTRADA DESC;
```

---

## Como usar os resultados
Cole as saídas (ou os `-json`) de volta no chat. Com F0.1 + F0.2 + F0.3 fechados, montamos a
baseline de SLA (F0.4) e confirmamos a decisão mono-processo vs containers antes de iniciar a Fase 1.
