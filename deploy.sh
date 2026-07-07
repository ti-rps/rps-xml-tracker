#!/usr/bin/env bash
# deploy.sh — caminho pavimentado de deploy do tracker no SRVRPS03.
#
#   ./deploy.sh                    # deploya tracker-api + tracker-poller + tracker-migrate
#   ./deploy.sh tracker-poller     # só o poller (tracker-migrate entra junto sempre)
#
# Codifica as lições dos deploys manuais:
#   - sempre `up -d` após o build (`compose run` NÃO troca a imagem do serviço em
#     execução — foi assim que o poller rodou semanas com imagem defasada);
#   - tracker-migrate é rebuildado SEMPRE (ele roda no boot da API; com imagem
#     antiga, aplica migrações defasadas);
#   - injeta a versão (commit+data) via --build-arg -> aparece no /health, nos
#     logs de partida e nos heartbeats do GET /status;
#   - se o buildx estiver quebrado ("docker endpoint for default not found",
#     recorrente neste host), tenta de novo com o builder legacy.
set -euo pipefail
cd "$(dirname "$0")"

if [ $# -gt 0 ]; then
  SERVICES=("$@")
else
  SERVICES=(tracker-api tracker-poller)
fi
# migrate roda no boot da API a partir da MESMA imagem — rebuild sempre, é barato (cache).
case " ${SERVICES[*]} " in
  *" tracker-migrate "*) ;;
  *) SERVICES+=(tracker-migrate) ;;
esac

echo "==> git pull --ff-only"
git pull --ff-only

COMMIT=$(git rev-parse --short HEAD)
BUILT_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)

echo "==> build ${SERVICES[*]} (versão ${COMMIT}, ${BUILT_AT})"
build() {
  docker compose build \
    --build-arg GIT_COMMIT="${COMMIT}" \
    --build-arg BUILT_AT="${BUILT_AT}" \
    "${SERVICES[@]}"
}
if ! build; then
  echo "==> build falhou (buildx quebrado?) — tentando builder legacy (DOCKER_BUILDKIT=0)"
  DOCKER_BUILDKIT=0 build
fi

echo "==> up -d ${SERVICES[*]}"
docker compose up -d "${SERVICES[@]}"

echo "==> containers"
docker compose ps

echo "==> health (a API pode levar minutos se houver migração pesada no boot)"
sleep 3
if curl -fsS --max-time 10 "http://localhost:8090/api/v1/health"; then
  echo
else
  echo "(health ainda não respondeu — se houve migração/deploy da API, aguarde e rode:"
  echo "  curl -s http://localhost:8090/api/v1/health)"
fi

echo "==> deploy ok: ${COMMIT}"
