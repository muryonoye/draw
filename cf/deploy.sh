#!/bin/sh
# Cloudflare 에 올리기: sh cf/deploy.sh   (먼저 C:\Users\LSH\draw-cf 에서 wrangler login 이 돼 있어야 한다)
# 1) 소스를 배포 폴더로 복사  2) wrangler deploy (질문을 기다리며 멈추지 않게 CI=1, 표준 입력 닫음)
# 올린 뒤: node tests/live_check.js https://<주소>  로 실제 사이트 점검
set -e
SRC="$(cd "$(dirname "$0")/.." && pwd)"
CF_DIR="${CF_DIR:-/c/Users/LSH/draw-cf}"
CF_APP="$CF_DIR/app" sh "$SRC/cf/sync.sh"
cd "$CF_DIR/app"
CI=1 WRANGLER_SEND_METRICS=false node ../node_modules/wrangler/bin/wrangler.js deploy < /dev/null
