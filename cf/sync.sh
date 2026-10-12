#!/bin/sh
# OneDrive 의 소스를 Cloudflare 배포 폴더로 복사 (wrangler 가 만드는 .wrangler 임시 파일이 OneDrive 에 생기지 않게)
SRC="$(cd "$(dirname "$0")/.." && pwd)"
APP="${CF_APP:-/c/Users/LSH/draw-cf/app}"
mkdir -p "$APP/cf" "$APP/public"
cp "$SRC/core.js" "$APP/core.js"
cp "$SRC/cf/worker.js" "$APP/cf/worker.js"
cp "$SRC/cf/wrangler.toml" "$APP/wrangler.toml"
cp "$SRC/public/index.html" "$SRC/public/og.png" "$APP/public/"
echo "synced to $APP"
