#!/bin/bash
set -euo pipefail

if [[ -n "${S3_ACCESS_KEY:-}" && -n "${S3_SECRET_KEY:-}" ]]; then
  if [[ ! -f /usr/share/elasticsearch/config/elasticsearch.keystore ]]; then
    /usr/share/elasticsearch/bin/elasticsearch-keystore create
  fi

  printf "%s" "$S3_ACCESS_KEY" | /usr/share/elasticsearch/bin/elasticsearch-keystore add -x s3.client.default.access_key -f
  printf "%s" "$S3_SECRET_KEY" | /usr/share/elasticsearch/bin/elasticsearch-keystore add -x s3.client.default.secret_key -f
fi

exec /usr/local/bin/docker-entrypoint.sh "$@"
