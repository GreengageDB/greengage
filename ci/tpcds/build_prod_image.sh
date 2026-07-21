#!/usr/bin/env bash
# Build ONE production (no-cassert) Greengage base image for the TPC-DS
# comparison, then verify the compiled binary is genuinely a production build.
#
#   ./build_prod_image.sh gg7   # GreengageDB 7.4.1  (rebuilt from published image)
#   ./build_prod_image.sh gg8   # origin/adb-8.x     (PG14)
#   ./build_prod_image.sh gg9   # claude-merge-7 HEAD (PG18.4)
#
# Produces image  greengage-prod:<key>  and logs to logs/build_<key>.log.
set -euo pipefail
cd "$(dirname "$0")"
HERE="$(pwd)"
REPO_ROOT="$(cd ../.. && pwd)"
KEY="${1:?usage: build_prod_image.sh gg7|gg8|gg9}"
DOCKER="sudo docker"
mkdir -p logs

# Shared production toggles for the ci/Dockerfile.ubuntu (GG8/GG9) builds:
# the CI cassert set minus --enable-cassert/--enable-debug/--enable-debug-extensions.
PROD_FLAGS_89="--enable-orca --with-gssapi --with-llvm --with-openssl --enable-depend --with-libxml --with-uuid=ossp --with-perl --enable-mapreduce --enable-orafce"

build_from_worktree() {   # $1=git-ref  $2=image-tag
  local ref="$1" tag="$2" wt="/home/dvoronkov/ws/tpcds-build/${KEY}"
  echo ">>> preparing clean worktree for $ref at $wt"
  git -C "$REPO_ROOT" worktree remove --force "$wt" 2>/dev/null || true
  rm -rf "$wt"
  git -C "$REPO_ROOT" worktree add --detach "$wt" "$ref"
  echo ">>> docker build $tag  (context=$wt, recipe=$HERE/../Dockerfile.ubuntu, PROD)"
  $DOCKER build \
    -f "$HERE/../Dockerfile.ubuntu" \
    --target test \
    --build-arg CONFIGURE_FLAGS="$PROD_FLAGS_89" \
    --build-arg SKIP_UNITTESTS=1 \
    -t "$tag" "$wt"
  git -C "$REPO_ROOT" worktree remove --force "$wt" 2>/dev/null || true
}

case "$KEY" in
  gg7)
    echo ">>> GG7: rebuild 7.4.1 production from published image"
    $DOCKER build -f "$HERE/Dockerfile.gg7-prod" -t greengage-prod:gg7 "$HERE"
    ;;
  gg8)
    build_from_worktree "gg/next" "greengage-prod:gg8"
    ;;
  gg9)
    build_from_worktree "HEAD" "greengage-prod:gg9"
    ;;
  *) echo "unknown key $KEY"; exit 1;;
esac

echo ">>> VERIFY greengage-prod:${KEY} is a production (no-cassert) build"
$DOCKER run --rm --entrypoint bash "greengage-prod:${KEY}" -lc '
  set -e
  mkdir -p /tmp/v && tar xzf /home/gpadmin/bin_gpdb/bin_gpdb.tar.gz -C /tmp/v
  PGC=$(find /tmp/v -name pg_config -type f | head -1)
  echo "version: $($PGC --version)"
  CFG=$($PGC --configure)
  echo "configure: $CFG"
  if echo "$CFG" | grep -q -- "--enable-cassert"; then
     echo "FAIL: --enable-cassert present — NOT a production build"; exit 1
  fi
  echo "OK: no --enable-cassert (production build confirmed)"
'
echo ">>> DONE greengage-prod:${KEY}"
