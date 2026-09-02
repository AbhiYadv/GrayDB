#!/usr/bin/env bash
set -euo pipefail

readonly R1_APPROVED_PREFIX="/Volumes/Crucial X9/GrayDB/.r1"
readonly R1_REPOSITORY_ROOT="/Volumes/Crucial X9/GrayDB"
R1_DATA_ROOT="${R1_DATA_ROOT:-$R1_APPROVED_PREFIX}"

fail() {
  printf 'r1-colima: %s\n' "$*" >&2
  exit 1
}

canonicalize_data_root() {
  case "$R1_DATA_ROOT" in
    "$R1_APPROVED_PREFIX"|"$R1_APPROVED_PREFIX"/*) ;;
    *) fail "R1_DATA_ROOT must stay below $R1_APPROVED_PREFIX" ;;
  esac

  local parent base canonical_parent
  parent="$(dirname "$R1_DATA_ROOT")"
  base="$(basename "$R1_DATA_ROOT")"
  [[ -d "$parent" ]] || fail "R1_DATA_ROOT parent does not exist: $parent"
  canonical_parent="$(cd "$parent" && pwd -P)"
  R1_DATA_ROOT="$canonical_parent/$base"

  case "$R1_DATA_ROOT" in
    "$R1_APPROVED_PREFIX"|"$R1_APPROVED_PREFIX"/*) ;;
    *) fail "canonical R1_DATA_ROOT escaped $R1_APPROVED_PREFIX" ;;
  esac

  [[ -d "$R1_REPOSITORY_ROOT" ]] || fail "external repository root is unavailable"
  if [[ ! -e "$R1_DATA_ROOT" ]]; then
    mkdir "$R1_DATA_ROOT"
  fi
  [[ -d "$R1_DATA_ROOT" ]] || fail "R1_DATA_ROOT is not a directory: $R1_DATA_ROOT"
  R1_DATA_ROOT="$(cd "$R1_DATA_ROOT" && pwd -P)"
}

create_named_children() {
  mkdir -p \
    "$R1_DATA_ROOT/colima" \
    "$R1_DATA_ROOT/postgres" \
    "$R1_DATA_ROOT/graydb" \
    "$R1_DATA_ROOT/clickhouse" \
    "$R1_DATA_ROOT/clickhouse-logs" \
    "$R1_DATA_ROOT/metadata"
}

record_repository_digest() {
  local image="$1" record digest existing
  record="$R1_DATA_ROOT/metadata/${image//\//_}.repository-digest"
  record="${record//:/_}"

  docker pull "$image" >/dev/null
  digest="$(docker image inspect --format '{{range .RepoDigests}}{{println .}}{{end}}' "$image" \
    | awk -v image="$image" '$0 ~ "^" image "@" { print; exit }')"
  [[ -n "$digest" ]] || fail "no repository digest found for $image"

  if [[ -f "$record" ]]; then
    existing="$(<"$record")"
    [[ "$existing" == "$digest" ]] || fail "recorded digest differs for $image; use a new R1 data root"
    printf 'verified recorded digest: %s\n' "$digest"
    return
  fi

  printf '%s\n' "$digest" > "$record"
  printf 'recorded dataset image digest: %s\n' "$digest"
}

canonicalize_data_root
create_named_children

if colima status --profile r1 2>/dev/null | grep -qi 'running'; then
  printf 'Colima profile r1 is already running; inspecting without recreation.\n'
else
  colima start --profile r1 \
    --cpu 8 \
    --memory 12 \
    --disk 600 \
    --disk-image "$R1_DATA_ROOT/colima/disk.img" \
    --mount /Volumes/Crucial\ X9/GrayDB:w
fi

record_repository_digest postgres:17
record_repository_digest clickhouse/clickhouse-server:25.8

colima status --profile r1
docker context show
