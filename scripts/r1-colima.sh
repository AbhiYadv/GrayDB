#!/usr/bin/env bash
set -euo pipefail

readonly R1_APPROVED_PREFIX="/Volumes/Crucial X9/GrayDB/.r1"
readonly R1_REPOSITORY_ROOT="/Volumes/Crucial X9/GrayDB"
readonly R1_DOCKER_CONTEXT="colima-r1"
readonly R1_COLIMA_CONFIG="${COLIMA_HOME:-$HOME/.colima}/r1/colima.yaml"
readonly R1_SOURCE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
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

config_scalar() {
  local key="$1"
  # Print everything after "key:" — values may contain spaces (for example
  # the disk image path "/Volumes/Crucial X9/..."), so $2 alone is wrong.
  awk -v key="$key:" '$1 == key { sub(/^[^:]*:[[:space:]]*/, ""); gsub(/,$/, ""); gsub(/^"|"$/, ""); print; exit }' "$R1_COLIMA_CONFIG"
}

validate_r1_profile() {
  local cpus memory disk disk_image expected_disk_image
  [[ -r "$R1_COLIMA_CONFIG" ]] || fail "r1 profile config is unreadable: $R1_COLIMA_CONFIG"

  cpus="$(config_scalar cpu)"
  memory="$(config_scalar memory)"
  disk="$(config_scalar disk)"
  disk_image="$(config_scalar diskImage)"
  expected_disk_image="$R1_DATA_ROOT/colima/disk.img"

  [[ "$cpus" == "8" ]] || fail "r1 profile CPU must be 8, found ${cpus:-unset}"
  [[ "$memory" == "12" || "$memory" == "12.0" ]] || fail "r1 profile memory must be 12 GiB, found ${memory:-unset}"
  [[ "$disk" == "600" ]] || fail "r1 profile disk must be 600 GiB, found ${disk:-unset}"
  [[ "$disk_image" == "$expected_disk_image" ]] || fail "r1 disk image must be $expected_disk_image, found ${disk_image:-unset}"

  awk -v expected="$R1_REPOSITORY_ROOT" '
    function unquote(value) { gsub(/^"|"$/, "", value); return value }
    $1 == "-" && $2 == "location:" {
      value = $0
      sub(/^.*location:[[:space:]]*/, "", value)
      location = unquote(value); writable = ""; next
    }
    $1 == "location:" {
      value = $0
      sub(/^[^:]*:[[:space:]]*/, "", value)
      location = unquote(value); writable = ""; next
    }
    $1 == "writable:" && $2 == "true" && location == expected { found = 1 }
    END { exit(found ? 0 : 1) }
  ' "$R1_COLIMA_CONFIG" || fail "r1 profile must have writable mount $R1_REPOSITORY_ROOT"
}

validate_running_resources() {
  local status_json="$1"
  command -v jq >/dev/null || fail "jq is required to validate the running r1 profile"
  printf '%s' "$status_json" | jq -e \
    --argjson memory "$((12 << 30))" \
    --argjson disk "$((600 << 30))" \
    '.cpu == 8 and .memory == $memory and .disk == $disk' >/dev/null \
    || fail "running r1 profile must report exactly 8 CPU, 12 GiB memory, and 600 GiB disk"
}

ensure_r1_docker_context() {
  docker context inspect "$R1_DOCKER_CONTEXT" >/dev/null \
    || fail "Docker context $R1_DOCKER_CONTEXT is unavailable"
  [[ "$(docker context show)" == "$R1_DOCKER_CONTEXT" ]] \
    || fail "Docker context must be $R1_DOCKER_CONTEXT before R1 image work; refusing to switch another context"
}

build_r1ctl_when_available() {
  local r1ctl_source="$R1_SOURCE_ROOT/crates/graydb-r1/src/bin/r1ctl.rs"
  if [[ ! -f "$r1ctl_source" ]]; then
    printf 'r1ctl build deferred: Task 12 owns %s; then run: cargo build --release -p graydb-r1 --bin r1ctl\n' "$r1ctl_source"
    return
  fi
  (
    cd "$R1_SOURCE_ROOT"
    cargo build --release -p graydb-r1 --bin r1ctl
  )
}

record_repository_digest() {
  local image="$1" record digest existing
  record="$R1_DATA_ROOT/metadata/${image//\//_}.repository-digest"
  record="${record//:/_}"

  docker --context "$R1_DOCKER_CONTEXT" pull "$image" >/dev/null
  # RepoDigests lists "postgres@sha256:..." — the tag is not part of the
  # digest reference, so match on the repository alone.
  local repository="${image%%:*}"
  digest="$(docker --context "$R1_DOCKER_CONTEXT" image inspect --format '{{range .RepoDigests}}{{println .}}{{end}}' "$image" \
    | awk -v image="$repository" '$0 ~ "^" image "@" { print; exit }')"
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
build_r1ctl_when_available

if colima status --profile r1 --json >/dev/null 2>&1; then
  printf 'Colima profile r1 is already running; validating without recreation.\n'
else
  colima start --profile r1 \
    --cpu 8 \
    --memory 12 \
    --disk 600 \
    --disk-image "$R1_DATA_ROOT/colima/disk.img" \
    --mount /Volumes/Crucial\ X9/GrayDB:w \
    --mount-type 9p
fi

R1_STATUS_JSON="$(colima status --profile r1 --json)"
validate_running_resources "$R1_STATUS_JSON"
validate_r1_profile
ensure_r1_docker_context
record_repository_digest postgres:17
record_repository_digest clickhouse/clickhouse-server:25.8

colima status --profile r1
docker context show
