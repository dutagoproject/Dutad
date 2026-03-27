#!/usr/bin/env bash
set -euo pipefail

VERSION="${1:-1.0.3}"
TARGET_TRIPLE="${2:-x86_64-unknown-linux-gnu}"
OUTPUT_ROOT="${3:-dist}"

WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$WORKSPACE_ROOT"

case "$TARGET_TRIPLE" in
  x86_64-unknown-linux-gnu)
    ARCH_LABEL="linux-x86_64"
    EXE_SUFFIX=""
    ;;
  x86_64-unknown-linux-musl)
    ARCH_LABEL="linux-x86_64-musl"
    EXE_SUFFIX=""
    ;;
  *)
    ARCH_LABEL="$TARGET_TRIPLE"
    EXE_SUFFIX=""
    ;;
esac

BUNDLE_NAME="duta-release-${VERSION}-${ARCH_LABEL}"
BUNDLE_DIR="${WORKSPACE_ROOT}/${OUTPUT_ROOT}/${BUNDLE_NAME}"
RELEASE_DIR="${WORKSPACE_ROOT}/target/${TARGET_TRIPLE}/release"

if [[ "$TARGET_TRIPLE" == "x86_64-unknown-linux-gnu" && -d "${WORKSPACE_ROOT}/target/release" ]]; then
  RELEASE_DIR="${WORKSPACE_ROOT}/target/release"
fi

PACKAGES=(
  dutad
)

ARTIFACTS=(
  "dutad:dutad${EXE_SUFFIX}:bin"
  "dutad:duta-cli${EXE_SUFFIX}:bin"
  "dutad:dutaminer${EXE_SUFFIX}:bin"
)

for package in "${PACKAGES[@]}"; do
  if [[ "$TARGET_TRIPLE" == "x86_64-unknown-linux-gnu" ]]; then
    cargo build --release -p "$package"
  else
    cargo build --release --target "$TARGET_TRIPLE" -p "$package"
  fi
done

mkdir -p "$BUNDLE_DIR"
manifest_artifacts=()
: > "${BUNDLE_DIR}/sha256sums.txt"

for artifact in "${ARTIFACTS[@]}"; do
  IFS=":" read -r package file kind <<< "$artifact"
  source_path="${RELEASE_DIR}/${file}"
  if [[ ! -f "$source_path" ]]; then
    echo "missing_release_artifact: ${file}" >&2
    exit 1
  fi

  dest_path="${BUNDLE_DIR}/${file}"
  cp "$source_path" "$dest_path"
  chmod +x "$dest_path" || true

  sha="$(sha256sum "$dest_path" | awk '{print $1}')"
  size="$(wc -c < "$dest_path" | tr -d ' ')"
  printf '%s  %s\n' "$sha" "$file" >> "${BUNDLE_DIR}/sha256sums.txt"
  manifest_artifacts+=("{\"package\":\"${package}\",\"file\":\"${file}\",\"kind\":\"${kind}\",\"sha256\":\"${sha}\",\"bytes\":${size}}")
done

generated_at="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
cargo_version="$(cargo --version)"
rustc_version="$(rustc --version)"
source_commit="$(git rev-parse HEAD)"

{
  echo "{"
  echo "  \"name\": \"${BUNDLE_NAME}\","
  echo "  \"version\": \"${VERSION}\","
  echo "  \"source_commit\": \"${source_commit}\","
  echo "  \"generated_at_utc\": \"${generated_at}\","
  echo "  \"workspace\": \"${WORKSPACE_ROOT}\","
  echo "  \"host_os\": \"linux\","
  echo "  \"target_triple\": \"${TARGET_TRIPLE}\","
  echo "  \"cargo_version\": \"${cargo_version}\","
  echo "  \"rustc_version\": \"${rustc_version}\","
  echo "  \"build_commands\": ["
  for i in "${!PACKAGES[@]}"; do
    pkg="${PACKAGES[$i]}"
    if [[ "$TARGET_TRIPLE" == "x86_64-unknown-linux-gnu" ]]; then
      cmd="cargo build --release -p ${pkg}"
    else
      cmd="cargo build --release --target ${TARGET_TRIPLE} -p ${pkg}"
    fi
    if [[ "$i" -lt $((${#PACKAGES[@]} - 1)) ]]; then
      echo "    \"${cmd}\","
    else
      echo "    \"${cmd}\""
    fi
  done
  echo "  ],"
  echo "  \"artifacts\": ["
  for i in "${!manifest_artifacts[@]}"; do
    if [[ "$i" -lt $((${#manifest_artifacts[@]} - 1)) ]]; then
      echo "    ${manifest_artifacts[$i]},"
    else
      echo "    ${manifest_artifacts[$i]}"
    fi
  done
  echo "  ]"
  echo "}"
} > "${BUNDLE_DIR}/manifest.json"

tar -C "${WORKSPACE_ROOT}/${OUTPUT_ROOT}" -czf "${WORKSPACE_ROOT}/${OUTPUT_ROOT}/${BUNDLE_NAME}.tar.gz" "${BUNDLE_NAME}"

echo "Release bundle ready: ${BUNDLE_DIR}"
