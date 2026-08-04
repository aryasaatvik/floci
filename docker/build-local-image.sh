#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: docker/build-local-image.sh [--metadata-only] [IMAGE] [-- DOCKER_BUILD_ARGS...]

Builds the JVM Floci image with source-provenance labels. IMAGE defaults to
floci/floci:local. The dirty patch digest covers tracked changes and untracked
files, so one label identifies the exact cumulative local source tree.

Examples:
  docker/build-local-image.sh samva/floci:cumulative
  docker/build-local-image.sh samva/floci:cumulative -- --platform linux/arm64
  docker/build-local-image.sh --metadata-only
EOF
}

metadata_only=false
if [[ "${1:-}" == "--metadata-only" ]]; then
    metadata_only=true
    shift
elif [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
    usage
    exit 0
fi

image_ref="${1:-floci/floci:local}"
if [[ $# -gt 0 ]]; then
    shift
fi
if [[ "${1:-}" == "--" ]]; then
    shift
fi

repo_root="$(git -C "$(dirname "${BASH_SOURCE[0]}")/.." rev-parse --show-toplevel)"
source_revision="$(git -C "$repo_root" rev-parse HEAD)"
short_revision="$(git -C "$repo_root" rev-parse --short=12 HEAD)"
build_created="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"

provenance_dir="$(mktemp -d "${TMPDIR:-/tmp}/floci-provenance.XXXXXX")"
trap 'rm -rf "$provenance_dir"' EXIT
provenance_input="$provenance_dir/source-state"

git -C "$repo_root" diff --binary HEAD > "$provenance_input"
while IFS= read -r -d '' source_file; do
    if [[ -L "$repo_root/$source_file" ]]; then
        source_mode=120000
        source_object="$(readlink "$repo_root/$source_file" | git hash-object --stdin)"
    elif [[ -x "$repo_root/$source_file" ]]; then
        source_mode=100755
        source_object="$(git -C "$repo_root" hash-object --no-filters -- "$source_file")"
    else
        source_mode=100644
        source_object="$(git -C "$repo_root" hash-object --no-filters -- "$source_file")"
    fi
    printf '\0untracked\0%s\0%s\0%s\0' \
        "$source_mode" "$source_file" "$source_object" >> "$provenance_input"
done < <(git -C "$repo_root" ls-files --others --exclude-standard -z)

if [[ ! -s "$provenance_input" ]]; then
    source_dirty=false
    source_patch_sha256=clean
else
    source_dirty=true
    source_patch_sha256="sha256:$(shasum -a 256 "$provenance_input" | awk '{print $1}')"
fi

version="local-${short_revision}"

printf 'image=%s\n' "$image_ref"
printf 'version=%s\n' "$version"
printf 'source_revision=%s\n' "$source_revision"
printf 'source_patch_sha256=%s\n' "$source_patch_sha256"
printf 'source_dirty=%s\n' "$source_dirty"
printf 'build_created=%s\n' "$build_created"

if [[ "$metadata_only" == true ]]; then
    exit 0
fi

docker build \
    --file "$repo_root/docker/Dockerfile" \
    --tag "$image_ref" \
    --build-arg "VERSION=$version" \
    --build-arg "SOURCE_REVISION=$source_revision" \
    --build-arg "SOURCE_PATCH_SHA256=$source_patch_sha256" \
    --build-arg "SOURCE_DIRTY=$source_dirty" \
    --build-arg "BUILD_CREATED=$build_created" \
    "$@" \
    "$repo_root"
