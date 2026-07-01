#!/usr/bin/env bash
# Usage: REGISTRY=registry.<zone> ./build-and-push.sh
set -euo pipefail

REGISTRY="${REGISTRY:?set REGISTRY, e.g. registry.sXXXXXX-at-student-dhbw-mannheim-de.users.dhbw.site}"
IMAGE="${REGISTRY}/translation-api:latest"

podman build --platform linux/amd64 -t "${IMAGE}" .
podman push "${IMAGE}"
echo "pushed ${IMAGE}"
