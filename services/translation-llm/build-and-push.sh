#!/usr/bin/env bash
# Usage: REGISTRY=registry.<zone> ./build-and-push.sh
set -euo pipefail

REGISTRY="${REGISTRY:?set REGISTRY, e.g. registry.s241646-at-student-dhbw-mannheim-de.users.dhbw.site}"
IMAGE="${REGISTRY}/translation-llm:latest"

podman build --platform linux/amd64 -t "${IMAGE}" .
#podman push "${IMAGE}"
# TODO: drop --tls-verify=false once the cluster wildcard-tls cert issues correctly
podman push --tls-verify=false "${IMAGE}"
echo "pushed ${IMAGE}"
