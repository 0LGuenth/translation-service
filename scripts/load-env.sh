#!/usr/bin/env bash
# Shared local configuration for build, deploy and kubectl helper scripts.

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${TRANSLATE_ENV_FILE:-${PROJECT_ROOT}/.env}"

if [[ -f "${ENV_FILE}" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "${ENV_FILE}"
  set +a
fi

: "${STUDENT_DOMAIN:=student-dhbw-mannheim-de.users.dhbw.site}"

if [[ -n "${STUDENT_ID:-}" ]]; then
  : "${ZONE:=${STUDENT_ID}-at-${STUDENT_DOMAIN}}"
fi

if [[ -n "${ZONE:-}" ]]; then
  : "${REGISTRY:=registry.${ZONE}}"
fi

: "${KUBECONFIG:=${PROJECT_ROOT}/infra/ansible/kubeconfig-generated.yaml}"

export PROJECT_ROOT STUDENT_DOMAIN KUBECONFIG
[[ -n "${STUDENT_ID:-}" ]] && export STUDENT_ID
[[ -n "${ZONE:-}" ]] && export ZONE
[[ -n "${REGISTRY:-}" ]] && export REGISTRY
