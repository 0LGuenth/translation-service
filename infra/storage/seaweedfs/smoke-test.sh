#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../../../scripts/load-env.sh
source "${SCRIPT_DIR}/../../../scripts/load-env.sh"

namespace="${SEAWEEDFS_NAMESPACE:-seaweedfs}"
secret_name="${SEAWEEDFS_S3_SECRET:-seaweedfs-s3-credentials}"
endpoint="${SEAWEEDFS_S3_ENDPOINT:-http://seaweedfs-filer.${namespace}.svc.cluster.local:8333}"
image="${AWS_CLI_IMAGE:-amazon/aws-cli:2.15.57}"
job_name="${SEAWEEDFS_SMOKE_JOB:-seaweedfs-s3-smoke}"
if [[ -n "${SEAWEEDFS_SMOKE_BUCKET:-}" ]]; then
  buckets="${SEAWEEDFS_SMOKE_BUCKET}"
else
  buckets="${SEAWEEDFS_SMOKE_BUCKETS:-translation-bronze translation-silver translation-gold translation-checkpoints}"
fi
object_prefix="smoke-test/$(date -u +%Y%m%dT%H%M%SZ)"

if ! kubectl get namespace "${namespace}" >/dev/null 2>&1; then
  echo "Namespace ${namespace} not found. Deploy SeaweedFS first:" >&2
  echo "  cd infra/ansible && ansible-playbook playbooks/04-deploy-platform.yaml" >&2
  echo "or:" >&2
  echo "  helm upgrade --install translate-platform ./charts/translate-platform -f infra/storage/seaweedfs/values.yaml" >&2
  exit 1
fi

if ! kubectl -n "${namespace}" get secret "${secret_name}" >/dev/null 2>&1; then
  echo "Secret ${secret_name} not found in namespace ${namespace}. SeaweedFS is not ready for the smoke test yet." >&2
  echo "Run cd infra/ansible && ansible-playbook playbooks/04-deploy-platform.yaml, then wait for seaweedfs-master, seaweedfs-volume and seaweedfs-filer to roll out." >&2
  exit 1
fi

kubectl -n "${namespace}" delete job "${job_name}" --ignore-not-found

kubectl -n "${namespace}" apply -f - <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: ${job_name}
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: aws-cli
          image: ${image}
          command:
            - /bin/sh
            - -ec
            - |
              aws --endpoint-url "\${S3_ENDPOINT}" s3 ls
              for bucket in ${buckets}; do
                object="${object_prefix}/\${bucket}.txt"
                aws --endpoint-url "\${S3_ENDPOINT}" s3api head-bucket --bucket "\${bucket}"
                printf 'seaweedfs smoke ok: %s\n' "\${bucket}" | aws --endpoint-url "\${S3_ENDPOINT}" s3 cp - "s3://\${bucket}/\${object}"
                aws --endpoint-url "\${S3_ENDPOINT}" s3 cp "s3://\${bucket}/\${object}" -
              done
          env:
            - name: S3_ENDPOINT
              value: "${endpoint}"
            - name: AWS_EC2_METADATA_DISABLED
              value: "true"
            - name: AWS_ACCESS_KEY_ID
              valueFrom:
                secretKeyRef:
                  name: ${secret_name}
                  key: AWS_ACCESS_KEY_ID
            - name: AWS_SECRET_ACCESS_KEY
              valueFrom:
                secretKeyRef:
                  name: ${secret_name}
                  key: AWS_SECRET_ACCESS_KEY
            - name: AWS_DEFAULT_REGION
              valueFrom:
                secretKeyRef:
                  name: ${secret_name}
                  key: AWS_DEFAULT_REGION
EOF

kubectl -n "${namespace}" wait --for=condition=complete "job/${job_name}" --timeout=120s || {
  kubectl -n "${namespace}" logs "job/${job_name}" >&2 || true
  exit 1
}

kubectl -n "${namespace}" logs "job/${job_name}"
kubectl -n "${namespace}" delete job "${job_name}" --ignore-not-found
