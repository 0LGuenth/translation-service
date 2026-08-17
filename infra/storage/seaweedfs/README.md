# SeaweedFS Storage

Dieser Ordner beschreibt die Storage-Schicht fuer Delta Lake:

- `values.yaml`: Helm-Values fuer SeaweedFS, S3-Credentials, Buckets und Spark-S3A-Konfiguration.

SeaweedFS wird weiterhin vom Umbrella-Chart `charts/translate-platform` gerendert. Die Storage-Values liegen hier, damit die Infrastruktur sichtbar von eigenem Anwendungscode unter `services/` getrennt bleibt. Ausfuehrbare Projekt-Helfer liegen gesammelt unter `scripts/`.

## Endpoint

Spark verwendet den internen S3-kompatiblen Service:

```text
http://seaweedfs-filer.seaweedfs.svc.cluster.local:8333
```

Die Delta-Pfade sind:

```text
s3a://translation-bronze/
s3a://translation-silver/
s3a://translation-gold/
s3a://translation-checkpoints/
```

## Deployment

Das Ansible-Plattform-Playbook laedt dieses Values-File automatisch, wenn es existiert:

```bash
cd /data/dhbw-vpn/work/cloud-and-big-data/infra/ansible
ansible-playbook playbooks/04-deploy-platform.yaml
```

Zum manuellen Rendern:

```bash
helm template translate-platform ./charts/translate-platform \
  -f infra/storage/seaweedfs/values.yaml
```

## Smoke-Test

Nach dem Deployment prueft der Smoke-Test Bucket-Listing, Bucket-Existenz sowie Schreib- und Lesezugriff:

```bash
./scripts/seaweedfs-smoke-test.sh
```

Optionale Overrides:

```bash
SEAWEEDFS_NAMESPACE=seaweedfs \
SEAWEEDFS_SMOKE_BUCKET=translation-bronze \
./scripts/seaweedfs-smoke-test.sh
```
