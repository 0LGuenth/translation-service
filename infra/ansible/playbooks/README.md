# Deploy playbooks (raw manifests + podman + in-cluster registry)

These run on **localhost** (`connection: local`): images build with `podman` on
the dev machine and `kubectl` / `kubernetes.core.k8s` talk to the cluster
locally. The cluster itself is bootstrapped separately by `../deploy.yaml`.

## Required / common vars

| var | how to set | default |
|-----|-----------|---------|
| `zone` | `-e zone=...` or `ZONE` env | *(required)* e.g. `s241646-at-student-dhbw-mannheim-de.users.dhbw.site` |
| `registry_host` | `-e registry_host=...` or `REGISTRY_HOST` | `registry.{{ zone }}` |
| `kubeconfig_path` | `-e kubeconfig_path=...` or `KUBECONFIG` | `../kubeconfig-generated.yaml` |

`zone` is substituted into the manifests' placeholder host
(`<id>-at-student-dhbw-mannheim-de.users.dhbw.site` and `*.example.invalid`),
covering both image registry refs and ingress hosts.

Optional: `ROLLOUT_TIMEOUT` (default `300s`), `STRIMZI_TIMEOUT` (default `300s`),
`PODMAN_PUSH_ARGS` (default `--tls-verify=false`; set to empty once the registry
cert validates).

Run from `infra/ansible/`.

## Playbooks

| file | does |
|------|------|
| `02-build-images.yaml` | build + push all 5 service images to `registry.{{ zone }}` |
| `03-install-strimzi.yaml` | install/ensure Strimzi operator (watches `default`), wait for the Kafka CRD |
| `04-deploy-platform.yaml` | apply raw manifests in order: seaweedfs -> kafka -> postgres -> services (llm, api, web-ui, analytics-api, spark) -> observability (ns `translate-platform`); wait for key rollouts |
| `05-deploy-full.yaml` | imports 02 -> 03 -> 04 (cluster bootstrap import is commented in) |
| `10..15-build-and-deploy-<svc>.yaml` | build+push one image, apply its manifests, restart + wait for its rollout |

## Examples

```bash
export ZONE=s241646-at-student-dhbw-mannheim-de.users.dhbw.site
export KUBECONFIG=$PWD/kubeconfig-generated.yaml

# full platform (already-bootstrapped cluster)
ansible-playbook playbooks/05-deploy-full.yaml

# just rebuild + roll one service
ansible-playbook playbooks/10-build-and-deploy-translation-api.yaml

# or pass vars explicitly instead of env
ansible-playbook playbooks/04-deploy-platform.yaml \
  -e zone=s241646-at-student-dhbw-mannheim-de.users.dhbw.site \
  -e kubeconfig_path=$PWD/kubeconfig-generated.yaml
```

Requires the `kubernetes.core` collection, `kubectl`, and `podman` on the
control host.
