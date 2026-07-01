# translation-api

Go HTTP gateway. Validates translation requests and forwards them to the `translation-llm` backend.

## Endpoints

- `POST /translate`: `{text, src_lang, tgt_lang}` -> `{translated, model, latency_ms_total, latency_ms_translate}`
- `GET /health`: liveness probe target
- `GET /ready`: readiness probe target

Language codes accept ISO 639-1, 639-3, and 639-5 (parsed via
`golang.org/x/text/language`). Codes are normalized before being forwarded
(e.g. `deu` -> `de`) so the LLM cache doesn't split across aliases. Text is
capped at `MAX_TEXT_LENGTH`.

## Config (env vars)

| Var                        | Default                              | Notes                                                                              |
|----------------------------|--------------------------------------|------------------------------------------------------------------------------------|
| `PORT`                     | `8000`                               |                                                                                    |
| `TRANSLATION_LLM_URL`      | *(required, service exits if empty)* | Backend base URL. In-cluster: `http://translation-llm:8000`.                       |
| `MAX_TEXT_LENGTH`          | `5000`                               | Max chars per request.                                                             |
| `LLM_TIMEOUT_SECONDS`      | `30`                                 | Per-request timeout to the backend.                                                |
| `SHUTDOWN_TIMEOUT_SECONDS` | `20`                                 | Drain window on SIGTERM. Keep < pod `terminationGracePeriodSeconds` (default 30s). |

## Run locally

Requires a reachable `translation-llm`: easiest is `kubectl port-forward
svc/translation-llm 8001:8000` in another shell.

```sh
TRANSLATION_LLM_URL=http://localhost:8001 go run .
curl -X POST localhost:8000/translate \
  -H content-type:application/json \
  -d '{"text":"hallo","src_lang":"de","tgt_lang":"en"}'
```

## Build & push

```sh
REGISTRY=registry.<your-zone> ./build-and-push.sh
```

## Deploy

Edit the host in `k8s/ingress.yaml` and the image in `k8s/deployment.yaml`
to match your zone/registry, then:

```sh
kubectl apply -f k8s/service.yaml -f k8s/deployment.yaml -f k8s/ingress.yaml
```

The deployment sets `TRANSLATION_LLM_URL=http://translation-llm:8000` so it resolves the sibling service in-cluster.