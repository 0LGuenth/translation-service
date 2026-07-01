# translation-llm

Python inference service. Loads OPUS-MT models from HuggingFace on demand
and serves translations.

## Endpoints

- `POST /translate`: `{text, src_lang, tgt_lang}` -> `{translated, model}`
- `GET /health`: liveness probe target
- `GET /ready`: readiness probe target: flips true once `PRELOAD_PAIRS` are loaded

Models are looked up as `Helsinki-NLP/opus-mt-<src>-<tgt>`. First request
for an unseen pair triggers a load; subsequent requests hit the in-memory cache. 
Downloaded weights persist under `HF_HOME` (`/cache/huggingface` in the container), 
backed by a PVC in k8s so restarts don't re-download.

## Config (env vars)

| Var             | Default              | Notes                                                                   |
|-----------------|----------------------|-------------------------------------------------------------------------|
| `PRELOAD_PAIRS` | `de-en,en-de`        | Comma-separated `src-tgt` pairs loaded at startup; also gates `/ready`. |
| `HF_HOME`       | `/cache/huggingface` | HuggingFace cache dir. Set on the image.                                |

## Run locally

```sh
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000
curl -X POST localhost:8000/translate \
  -H content-type:application/json \
  -d '{"text":"hallo","src_lang":"de","tgt_lang":"en"}'
```

## Build & push

```sh
REGISTRY=registry.<your-zone> ./build-and-push.sh
```

## Deploy

Edit the image in `k8s/statefulset.yaml` to match your registry, then:

```sh
kubectl apply -f k8s/service.yaml -f k8s/statefulset.yaml
```

Deployed as a StatefulSet with a per-pod PVC (`hf-cache`, 10Gi) so the
HuggingFace cache survives restarts and rolls. Resource requests are sized
for CPU inference (500m/1500Mi request, 2000m/3Gi limit); at this point,
might be changed in the future.