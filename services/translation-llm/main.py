# Translation LLM — FastAPI wrapper around Helsinki-NLP OPUS-MT.
#
# CPU-only inference (DHBW VMs have no GPU). One pipeline per (src, tgt) pair,
# lazy-loaded on first request, cached in-process + on PVC under HF_HOME.
# Matches the {translated, model} shape the Go gateway's httpBackend expects.
import logging
import os
import threading
import time
from collections import OrderedDict
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from huggingface_hub import snapshot_download
from nltk.tokenize import sent_tokenize
from pydantic import BaseModel, Field
from transformers import pipeline

log = logging.getLogger("translation-llm")
log.setLevel(logging.INFO)
if not log.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    log.addHandler(_h)
log.propagate = False

# Cache: (src, tgt) -> HF pipeline. Loading is slow (~5–20s), so models stay
# alive until MAX_MODELS asks us to evict the least-recently-used pipeline.
_pipes: OrderedDict[tuple[str, str], object] = OrderedDict()
_loading_pairs: set[tuple[str, str]] = set()
_state_lock = threading.Lock()
_lock = threading.Lock()
_ready = False


def _parse_pairs(raw: str) -> set[tuple[str, str]]:
    pairs: set[tuple[str, str]] = set()
    for spec in (s.strip().lower() for s in raw.split(",")):
        if not spec:
            continue
        src, sep, tgt = spec.partition("-")
        if sep and src and tgt:
            pairs.add((src, tgt))
    return pairs


def _parse_positive_int(raw: str, default: int = 0) -> int:
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


_SUPPORTED_PAIRS = _parse_pairs(os.getenv("SUPPORTED_PAIRS", ""))
_MAX_MODELS = _parse_positive_int(os.getenv("MAX_MODELS", "0"))


def _model_name(src: str, tgt: str) -> str:
    # OPUS-MT naming convention. https://huggingface.co/Helsinki-NLP
    return f"Helsinki-NLP/opus-mt-{src}-{tgt}"


def _normalize_pair(src: str, tgt: str) -> tuple[str, str]:
    return src.strip().lower(), tgt.strip().lower()


def _existing_pipe(key: tuple[str, str]):
    with _state_lock:
        pipe = _pipes.get(key)
        if pipe is not None:
            _pipes.move_to_end(key)
        return pipe


def _model_cached_on_disk(src: str, tgt: str) -> bool:
    try:
        snapshot_download(repo_id=_model_name(src, tgt), local_files_only=True)
        return True
    except Exception:
        return False


def _get_pipe(src: str, tgt: str):
    key = _normalize_pair(src, tgt)
    if _SUPPORTED_PAIRS and key not in _SUPPORTED_PAIRS:
        raise HTTPException(400, f"language pair {src}->{tgt} not supported")

    cached_pipe = _existing_pipe(key)
    if cached_pipe is not None:
        return cached_pipe

    with _lock:
        cached_pipe = _existing_pipe(key)  # double-check after acquiring lock
        if cached_pipe is not None:
            return cached_pipe

        with _state_lock:
            _loading_pairs.add(key)
        try:
            src_norm, tgt_norm = key
            name = _model_name(src_norm, tgt_norm)
            log.info("loading model %s", name)
            t0 = time.time()
            try:
                p = pipeline("translation", model=name, device=-1)  # -1 = CPU
            except Exception as e:
                log.warning("model %s unavailable: %s", name, e)
                raise HTTPException(400, f"language pair {src}->{tgt} not supported")
            log.info("loaded %s in %.1fs", name, time.time() - t0)
            with _state_lock:
                _pipes[key] = p
                _pipes.move_to_end(key)
                while _MAX_MODELS > 0 and len(_pipes) > _MAX_MODELS:
                    evicted, _ = _pipes.popitem(last=False)
                    log.info("evicted model %s", _model_name(*evicted))
            return p
        finally:
            with _state_lock:
                _loading_pairs.discard(key)


def _requested_pair_status(src: str, tgt: str) -> dict[str, str]:
    key = _normalize_pair(src, tgt)
    src_norm, tgt_norm = key
    if _SUPPORTED_PAIRS and key not in _SUPPORTED_PAIRS:
        state = "unsupported"
    else:
        with _state_lock:
            loaded = key in _pipes
            loading = key in _loading_pairs
        if loaded:
            state = "loaded"
        elif loading:
            state = "loading"
        elif _model_cached_on_disk(src_norm, tgt_norm):
            state = "cached"
        else:
            state = "download_required"
    return {"src_lang": src_norm, "tgt_lang": tgt_norm, "state": state}


def _model_status(src: str | None = None, tgt: str | None = None) -> dict:
    with _state_lock:
        loaded_keys = list(_pipes.keys())
        loading_keys = sorted(_loading_pairs)
    result = {
        "loaded_pairs": [{"src_lang": src, "tgt_lang": tgt} for src, tgt in loaded_keys],
        "loading_pairs": [{"src_lang": src, "tgt_lang": tgt} for src, tgt in loading_keys],
    }
    if src and tgt:
        result["requested_pair"] = _requested_pair_status(src, tgt)
    return result


@asynccontextmanager
async def lifespan(_app: FastAPI):
    # Preload the demo pair so the first user request isn't a cold-start.
    # Override via PRELOAD_PAIRS="de-en,en-de" or set empty to skip.
    global _ready
    pairs = os.getenv("PRELOAD_PAIRS", "de-en")
    for spec in filter(None, (s.strip() for s in pairs.split(","))):
        src, tgt = spec.split("-", 1)
        try:
            _get_pipe(src, tgt)
        except HTTPException as e:
            log.warning("preload %s failed: %s", spec, e.detail)
    _ready = True
    yield


app = FastAPI(lifespan=lifespan)


@app.get("/model-status")
def model_status(src_lang: str | None = None, tgt_lang: str | None = None):
    return _model_status(src_lang, tgt_lang)


class TranslateReq(BaseModel):
    text: str = Field(min_length=1, max_length=5000)
    src_lang: str = Field(min_length=2, max_length=5)
    tgt_lang: str = Field(min_length=2, max_length=5)


class TranslateResp(BaseModel):
    translated: str
    model: str


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/ready")
def ready():
    # Readiness gates traffic until at least the preloaded pair is in memory —
    # avoids serving 5–20s cold-starts to the first user the Service routes to.
    if not _ready:
        raise HTTPException(503, "not ready")
    return {"status": "ready"}


# OPUS-MT is sentence-level — feeding a paragraph drops everything after the
# first <eos>. punkt knows abbreviations per language; map the src code to
# punkt's language name and fall back to english (its default) for anything
# punkt doesn't ship.
_PUNKT_LANG = {
    "de": "german", "en": "english", "fr": "french", "es": "spanish",
    "it": "italian", "pt": "portuguese", "nl": "dutch", "pl": "polish",
    "cs": "czech", "da": "danish", "fi": "finnish", "no": "norwegian",
    "sv": "swedish", "tr": "turkish", "ru": "russian", "et": "estonian",
    "el": "greek", "sl": "slovene",
}


@app.post("/translate", response_model=TranslateResp)
def translate(req: TranslateReq):
    t0 = time.time()
    pipe = _get_pipe(req.src_lang, req.tgt_lang)
    lang = _PUNKT_LANG.get(req.src_lang.lower()[:2], "english")
    sentences = sent_tokenize(req.text.strip(), language=lang)
    outs = pipe(sentences) if sentences else []
    translated = " ".join(o["translation_text"] for o in outs)
    src_norm, tgt_norm = _normalize_pair(req.src_lang, req.tgt_lang)
    model = _model_name(src_norm, tgt_norm)
    log.info(
        "translated text_len=%d src_lang=%s tgt_lang=%s model=%s sentence_count=%d latency_ms=%d",
        len(req.text),
        src_norm,
        tgt_norm,
        model,
        len(sentences),
        int((time.time() - t0) * 1000),
    )
    return TranslateResp(translated=translated, model=model)
