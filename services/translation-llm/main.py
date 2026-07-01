import logging
import os
import threading
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from transformers import pipeline

log = logging.getLogger("translation-llm")
log.setLevel(logging.INFO)
if not log.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    log.addHandler(_h)
log.propagate = False

# Store models in cache; TODO implement cleanup for unused models
_pipes: dict[tuple[str, str], object] = {}
_lock = threading.Lock()
_ready = False


def _model_name(src: str, tgt: str) -> str:
    # OPUS-MT naming convention
    return f"Helsinki-NLP/opus-mt-{src}-{tgt}"


def _get_pipe(src: str, tgt: str):
    key = (src, tgt)
    if key in _pipes:
        return _pipes[key]
    with _lock:
        if key in _pipes:  # double-check after acquiring lock
            return _pipes[key]
        name = _model_name(src, tgt)
        log.info("loading model %s", name)
        t0 = time.time()
        try:
            p = pipeline("translation", model=name, device=-1)  # -1 = CPU
        except Exception as e:
            log.warning("model %s unavailable: %s", name, e)
            raise HTTPException(400, f"language pair {src}->{tgt} not supported")
        log.info("loaded %s in %.1fs", name, time.time() - t0)
        _pipes[key] = p
        return p


@asynccontextmanager
async def lifespan(_app: FastAPI):
    # Preload language pairs that appear often
    global _ready
    pairs = os.getenv("PRELOAD_PAIRS", "de-en,en-de")
    for spec in filter(None, (s.strip() for s in pairs.split(","))):
        src, tgt = spec.split("-", 1)
        try:
            _get_pipe(src, tgt)
        except HTTPException as e:
            log.warning("preload %s failed: %s", spec, e.detail)
    _ready = True
    yield


app = FastAPI(lifespan=lifespan)


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
    # Return ready when PRELOAD_PAIRS are loaded
    if not _ready:
        raise HTTPException(503, "not ready")
    return {"status": "ready"}


@app.post("/translate", response_model=TranslateResp)
def translate(req: TranslateReq):
    # Handle translation requests
    pipe = _get_pipe(req.src_lang, req.tgt_lang)
    out = pipe(req.text)[0]["translation_text"]
    return TranslateResp(translated=out, model=_model_name(req.src_lang, req.tgt_lang))
