# CPU-only PyTorch keeps the image "smaller" (~1gb)
FROM python:3.12-slim AS build

WORKDIR /app
ENV PIP_NO_CACHE_DIR=1 PYTHONDONTWRITEBYTECODE=1
COPY requirements.txt .
RUN pip install --prefix=/install -r requirements.txt

FROM python:3.12-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    HF_HOME=/cache/huggingface \
    NLTK_DATA=/usr/share/nltk_data

COPY --from=build /install /usr/local
COPY main.py .

# Bake punkt tokenizer data into the image -> no internet at startup required
RUN python -m nltk.downloader -d /usr/share/nltk_data punkt punkt_tab

# Non-root for least privilege — k8s securityContext will pin this anyway.
RUN useradd -u 10001 -m app && mkdir -p /cache/huggingface && chown -R app:app /cache
USER app

EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
