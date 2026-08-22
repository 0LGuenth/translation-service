#!/usr/bin/env python3

import json
import re
import urllib.request
from urllib.parse import urljoin
from pathlib import Path

OUTPUT_FILE = Path("opus_models.txt")

MODEL_PATTERN = re.compile(
    r"^Helsinki-NLP/opus-mt-([a-z]{2,5})-([a-z]{2,5})$"
)

START_URL = (
    "https://huggingface.co/api/models"
    "?author=Helsinki-NLP"
    "&search=opus-mt-"
    "&limit=1000"
)

def get_next_url(link_header):
    if not link_header:
        return None

    for part in link_header.split(","):
        if 'rel="next"' not in part:
            continue
        url = part.split(";")[0].strip().strip("<>")
        return urljoin("https://huggingface.co", url)
    return None

def main():
    url = START_URL
    seen = set()
    page = 1

    print(f"Erstelle: {OUTPUT_FILE.resolve()}")

    with OUTPUT_FILE.open("w", encoding="utf-8") as output:
        while url:
            print(f"Lade Seite {page} ...")

            request = urllib.request.Request(
                url,
                headers={
                    "User-Agent": "opus-model-fetcher/1.0"
                }
            )

            try:
                with urllib.request.urlopen(
                    request,
                    timeout=30
                ) as response:
                    models = json.load(response)
                    for model in models:
                        model_id = model.get("id", "")
                        if not MODEL_PATTERN.fullmatch(model_id):
                            continue
                        if model_id in seen:
                            continue
                        seen.add(model_id)
                        output.write(model_id + "\n")
                    output.flush()
                    
                    print(
                        f"  Insgesamt passende Modelle: {len(seen)}"
                    )
                    
                    url = get_next_url(
                        response.headers.get("Link")
                    )

            except Exception as error:
                print(f"Fehler: {error}")
                break

            page += 1

    print()
    print(f"Gefundene Modelle: {len(seen)}")
    print(f"Erstellt: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()