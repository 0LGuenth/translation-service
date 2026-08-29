#!/usr/bin/env python3

import json
import re
from pathlib import Path

MODELS_FILE = Path("opus_models.txt")
ISO_FILE = Path("iso639-2.txt")

LANGUAGES_FILE = Path("languages.json")
PAIRS_FILE = Path("language_pairs.json")

MODEL_PATTERN = re.compile(
    r"^Helsinki-NLP/opus-mt-([a-z]{2,5})-([a-z]{2,5})$"
)


def load_language_names():
    names = {}

    with ISO_FILE.open("r", encoding="utf-8-sig") as file:
        for line in file:
            parts = line.strip().split("|")

            if len(parts) < 4:
                continue

            bibliographic = parts[0].strip()
            terminologic = parts[1].strip()
            alpha2 = parts[2].strip()
            english_name = parts[3].strip()

            if not english_name:
                continue

            for code in (bibliographic, terminologic, alpha2):
                if code:
                    names[code] = english_name

    return names


def main():
    language_names = load_language_names()

    all_pairs = set()
    valid_pairs = set()
    skipped_codes = set()

    with MODELS_FILE.open("r", encoding="utf-8") as file:
        for line in file:
            match = MODEL_PATTERN.fullmatch(line.strip())

            if not match:
                continue

            src, tgt = match.groups()

            if src == tgt:
                continue

            all_pairs.add((src, tgt))

            src_name = language_names.get(src)
            tgt_name = language_names.get(tgt)

            # Kein Name oder Name == Sprachcode:
            # Sprache nicht verwenden.
            if not src_name or src_name.lower() == src.lower():
                skipped_codes.add(src)
                continue

            if not tgt_name or tgt_name.lower() == tgt.lower():
                skipped_codes.add(tgt)
                continue

            valid_pairs.add((src, tgt))

    used_languages = {
        code
        for pair in valid_pairs
        for code in pair
    }

    languages_json = {
        code: language_names[code]
        for code in sorted(used_languages)
    }

    pairs_json = {
        "pairs": [
            {
                "src_lang": src,
                "tgt_lang": tgt
            }
            for src, tgt in sorted(valid_pairs)
        ]
    }

    LANGUAGES_FILE.write_text(
        json.dumps(
            languages_json,
            ensure_ascii=False,
            indent=2
        ) + "\n",
        encoding="utf-8"
    )

    PAIRS_FILE.write_text(
        json.dumps(
            pairs_json,
            ensure_ascii=False,
            indent=2
        ) + "\n",
        encoding="utf-8"
    )

    print(f"Gefundene Sprachpaare: {len(all_pairs)}")
    print(f"Gültige Sprachpaare: {len(valid_pairs)}")
    print(f"Verwendete Sprachen: {len(used_languages)}")
    print(f"Übersprungene Codes: {len(skipped_codes)}")

    if skipped_codes:
        print("Nicht geladen:")
        print(", ".join(sorted(skipped_codes)))


if __name__ == "__main__":
    main()