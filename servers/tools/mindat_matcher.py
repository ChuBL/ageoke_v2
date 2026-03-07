# servers/tools/mindat_matcher.py
"""
Mindat API mineral and rock normalization tool.

Replaces server_mindat.py with two improvements:
  1. Target fields read from schema metadata — not hardcoded "Mineralogy"/"Rock_Types"
  2. Cache paths from settings — not hardcoded "data/mindat"

Core matching logic (_normalize_mindat_name, _match_to_mindat, _extract_names_from_text)
is ported directly from server_mindat.py with only import/path changes.
"""
import ast
import json
import os
import unicodedata
from pathlib import Path
from typing import Optional

from openai import AsyncAzureOpenAI

from utils import settings, save_json, load_json, ensure_dir
from utils.schema_registry import get_schema_metadata


def _ensure_mindat_api_key() -> None:
    """
    Propagate MINDAT_API_KEY from pydantic settings into os.environ so that
    the openmindat library can find it. pydantic-settings reads .env into model
    fields but does NOT write back to os.environ; openmindat checks os.environ
    directly, so this bridge is required.
    """
    if settings.mindat_api_key and not os.environ.get("MINDAT_API_KEY"):
        os.environ["MINDAT_API_KEY"] = settings.mindat_api_key


# ── Mindat Cache Helpers ──────────────────────────────────────────────────────

def _mineral_cache_path() -> Path:
    return Path(settings.mindat_cache_dir) / "mindat_ima_list_normalized.json"


def _rock_cache_path() -> Path:
    return Path(settings.mindat_cache_dir) / "mindat_rock_list_normalized.json"


def _normalize_mindat_name(raw_path: Path) -> Path:
    """
    Add 'name_variants' field to each Mindat entry (original name + ASCII variant).

    Ported directly from server_mindat.py:284–317. Logic is identical.
    """
    def remove_accents(s: str) -> str:
        return "".join(
            c for c in unicodedata.normalize("NFD", s)
            if unicodedata.category(c) != "Mn"
        )

    data = load_json(raw_path)["results"]
    for entry in data:
        raw_name = entry["name"]
        ascii_name = remove_accents(raw_name)
        variants = {raw_name, ascii_name}
        if raw_name.lower().startswith("native "):
            stripped = raw_name[len("native "):].strip()
            variants.add(stripped)
            variants.add(remove_accents(stripped))
        entry["name_variants"] = list(variants)

    normalized_path = raw_path.with_name(f"{raw_path.stem}_normalized.json")
    save_json({"results": data}, normalized_path)
    return normalized_path


def ensure_mineral_cache() -> Path:
    """
    Ensure the normalized Mindat mineral list exists. Downloads if absent.
    Returns path to normalized JSON.
    """
    cache = _mineral_cache_path()
    if cache.exists():
        return cache

    ensure_dir(cache.parent)
    _ensure_mindat_api_key()
    from openmindat import GeomaterialRetriever

    raw_name = "mindat_ima_list"
    raw_path = cache.parent / f"{raw_name}.json"

    mir = GeomaterialRetriever()
    mir.ima(1).fields("id,name").verbose(1)
    mir.saveto(str(cache.parent), raw_name)

    return _normalize_mindat_name(raw_path)


def ensure_rock_cache() -> Path:
    """
    Ensure the normalized Mindat rock list exists (entrytype=7). Downloads if absent.
    Returns path to normalized JSON.
    """
    cache = _rock_cache_path()
    if cache.exists():
        return cache

    ensure_dir(cache.parent)
    _ensure_mindat_api_key()
    from openmindat import GeomaterialRetriever

    raw_name = "mindat_rock_list"
    raw_path = cache.parent / f"{raw_name}.json"

    gr = GeomaterialRetriever()
    # Use the .entrytype() method (expects int, wraps to list [7]) instead of
    # _params.update({"entrytype": "7"}) which passes a bare string and is rejected.
    gr.entrytype(7).fields("id,name,entrytype,entrytype_text").verbose(0)
    gr.saveto(str(cache.parent), raw_name)

    return _normalize_mindat_name(raw_path)


# ── Name Extraction via LLM ───────────────────────────────────────────────────

async def _extract_names_from_text(
    text: str,
    entity_type: str,
    raw_client: AsyncAzureOpenAI,
) -> list[str]:
    """
    Use the LLM to extract mineral or rock names from a geological description.

    Ported from server_mindat.py:689–783 (extract_mineral_name_from_text /
    extract_rock_name_from_text). Logic is identical; credentials from settings.

    Args:
        text:        Source text containing geological names.
        entity_type: "mineral" or "rock"
        raw_client:  Shared AsyncAzureOpenAI client (caller-managed).

    Returns:
        List of extracted name strings.
    """
    type_label = "mineral species" if entity_type == "mineral" else "rock species"
    example = (
        '["quartz", "hematite", "gibbsite"]'
        if entity_type == "mineral"
        else '["granite", "basalt", "schist"]'
    )

    system_prompt = (
        f"You are a geological assistant. Extract a list of all valid {type_label} names "
        f"from the given input. Only include valid {type_label} names. "
        f"Ignore chemical formulas, descriptive adjectives, and non-{entity_type} terms. "
        f"Output a Python list of strings using double quotes. No explanations.\n"
        f"Example: {example}"
    )
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"Input:\n{text}\n\nExtracted {entity_type}s:"},
    ]

    last_error: Exception = RuntimeError("No attempts made")
    for _ in range(settings.max_retries):
        assistant_reply = None
        try:
            response = await raw_client.chat.completions.create(
                model=settings.deployment_name,
                messages=messages,
                temperature=settings.mindat_temperature,
                timeout=settings.llm_timeout_seconds,
            )
            assistant_reply = response.choices[0].message.content.strip()
        except Exception as exc:
            last_error = exc
            continue  # API/network error — retry with the same messages

        # Try to parse the response
        extracted = None
        parse_error: Optional[Exception] = None
        try:
            extracted = json.loads(assistant_reply)
        except json.JSONDecodeError:
            try:
                extracted = ast.literal_eval(assistant_reply)
            except (ValueError, SyntaxError) as e:
                parse_error = e

        if (
            extracted is not None
            and isinstance(extracted, list)
            and all(isinstance(x, str) for x in extracted)
        ):
            return extracted

        # Feed the bad response back so the LLM can self-correct
        error_detail = f"parse error: {parse_error}" if parse_error else "not a list of strings"
        messages.append({"role": "assistant", "content": assistant_reply})
        messages.append({
            "role": "user",
            "content": (
                f"Your response was invalid ({error_detail}). "
                f"Return ONLY a Python list of strings. Example: {example}"
            ),
        })

    raise RuntimeError(
        f"Failed to extract {entity_type} names after {settings.max_retries} attempts. "
        f"Last error: {last_error}"
    )


# ── Mindat Matching ───────────────────────────────────────────────────────────

def _match_to_mindat(names: list[str], cache_path: Path) -> list[str]:
    """
    Match extracted names against the Mindat normalized cache.

    Returns list of "mindat_{id}_{name}" for matches, "unmatched_{name}" for misses.
    Ported from server_mindat.py:403–434. Logic is identical.
    """
    mindat_data = load_json(cache_path)["results"]

    variant_lookup: dict[str, str] = {}
    for entry in mindat_data:
        for variant in entry.get("name_variants", []):
            variant_lookup[variant.lower()] = f"mindat_{entry['id']}_{entry['name']}"

    results = []
    for name in names:
        key = name.lower()
        results.append(
            variant_lookup[key] if key in variant_lookup else f"unmatched_{name}"
        )

    return results


# ── Main Entry Point ──────────────────────────────────────────────────────────

async def normalize_mindat(
    input_dir: str,
    schema_name: Optional[str] = None,
    output_dir: Optional[str] = None,
    output_suffix: str = "_mindat",
) -> dict:
    """
    Process all JSON files in input_dir:
      - For each mineralogy field (from schema metadata): LLM extracts names → Mindat match
      - For each rock field (from schema metadata): LLM extracts names → Mindat match
      - Saves results to output_dir

    Field names are read from schema.schema_metadata() — NOT hardcoded.
    This means any schema with any field names works automatically.

    Args:
        input_dir:     Directory containing extracted JSON files.
        schema_name:   Schema identifier (default: settings.default_schema_name).
        output_dir:    Override output directory (optional).
        output_suffix: Suffix appended to the output filename stem (default: "_mindat").
                       Pass "_extracted" when this is the final pipeline stage.

    Returns:
        dict with status, processed_count, failed_count, failed_files,
               output_dir, schema_used, mineralogy_fields_processed, rock_fields_processed
    """
    input_path = Path(input_dir)
    if not input_path.exists():
        return {"status": "error", "message": f"Directory not found: {input_dir}"}

    # ── Resolve field lists from schema metadata ───────────────────────────────
    resolved_schema_name = schema_name or settings.default_schema_name
    try:
        meta = get_schema_metadata(resolved_schema_name)
    except (ValueError, AttributeError) as exc:
        return {"status": "error", "message": str(exc)}

    mineralogy_fields = meta.get("mineralogy_fields", settings.mindat_mineral_fields)
    rock_fields = meta.get("rock_fields", settings.mindat_rock_fields)

    # ── Resolve output directory ───────────────────────────────────────────────
    out_dir = (
        Path(output_dir) if output_dir
        else Path(settings.outputs_dir) / settings.intermediate_subdir / settings.mindat_subdir
    )
    ensure_dir(out_dir)

    # ── Ensure Mindat caches exist ─────────────────────────────────────────────
    try:
        mineral_cache = ensure_mineral_cache()
        rock_cache = ensure_rock_cache()
    except Exception as exc:
        return {
            "status": "error",
            "message": f"Failed to initialize Mindat cache: {exc}",
        }

    json_files = list(input_path.glob("*.json"))
    if not json_files:
        return {"status": "warning", "message": f"No JSON files found in {input_dir}"}

    # Shared client — one connection pool for all LLM calls in this run
    raw_client = AsyncAzureOpenAI(
        api_key=settings.api_key,
        api_version=settings.api_version,
        azure_endpoint=settings.azure_endpoint,
        azure_deployment=settings.deployment_name,
        max_retries=0,  # disable SDK retries; our own loop handles all retry logic
    )

    success_count = 0
    failed_files: list[dict] = []

    for json_file in json_files:
        try:
            content = load_json(json_file)

            # Process mineralogy fields
            for field in mineralogy_fields:
                text = content.get(field, "")
                if text and isinstance(text, str):
                    names = await _extract_names_from_text(text, "mineral", raw_client)
                    content[field] = _match_to_mindat(names, mineral_cache)

            # Process rock fields
            for field in rock_fields:
                text = content.get(field, "")
                if text and isinstance(text, str):
                    names = await _extract_names_from_text(text, "rock", raw_client)
                    content[field] = _match_to_mindat(names, rock_cache)

            out_name = json_file.stem.removesuffix("_docling") + f"{output_suffix}.json"
            save_json(content, out_dir / out_name)
            success_count += 1

        except Exception as exc:
            failed_files.append({"file": json_file.name, "error": str(exc)})

    return {
        "status": "success" if not failed_files else "partial",
        "processed_count": success_count,
        "failed_count": len(failed_files),
        "failed_files": failed_files,
        "output_dir": str(out_dir),
        "schema_used": resolved_schema_name,
        "mineralogy_fields_processed": mineralogy_fields,
        "rock_fields_processed": rock_fields,
    }
