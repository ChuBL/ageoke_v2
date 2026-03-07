# servers/tools/geosciml_matcher.py
"""
GeoSciML vocabulary matching tool.

Replaces server_geosciml.py with two key improvements:
  1. target_keys read from schema metadata — not hardcoded list of 10 keys
  2. All paths from settings — not hardcoded "./data/vocabularies", "4vocab_selection"

Core two-level matching logic (_pick_vocab_files, _pick_vocab_terms) is ported
from server_geosciml.py (pick_geosciml_vocabulary_files, pick_geosciml_vocabulary)
with only path/import/credential changes.
"""
import ast
import asyncio
import json
from pathlib import Path
from typing import Optional

from openai import AsyncAzureOpenAI

from utils import settings, save_json, load_json, ensure_dir
from utils.geosciml import (
    download_geosciml_vocabularies,
    extract_ttl_members,
    generate_vocab_descriptions,
)
from utils.schema_registry import get_schema_metadata

_DESCRIPTIONS_FILENAME = "_geosciml_descriptions.md"


# ── Vocabulary Initialization ─────────────────────────────────────────────────

async def _ensure_vocabularies() -> Path:
    """
    Download GeoSciML TTL files if not present.
    Generate (or regenerate) vocabulary descriptions markdown if missing or incomplete.
    Returns path to descriptions .md file.
    """
    vocab_dir = Path(settings.vocab_dir)
    ensure_dir(vocab_dir)

    # Determine which TTL files are already present
    ttl_files = {p.name for p in vocab_dir.glob("*.ttl")}

    # Only attempt download when the vocab directory is empty; individual files
    # are also skipped inside the downloader, but calling it unconditionally
    # builds all URL mappings and touches the filesystem on every pipeline run.
    if not ttl_files:
        download_geosciml_vocabularies(output_dir=str(vocab_dir))
        ttl_files = {p.name for p in vocab_dir.glob("*.ttl")}

    descriptions_path = vocab_dir / _DESCRIPTIONS_FILENAME

    needs_generation = False
    if not descriptions_path.exists():
        needs_generation = True
    else:
        # Check if descriptions covers all current TTL files; regenerate if stale
        existing = _read_descriptions(descriptions_path)
        missing = ttl_files - set(existing.keys())
        if missing:
            descriptions_path.unlink()
            needs_generation = True

    if needs_generation:
        await generate_vocab_descriptions(
            VOCAB_PATH=str(vocab_dir),
            OUTPUT_PATH=str(descriptions_path),
        )

    return descriptions_path


def _read_descriptions(descriptions_path: Path) -> dict[str, str]:
    """
    Parse _geosciml_descriptions.md into {filename: description} dict.
    Ported from server_geosciml.py's inline parsing logic.
    """
    content = descriptions_path.read_text(encoding="utf-8")
    descriptions: dict[str, str] = {}

    for section in content.split("## ")[1:]:
        lines = section.strip().split("\n")
        if not lines:
            continue
        filename = lines[0].strip()
        description = ""
        for line in lines[1:]:
            if line.startswith("**Description:**"):
                description = line.replace("**Description:**", "").strip()
                break
        if filename and description:
            descriptions[filename] = description

    return descriptions


# ── Level 1: Vocabulary File Selection ───────────────────────────────────────

async def _pick_vocab_files(
    input_text: str,
    descriptions_path: Path,
    raw_client: AsyncAzureOpenAI,
) -> list[str]:
    """
    LLM selects the most relevant TTL vocabulary files for a given text.

    Ported from server_geosciml.py:pick_geosciml_vocabulary_files().
    Max selections from settings.geosciml_max_file_selections.
    Timeout from settings.llm_timeout_seconds.

    Returns list of TTL file names (without .ttl extension).
    """
    descriptions = _read_descriptions(descriptions_path)
    if not descriptions:
        return []

    available_files = [fname.replace(".ttl", "") for fname in descriptions]
    vocab_formatted = "\n".join(
        f"- {fname}: {desc}" for fname, desc in descriptions.items()
    )

    system_prompt = (
        f"You are a geological terminology expert specializing in GeosciML vocabulary.\n"
        f"Select the most relevant GeosciML vocabulary TTL files for the given geological description.\n\n"
        f"Available files ({len(descriptions)} total):\n{vocab_formatted}\n\n"
        f"Rules:\n"
        f"  1. File names must exist exactly in the provided list\n"
        f"  2. Maximum {settings.geosciml_max_file_selections} selections\n"
        f"  3. Quality over quantity — return only truly relevant files\n"
        f"  4. Return filenames WITHOUT the .ttl extension\n"
        f'  5. Output format: Python list, e.g., ["alterationtype", "faulttype"]\n'
        f"  6. If nothing is relevant, return: []"
    )
    human_prompt = f"Geological description:\n\n{input_text}\n\nSelected TTL files:"

    last_exc: Exception = RuntimeError("No attempts made")
    got_valid_response = False
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": human_prompt},
    ]

    for _ in range(settings.max_retries):
        assistant_reply = None
        try:
            response = await raw_client.chat.completions.create(
                model=settings.deployment_name,
                messages=messages,
                temperature=settings.geosciml_temperature,
                timeout=settings.llm_timeout_seconds,
            )
            assistant_reply = response.choices[0].message.content.strip()
        except Exception as exc:
            last_exc = exc
            continue  # API/timeout error — retry with the same messages

        # Try to parse the response
        extracted = None
        parse_error = None
        try:
            extracted = json.loads(assistant_reply)
        except json.JSONDecodeError:
            try:
                extracted = ast.literal_eval(assistant_reply)
            except (ValueError, SyntaxError) as e:
                parse_error = e

        if isinstance(extracted, list):
            got_valid_response = True
            valid = [f for f in extracted if f in available_files]
            return valid[: settings.geosciml_max_file_selections]

        # Feed the bad response back so the LLM can self-correct
        got_valid_response = True  # we got a reply; it was just the wrong shape
        error_detail = f"parse error: {parse_error}" if parse_error else "not a list"
        messages.append({"role": "assistant", "content": assistant_reply})
        messages.append({
            "role": "user",
            "content": (
                f"Your response was invalid ({error_detail}). "
                f'Return ONLY a Python list of filenames, e.g. ["alterationtype", "faulttype"]. '
                f"If nothing is relevant, return: []"
            ),
        })

    if not got_valid_response:
        raise RuntimeError(
            f"Vocab file selection failed after {settings.max_retries} retries: {last_exc}"
        )
    return []


# ── Level 2: Vocabulary Term Selection ───────────────────────────────────────

async def _pick_vocab_terms(
    input_text: str,
    ttl_file_path: Path,
    raw_client: AsyncAzureOpenAI,
) -> dict[str, list[str]]:
    """
    LLM selects specific terms from a TTL vocabulary file for a given text.

    Ported from server_geosciml.py:pick_geosciml_vocabulary().
    Includes validation: only terms that actually exist in the vocabulary are kept.

    Returns {uri: [list of valid terms]} or {} if nothing relevant.
    """
    collections = extract_ttl_members(str(ttl_file_path))
    if not collections:
        return {}

    collections_formatted = ""
    for uri, members in collections.items():
        members_str = ", ".join(members)
        collections_formatted += f"\nURI: {uri}\nMembers: {members_str}\n"

    system_prompt = (
        f"You are a geological terminology expert. Select the most relevant "
        f"GeosciML vocabulary terms for the given geological description.\n\n"
        f"Available collections:\n{collections_formatted}\n\n"
        f"Rules:\n"
        f"  1. Terms must exist EXACTLY in the provided collections\n"
        f"  2. Maximum {settings.geosciml_max_term_selections} terms total\n"
        f"  3. Return as JSON: {{\"uri\": [\"term1\", \"term2\"]}}\n"
        f"  4. Return ONLY the JSON object, no explanations\n"
        f"  5. If nothing is relevant, return: {{}}"
    )
    human_prompt = (
        f"Geological description:\n\n{input_text}\n\nSelected terms (JSON):"
    )

    last_exc: Exception = RuntimeError("No attempts made")
    got_valid_response = False
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": human_prompt},
    ]

    for _ in range(settings.max_retries):
        assistant_reply = None
        try:
            response = await raw_client.chat.completions.create(
                model=settings.deployment_name,
                messages=messages,
                temperature=settings.geosciml_temperature,
                timeout=settings.llm_timeout_seconds,
            )
            assistant_reply = response.choices[0].message.content.strip()
        except Exception as exc:
            last_exc = exc
            continue  # API/timeout error — retry with the same messages

        # Strip markdown fences if present
        clean = assistant_reply
        if clean.startswith("```"):
            clean = clean.split("```")[1]
            if clean.startswith("json"):
                clean = clean[4:]
            clean = clean.strip()

        # Try to parse the response
        extracted = None
        parse_error = None
        try:
            extracted = json.loads(clean)
        except json.JSONDecodeError:
            try:
                extracted = ast.literal_eval(clean)
            except (ValueError, SyntaxError) as e:
                parse_error = e

        if isinstance(extracted, dict):
            got_valid_response = True

            # Validate: only keep terms that genuinely exist in the vocabulary
            validated: dict[str, list[str]] = {}
            for uri, terms in extracted.items():
                if uri in collections and isinstance(terms, list):
                    valid_terms = [t for t in terms if t in collections[uri]]
                    if valid_terms:
                        validated[uri] = valid_terms

            if validated:
                return validated

            # Dict was valid but contained no matching terms — no point retrying
            return {}

        # Feed the bad response back so the LLM can self-correct
        got_valid_response = True  # we got a reply; it was just the wrong shape
        error_detail = f"parse error: {parse_error}" if parse_error else "not a JSON object"
        messages.append({"role": "assistant", "content": assistant_reply})
        messages.append({
            "role": "user",
            "content": (
                f"Your response was invalid ({error_detail}). "
                f'Return ONLY a JSON object mapping URIs to term lists, e.g. {{"uri": ["term1"]}}. '
                f"If nothing is relevant, return: {{}}"
            ),
        })

    if not got_valid_response:
        raise RuntimeError(
            f"Vocab term selection failed after {settings.max_retries} retries: {last_exc}"
        )
    return {}


# ── Per-field helper (enables parallel gather across target_keys) ─────────────

async def _match_one_field(
    key: str,
    value,
    descriptions_path: Path,
    vocab_dir: Path,
    raw_client: AsyncAzureOpenAI,
) -> tuple[str, object]:
    """
    Run the two-level GeoSciML match for a single field value.

    Returns (key, new_value) where new_value is either:
      - a {uri: [terms]} dict if matches were found
      - the original value if nothing matched
      - {"_error": str} on exception
    """
    if not value or (isinstance(value, str) and not value.strip()):
        return key, value

    input_text = str(value)

    try:
        # Level 1: select relevant TTL files
        relevant_files = await _pick_vocab_files(input_text, descriptions_path, raw_client)
        if not relevant_files:
            return key, value  # no match — leave original untouched

        # Level 2: select terms from each file (parallelised within this field)
        term_tasks = [
            _pick_vocab_terms(input_text, vocab_dir / f"{ttl_name}.ttl", raw_client)
            for ttl_name in relevant_files
            if (vocab_dir / f"{ttl_name}.ttl").exists()
        ]
        if not term_tasks:
            return key, value

        term_results = await asyncio.gather(*term_tasks, return_exceptions=True)
        combined: dict[str, list[str]] = {}
        for res in term_results:
            if not isinstance(res, BaseException) and res:
                combined.update(res)

        return key, combined if combined else value

    except Exception as exc:
        return key, {"_error": str(exc)}


# ── Main Entry Point ──────────────────────────────────────────────────────────

async def match_geosciml(
    input_dir: str,
    schema_name: Optional[str] = None,
    output_dir: Optional[str] = None,
) -> dict:
    """
    Match geological field values to GeoSciML controlled vocabulary terms.

    For each file in input_dir, for each field in schema's geosciml_fields:
      Step 1: Select relevant TTL files via LLM
      Step 2: Select specific terms from each TTL file via LLM
      Merge results → replace field value with {uri: [terms]} dict

    Target fields come from schema metadata — NOT hardcoded.

    Args:
        input_dir:   Directory containing JSON files to process.
        schema_name: Schema identifier (default: settings.default_schema_name).
        output_dir:  Override output directory (optional).

    Returns:
        dict with status, processed_count, failed_count, failed_files,
               output_dir, schema_used, target_keys_processed
    """
    input_path = Path(input_dir)
    if not input_path.exists():
        return {"status": "error", "message": f"Directory not found: {input_dir}"}

    # ── Resolve target fields from schema ──────────────────────────────────────
    resolved_schema_name = schema_name or settings.default_schema_name
    try:
        meta = get_schema_metadata(resolved_schema_name)
    except (ValueError, AttributeError) as exc:
        return {"status": "error", "message": str(exc)}

    target_keys: list[str] = meta.get("geosciml_fields", [])

    # mineralogy_fields and rock_fields are exclusively Mindat targets — exclude them
    # from GeoSciML matching even if a schema accidentally overlaps the lists.
    mindat_only: set[str] = set(meta.get("mineralogy_fields", [])) | set(meta.get("rock_fields", []))
    target_keys = [k for k in target_keys if k not in mindat_only]

    if not target_keys:
        return {
            "status": "warning",
            "message": f"Schema '{resolved_schema_name}' defines no geosciml_fields.",
        }

    # ── Resolve output directory ───────────────────────────────────────────────
    out_dir = (
        Path(output_dir) if output_dir
        else Path(settings.outputs_dir)
    )
    ensure_dir(out_dir)

    # ── Ensure vocabularies are downloaded and described ───────────────────────
    try:
        descriptions_path = await _ensure_vocabularies()
    except Exception as exc:
        return {
            "status": "error",
            "message": f"Vocabulary initialization failed: {exc}",
        }

    vocab_dir = Path(settings.vocab_dir)

    # ── Shared LLM client ──────────────────────────────────────────────────────
    raw_client = AsyncAzureOpenAI(
        api_key=settings.api_key,
        api_version=settings.api_version,
        azure_endpoint=settings.azure_endpoint,
        azure_deployment=settings.deployment_name,
        max_retries=0,  # disable SDK retries; our own loop handles all retry logic
    )

    json_files = list(input_path.glob("*.json"))
    if not json_files:
        return {"status": "warning", "message": f"No JSON files found in {input_dir}"}

    success_count = 0
    failed_files: list[dict] = []

    for json_file in json_files:
        # Final output is always suffixed _extracted regardless of input suffix
        base = json_file.stem
        for suffix in ("_mindat", "_docling"):
            base = base.removesuffix(suffix)
        out_file = out_dir / f"{base}_extracted.json"

        # Skip already-processed files
        if out_file.exists():
            success_count += 1
            continue

        try:
            data = load_json(json_file)

            # Process all target keys in parallel (each fires its own LLM calls)
            active_keys = [k for k in target_keys if k in data]
            field_tasks = [
                _match_one_field(k, data[k], descriptions_path, vocab_dir, raw_client)
                for k in active_keys
            ]
            field_results = await asyncio.gather(*field_tasks, return_exceptions=True)
            field_errors: list[str] = []
            for res in field_results:
                if isinstance(res, BaseException):
                    field_errors.append(f"task raised: {res}")
                else:
                    key, new_value = res
                    if isinstance(new_value, dict) and "_error" in new_value:
                        field_errors.append(f"field '{key}': {new_value['_error']}")
                    else:
                        data[key] = new_value

            if field_errors:
                for msg in field_errors:
                    print(f"  [geosciml] {json_file.name}: {msg}", flush=True)
                failed_files.append({"file": json_file.name, "error": "; ".join(field_errors)})
            else:
                save_json(data, out_file)
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
        "target_keys_processed": target_keys,
    }
