# servers/tools/extraction.py
"""
Instructor-based structured extraction tool.

Replaces server_preprocessor.py with three key improvements:
  1. Instructor handles Pydantic validation + retries natively (replaces fragile
     PydanticOutputParser + manual parsing).
  2. The schema is loaded dynamically from the registry — no hardcoded field lists.
  3. Output paths come from settings — no heuristic directory traversal.

The multi-version generation + LLM comparison pattern from v1 is preserved:
  - N candidate extractions are generated independently (default: 3)
  - A separate "expert geologist" LLM call selects the best one
This pattern adds robustness against OCR noise and LLM inconsistency.
"""
import asyncio
from pathlib import Path
from typing import Optional

from openai import AsyncAzureOpenAI

from utils import settings, save_json, load_text, ensure_dir
from utils.schema_registry import get_schema_class

# Ported from server_preprocessor.py:334–349 (text preserved, made schema-agnostic)
#
# The four-criterion rubric below is a principled quality-control mechanism
# for multi-candidate LLM extraction. Each criterion targets a distinct failure
# mode observed in single-pass LLM extraction of OCR-derived geological text:
#
#  1. TERMINOLOGICAL ACCURACY  — catches OCR corruption of specialist terms
#     (e.g. "clionpyroxene" → "clinopyroxene"). Independent candidates may
#     resolve the same ambiguous glyph differently; the comparator picks the
#     geologically correct reading.
#
#  2. GEOLOGICAL CONSISTENCY   — rejects internally inconsistent extractions
#     (e.g. olivine basalt mineralogy paired with a contradictory tectonic
#     context). A single extraction pass has no self-checking mechanism;
#     comparing N versions exposes such contradictions.
#
#  3. LANGUAGE CLARITY         — filters hallucinated or over-simplified values
#     that look plausible in isolation but diverge from the source document's
#     technical register (a known failure mode of high-temperature sampling).
#
#  4. MINIMAL INTERVENTION     — penalises versions that paraphrase or add
#     content not present in the source. The ai_modification_log field records
#     only genuine OCR corrections, providing an audit trail that distinguishes
#     LLM-inferred corrections from source content.
#
# The comparator runs at comparison_temperature=0.1 (settings) to minimise
# stochasticity in the selection decision itself. The fallback (first candidate)
# is triggered only if all comparator retries fail.
_COMPARISON_SYSTEM_PROMPT = """\
You are a senior economic geologist with expertise in mineral deposit modeling \
and geoscientific data curation. You are given multiple structured JSON-formatted \
versions of a geological record, each derived from a PDF document with AI-assisted extraction.

Your task is to compare these versions and select the single BEST version based on:

1. TERMINOLOGICAL ACCURACY — Are geological terms, rock and mineral names, and \
technical expressions correctly rendered? Favor versions that accurately correct \
OCR artifacts (e.g., "monagite" → "monazite").

2. GEOLOGICAL CONSISTENCY — Is the mineralogical, petrological, and tectonic content \
scientifically plausible and internally consistent?

3. LANGUAGE CLARITY AND PROFESSIONAL TONE — Prefer precise, technical language without \
unnecessary simplification or hallucination.

4. MINIMAL INTERVENTION — Favor versions that make only necessary and justifiable \
corrections to the original source, preserving its authentic structure and content.

Output ONLY the version identifier (version1, version2, version3, …). \
Optionally include a brief justification after the identifier.\
"""


def _build_instructor_client():
    """
    Create an Instructor-wrapped AsyncAzureOpenAI client.
    Instructor handles Pydantic schema validation and auto-retry on failure.
    """
    import instructor

    raw_client = AsyncAzureOpenAI(
        api_key=settings.api_key,
        api_version=settings.api_version,
        azure_endpoint=settings.azure_endpoint,
        azure_deployment=settings.deployment_name,
    )
    return instructor.from_openai(raw_client, mode=instructor.Mode.JSON)


def _build_raw_client() -> AsyncAzureOpenAI:
    """Plain AsyncAzureOpenAI client for non-structured calls (version comparison)."""
    return AsyncAzureOpenAI(
        api_key=settings.api_key,
        api_version=settings.api_version,
        azure_endpoint=settings.azure_endpoint,
        azure_deployment=settings.deployment_name,
    )


async def _generate_one_candidate(
    client,
    schema_class,
    system_prompt: str,
    raw_text: str,
    version_num: int,
    total: int,
) -> dict:
    """
    Generate one structured extraction candidate via Instructor.

    Instructor automatically:
      - Calls the LLM
      - Parses the JSON response into the Pydantic schema
      - Retries up to settings.max_retries times if validation fails
    """
    user_prompt = (
        f"Please extract structured data from the following geological text "
        f"(extraction attempt {version_num}/{total}):\n\n{raw_text}"
    )

    result = await client.chat.completions.create(
        model=settings.deployment_name,
        response_model=schema_class,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=settings.extraction_temperature,
        max_retries=settings.max_retries,
    )

    return result.model_dump()


async def _select_best_candidate(
    raw_client: AsyncAzureOpenAI,
    candidates: dict,
    raw_text: str,
) -> str:
    """
    Use an LLM call to pick the best candidate from multiple extractions.
    Returns the key of the winning version (e.g., "version1").
    Falls back to the first version if all attempts fail.
    """
    import json

    candidates_str = json.dumps(candidates, indent=2, ensure_ascii=False)
    valid_keys = list(candidates.keys())
    messages = [
        {"role": "system", "content": _COMPARISON_SYSTEM_PROMPT},
        {
            "role": "user",
            "content": (
                f"==== RAW SOURCE TEXT ====\n{raw_text}\n\n"
                f"==== EXTRACTED VERSIONS ====\n{candidates_str}"
            ),
        },
    ]

    for _ in range(settings.max_retries):
        assistant_reply = None
        try:
            response = await raw_client.chat.completions.create(
                model=settings.deployment_name,
                messages=messages,
                temperature=settings.comparison_temperature,
            )
            assistant_reply = response.choices[0].message.content.strip()
        except Exception:
            continue  # API/network error — retry with the same messages

        # Find which version key the LLM named in its response
        selected = next(
            (key for key in candidates if key.lower() in assistant_reply.lower()), None
        )
        if selected:
            return selected

        # Response didn't contain a valid identifier — feed it back so LLM can self-correct
        messages.append({"role": "assistant", "content": assistant_reply})
        messages.append({
            "role": "user",
            "content": (
                f"Your response did not contain a valid version identifier. "
                f"Reply with exactly one of: {', '.join(valid_keys)}."
            ),
        })

    return valid_keys[0]


async def extract_structured(
    input_path: str,
    schema_name: Optional[str] = None,
    output_dir: Optional[str] = None,
    num_candidates: Optional[int] = None,
) -> dict:
    """
    Extract structured geological data from a Markdown file.

    Steps:
      1. Load the schema from the registry by name.
      2. Build the system prompt from schema.get_extraction_prompt().
      3. Generate N candidate extractions (Instructor + Azure OpenAI).
      4. Save all candidates as {stem}_candidates.json.
      5. Use a separate LLM call to select the geologically best version.
      6. Save the winning extraction as {stem}_docling.json.

    Args:
        input_path:     Path to .md file (output of ingest_pdf).
        schema_name:    Schema identifier (default: settings.default_schema_name).
        output_dir:     Override output directory (optional).
        num_candidates: Override number of candidate versions (optional).

    Returns:
        dict with keys:
          status ("success" | "error")
          output_path        — path to final _docling.json
          candidates_path    — path to _candidates.json (all N versions)
          selected_version   — which version won (e.g. "version2")
          schema_used
          candidates_generated
          message            — present only on error
    """
    input_path_obj = Path(input_path)

    if not input_path_obj.exists():
        return {"status": "error", "message": f"File not found: {input_path}"}

    # ── Resolve schema ────────────────────────────────────────────────────────
    resolved_schema_name = schema_name or settings.default_schema_name
    try:
        schema_class = get_schema_class(resolved_schema_name)
    except (ValueError, AttributeError) as exc:
        return {"status": "error", "message": str(exc)}

    # ── Resolve output directories ────────────────────────────────────────────
    stem = input_path_obj.stem
    if output_dir:
        out_dir = Path(output_dir) / stem
    else:
        out_dir = (
            Path(settings.outputs_dir) / settings.intermediate_subdir / settings.extracted_subdir / stem
        )
    candidates_dir = out_dir / settings.candidates_subdir
    ensure_dir(candidates_dir)

    # ── Read input markdown ───────────────────────────────────────────────────
    try:
        raw_text = load_text(input_path_obj)
    except FileNotFoundError as exc:
        return {"status": "error", "message": str(exc)}

    # ── Build system prompt from schema ───────────────────────────────────────
    system_prompt = schema_class.get_extraction_prompt()

    # ── Generate N candidates (parallel) ─────────────────────────────────────
    n = num_candidates or settings.num_extraction_candidates
    instructor_client = _build_instructor_client()
    all_candidates: dict = {}

    tasks = [
        _generate_one_candidate(instructor_client, schema_class, system_prompt, raw_text, i + 1, n)
        for i in range(n)
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for i, res in enumerate(results):
        if not isinstance(res, BaseException):
            all_candidates[f"version{i + 1}"] = res

    if not all_candidates:
        return {
            "status": "error",
            "message": (
                "All extraction candidates failed. "
                "Check Azure OpenAI connectivity and schema configuration."
            ),
        }

    # ── Save candidates ───────────────────────────────────────────────────────
    candidates_path = candidates_dir / f"{stem}_candidates.json"
    save_json(all_candidates, candidates_path)

    # ── Select best candidate ─────────────────────────────────────────────────
    if len(all_candidates) == 1:
        selected_key = list(all_candidates.keys())[0]
    else:
        raw_client = _build_raw_client()
        selected_key = await _select_best_candidate(
            raw_client, all_candidates, raw_text
        )

    # ── Save final result ─────────────────────────────────────────────────────
    final_output = all_candidates[selected_key]
    final_path = out_dir / f"{stem}_docling.json"
    save_json(final_output, final_path)

    return {
        "status": "success",
        "output_path": str(final_path),
        "candidates_path": str(candidates_path),
        "selected_version": selected_key,
        "schema_used": resolved_schema_name,
        "candidates_generated": len(all_candidates),
    }
