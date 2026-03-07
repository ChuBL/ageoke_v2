# client/workflow.py
"""
Linear sequential pipeline for geological data extraction.

Replaces workflow_main.py (LangGraph supervisor with 5 subprocess agents)
with a simple deterministic async chain.

Rationale: the extraction pipeline is a fixed sequential ETL process
(ingest → extract → normalize → match). A LangGraph supervisor adds
complexity and latency with no benefit for a deterministic flow. For
interactive/agentic use, connect via client/agent.py instead.

Output directory layout (namespaced by input-dir + schema for side-by-side comparison):
    data/outputs/<input_dir>_<schema>/*_extracted.json             ← final output (any last stage)
    data/outputs/intermediate/<input_dir>_<schema>/docling_results/<stem>/   ← LLM extraction (*_docling.json)
    data/outputs/intermediate/<input_dir>_<schema>/mindat_matched/            ← Mindat-normalized (*_mindat.json)
    data/outputs/intermediate/ingested/                       ← schema-agnostic, shared

Timing:
    Each run appends one entry to data/outputs/<schema_name>/timing_log.json.
    Batch runs (multiple PDFs) accumulate all entries in the same file, making
    it easy to report per-stage and per-file processing times in a paper.
"""
import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


@dataclass
class PipelineResult:
    """Accumulates results and per-stage timing from all pipeline stages."""
    stages: dict = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    # {stage_name: elapsed_seconds}
    timing: dict[str, float] = field(default_factory=dict)
    skipped: bool = False
    skipped_path: Optional[str] = None

    def record(self, stage: str, result: dict, elapsed_s: float) -> None:
        """Record a stage result together with its wall-clock elapsed time."""
        self.stages[stage] = result
        self.timing[stage] = round(elapsed_s, 2)
        if result.get("status") == "error":
            self.errors.append(f"{stage}: {result.get('message', 'unknown error')}")

    @property
    def success(self) -> bool:
        return len(self.errors) == 0

    def final_output_dir(self) -> Optional[str]:
        """Return the deepest completed stage's output directory."""
        for stage in reversed(["geosciml", "mindat", "extraction", "ingestion"]):
            if stage not in self.stages:
                continue
            r = self.stages[stage]
            if "output_dir" in r:
                return r["output_dir"]
            if "output_path" in r:
                return str(Path(r["output_path"]).parent)
        return None

    def summary(self) -> str:
        if self.skipped:
            return f"  skipped — output already exists: {self.skipped_path}"
        lines = []
        for stage, result in self.stages.items():
            status = result.get("status", "unknown")
            elapsed = self.timing.get(stage, 0.0)
            lines.append(f"  {stage}: {status} ({elapsed}s)")
        total = round(sum(self.timing.values()), 2)
        lines.append(f"  total: {total}s")
        if self.errors:
            lines.append("Errors:")
            for e in self.errors:
                lines.append(f"  {e}")
        return "\n".join(lines)


def _append_timing_log(output_dir: str, entry: dict) -> None:
    """
    Append a timing entry to timing_log.json in output_dir.

    Creates the file if absent; appends to the "runs" list if it exists.
    This allows batch pipelines to accumulate all per-file timing in one file.
    """
    log_path = Path(output_dir) / "timing_log.json"
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    if log_path.exists():
        try:
            existing = json.loads(log_path.read_text(encoding="utf-8"))
            if not isinstance(existing, dict) or "runs" not in existing:
                existing = {"runs": []}
        except Exception:
            existing = {"runs": []}
    else:
        existing = {"runs": []}

    existing["runs"].append(entry)
    log_path.write_text(
        json.dumps(existing, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def _expected_final_file(
    pdf_path: str,
    resolved_schema_name: str,
    skip_mindat: bool,
    skip_geosciml: bool,
    output_dir: Optional[str],
) -> Path:
    """
    Return the path where the final output JSON will be written for pdf_path.

    Mirrors the path logic inside run_pipeline() so that the resume check is
    computed before any imports or async work are started.
    """
    from utils import settings

    stem = Path(pdf_path).stem
    input_dir_name = Path(pdf_path).parent.name
    dir_schema_suffix = f"{input_dir_name}_{resolved_schema_name}"
    final_out_dir = Path(output_dir) if output_dir else Path(settings.outputs_dir) / dir_schema_suffix

    if skip_mindat and skip_geosciml:
        # Extraction is the last stage; output lives inside a per-stem subdirectory.
        intermediate_base = (
            Path(settings.outputs_dir)
            / settings.intermediate_subdir
            / dir_schema_suffix
        )
        extract_out_dir = intermediate_base / settings.extracted_subdir
        return extract_out_dir / stem / f"{stem}_docling.json"

    # Any path that reaches final_out_dir is tagged _extracted — the stage-specific
    # suffixes (_docling, _mindat) are only used inside the intermediate tree.
    return final_out_dir / f"{stem}_extracted.json"


async def run_pipeline(
    pdf_path: str,
    schema_name: Optional[str] = None,
    skip_mindat: bool = False,
    skip_geosciml: bool = False,
    start_page: Optional[int] = None,
    end_page: Optional[int] = None,
    output_dir: Optional[str] = None,
) -> PipelineResult:
    """
    Run the complete extraction pipeline for a single PDF.

    Steps:
      1. ingest_pdf         → Markdown  (Docling layout-aware parsing)
      2. extract_structured → JSON      (Instructor + schema registry)
      3. normalize_mindat   → JSON      (Mindat API mineral/rock matching)  [skippable]
      4. match_geosciml     → JSON      (GeoSciML vocab matching)           [skippable]

    Output directories are namespaced by schema name so that extracting the
    same PDF with different schemas (e.g. generic_geology vs lunar_basalt)
    produces separate, comparable result trees — demonstrating adaptivity
    without any pipeline code changes.

    Timing:
      Each stage's wall-clock elapsed time is recorded in PipelineResult.timing
      and appended to timing_log.json in the final output directory.

    Args:
        pdf_path:      Path to input PDF.
        schema_name:   Schema to use (default: settings.default_schema_name).
        skip_mindat:   Skip Mindat normalization step.
        skip_geosciml: Skip GeoSciML vocabulary matching step.
        start_page:    0-based inclusive start page (optional).
        end_page:      0-based exclusive end page (optional).
        output_dir:    Override for the FINAL stage output directory. When None,
                       defaults to settings.outputs_dir / "<input_dir>_<schema_name>".

    Returns:
        PipelineResult containing per-stage results, timing, and any errors.
    """
    from utils import settings
    from servers.tools.ingestion import ingest_pdf
    from servers.tools.extraction import extract_structured
    from servers.tools.mindat_matcher import normalize_mindat
    from servers.tools.geosciml_matcher import match_geosciml

    # ── Resolve schema name once — drives all output paths ───────────────────
    resolved_schema_name = schema_name or settings.default_schema_name

    # ── Resume check: skip if final output already exists ────────────────────
    expected_final = _expected_final_file(
        pdf_path, resolved_schema_name, skip_mindat, skip_geosciml, output_dir
    )
    if expected_final.exists():
        result = PipelineResult()
        result.skipped = True
        result.skipped_path = str(expected_final)
        return result

    # ── Compute output directory suffix: <input_dir_name>_<schema_name> ───────
    # Using the input directory name (not the PDF stem) as a prefix means that
    # all PDFs from the same folder land in the same output tree, and running
    # the same folder with two different schemas produces distinct, comparable
    # output trees (e.g. mineral_generic_geology/ vs mineral_usgs_deposit_model/).
    input_dir_name = Path(pdf_path).parent.name
    dir_schema_suffix = f"{input_dir_name}_{resolved_schema_name}"

    # ── Compute namespaced output paths ───────────────────────────────────────
    # Ingested markdown is schema-agnostic (pure PDF parsing); shared across schemas.
    intermediate_base = (
        Path(settings.outputs_dir)
        / settings.intermediate_subdir
        / dir_schema_suffix
    )
    extract_out_dir = str(intermediate_base / settings.extracted_subdir)
    mindat_out_dir = str(intermediate_base / settings.mindat_subdir)
    final_out_dir = output_dir or str(Path(settings.outputs_dir) / dir_schema_suffix)

    pipeline_start = time.perf_counter()
    result = PipelineResult()

    # ── Step 1: Ingestion ──────────────────────────────────────────────────────
    t0 = time.perf_counter()
    ingest_result = ingest_pdf(pdf_path, start_page=start_page, end_page=end_page)
    result.record("ingestion", ingest_result, time.perf_counter() - t0)

    if ingest_result.get("status") == "error":
        _save_timing(result, pdf_path, resolved_schema_name, pipeline_start, final_out_dir)
        return result

    md_path = ingest_result["output_path"]

    # ── Step 2: Extraction ────────────────────────────────────────────────────
    t0 = time.perf_counter()
    extract_result = await extract_structured(
        md_path, schema_name=resolved_schema_name, output_dir=extract_out_dir
    )
    result.record("extraction", extract_result, time.perf_counter() - t0)

    if extract_result.get("status") == "error":
        _save_timing(result, pdf_path, resolved_schema_name, pipeline_start, final_out_dir)
        return result

    # extract_structured creates: extract_out_dir/{stem}/{stem}_docling.json
    # The dir containing the JSON is one level up from output_path
    extracted_dir = str(Path(extract_result["output_path"]).parent)

    # ── Step 3: Mindat normalization ──────────────────────────────────────────
    if not skip_mindat:
        # When mindat is the last active stage, write directly to final_out_dir
        mindat_target = final_out_dir if skip_geosciml else mindat_out_dir
        t0 = time.perf_counter()
        mindat_result = await normalize_mindat(
            extracted_dir,
            schema_name=resolved_schema_name,
            output_dir=mindat_target,
            output_suffix="_extracted" if skip_geosciml else "_mindat",
        )
        result.record("mindat", mindat_result, time.perf_counter() - t0)

        if mindat_result.get("status") == "error":
            _save_timing(result, pdf_path, resolved_schema_name, pipeline_start, final_out_dir)
            return result

        next_dir = mindat_result["output_dir"]
    else:
        next_dir = extracted_dir

    # ── Step 4: GeoSciML matching ─────────────────────────────────────────────
    if not skip_geosciml:
        t0 = time.perf_counter()
        geosciml_result = await match_geosciml(
            next_dir,
            schema_name=resolved_schema_name,
            output_dir=final_out_dir,
        )
        result.record("geosciml", geosciml_result, time.perf_counter() - t0)

    # ── Save timing log ────────────────────────────────────────────────────────
    _save_timing(result, pdf_path, resolved_schema_name, pipeline_start, final_out_dir)

    return result


def _save_timing(
    result: PipelineResult,
    pdf_path: str,
    schema_name: str,
    pipeline_start: float,
    output_dir: str,
) -> None:
    """Build a timing entry and append it to timing_log.json."""
    total_elapsed = round(time.perf_counter() - pipeline_start, 2)
    entry = {
        "run_id": datetime.now(timezone.utc).isoformat(),
        "pdf": pdf_path,
        "schema": schema_name,
        "stages": {stage: {"elapsed_s": elapsed} for stage, elapsed in result.timing.items()},
        "total_elapsed_s": total_elapsed,
        "success": result.success,
    }
    if result.errors:
        entry["errors"] = result.errors
    try:
        _append_timing_log(output_dir, entry)
    except Exception:
        pass  # Timing log failure must never abort the pipeline result
