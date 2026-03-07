"""
ageoke v2 — Adaptive Geological Knowledge Extraction

Two ways to run:

  1. Script mode (edit parameters below, then run directly):
         python main.py

  2. CLI mode (pass arguments on the command line):
         python main.py extract data/inputs/usgs_bulletin.pdf
         python main.py extract-dir data/inputs/ --schema usgs_deposit_model
         python main.py extract-dirs data/inputs/dir1 data/inputs/dir2 --schema usgs_deposit_model
         python main.py ingest data/inputs/report.pdf
         python main.py list-schemas
"""
import asyncio
import sys
from pathlib import Path
from typing import Optional

# Ensure project root is on sys.path when run as a script
sys.path.insert(0, str(Path(__file__).parent))

# ============================================================
# USER CONFIGURATION — edit these parameters, then run:
#     python main.py
# ============================================================

# ── Single-directory mode ─────────────────────────────────────────────────────
# Directory containing the PDF files to process (used when DIRS_CONFIG is empty)
# INPUT_DIR = "data/inputs"
INPUT_DIR = "data/inputs/usgs_test"

# ── Multi-directory mode ──────────────────────────────────────────────────────
# To process multiple directories in sequence, populate DIRS_CONFIG below.
# Each entry is a dict with:
#   "dir"        : path to the input directory (required)
#   "schema"     : schema name override for this dir (optional, falls back to SCHEMA)
#   "skip_mindat": bool override (optional, falls back to SKIP_MINDAT)
#   "skip_geosciml": bool override (optional, falls back to SKIP_GEOSCIML)
#
# Example:
#   DIRS_CONFIG = [
#       {"dir": "data/inputs/usgs_test",   "schema": "usgs_deposit_model"},
#       {"dir": "data/inputs/lunar_test",  "schema": "lunar_basalt"},
#       {"dir": "data/inputs/generic_pdfs"},   # uses global SCHEMA below
#   ]
#
# Leave DIRS_CONFIG empty (default) to fall back to single INPUT_DIR mode.
DIRS_CONFIG: list[dict] = []

# ── Shared defaults ───────────────────────────────────────────────────────────
# Extraction schema name — set to the stem of any file in schemas/custom/.
# Available options (run `python main.py list-schemas` to verify):
#   None                 → uses default: "generic_geology" (broad fallback)
#   "usgs_deposit_model" → USGS Bulletin 1693 descriptive mineral deposit models
#   "lunar_basalt"       → Apollo lunar basalt sample catalog (NASA/LPI format)
#   "<your_schema>"      → any schemas/custom/<your_schema>.py you create
SCHEMA: Optional[str] = 'usgs_deposit_model'

# Set True to skip Mindat mineral/rock name normalization
SKIP_MINDAT = False

# Set True to skip GeoSciML controlled-vocabulary matching
SKIP_GEOSCIML = False

# Optional page range (0-based integers, or None for the full document)
START_PAGE: Optional[int] = None   # inclusive start page
END_PAGE: Optional[int] = None     # exclusive end page

# ============================================================
# Script-mode runner — no changes needed below this line
# ============================================================

import typer

from utils import settings, setup_phoenix
from utils.schema_registry import list_available_schemas, get_schema_metadata

tracer_provider = setup_phoenix()  # noqa: F841  (held for force_flush via atexit)

app = typer.Typer(
    name="ageoke",
    help="Adaptive Geological Knowledge Extraction v2.0",
    pretty_exceptions_enable=False,
)


async def _run_directory(
    input_dir: str,
    schema: Optional[str],
    skip_mindat: bool,
    skip_geosciml: bool,
    start_page: Optional[int],
    end_page: Optional[int],
) -> None:
    """Process all PDFs in input_dir.

    Final outputs go to data/outputs/<input_dir_name>_<schema_name>/ so that
    running the same directory with two different schemas (or two directories
    with the same schema) always produces separate, comparable output trees.
    """
    from client.workflow import run_pipeline

    input_path = Path(input_dir).resolve()
    if not input_path.is_dir():
        print(f"Error: directory not found: {input_path}", file=sys.stderr)
        sys.exit(1)

    pdf_files = sorted(
        list(input_path.glob("*.pdf")) + list(input_path.glob("*.PDF"))
    )

    if not pdf_files:
        print(f"No PDF files found in: {input_path}", file=sys.stderr)
        sys.exit(1)

    resolved_schema = schema or settings.default_schema_name
    # Final output dir mirrors what run_pipeline computes when output_dir=None.
    output_path = Path(settings.outputs_dir) / f"{input_path.name}_{resolved_schema}"

    print(f"Input dir : {input_path}")
    print(f"Output dir: {output_path}  (input-dir + schema namespaced)")
    print(f"Schema    : {resolved_schema}")
    print(f"Files     : {len(pdf_files)} PDF(s)\n")

    succeeded = 0
    skipped = 0
    failed = 0

    for pdf_file in pdf_files:
        print(f"--- {pdf_file.name} ---")
        # Do NOT pass output_dir — let run_pipeline apply the schema-suffix
        # logic so intermediate and final paths are all consistently namespaced.
        result = await run_pipeline(
            pdf_path=str(pdf_file),
            schema_name=schema,
            skip_mindat=skip_mindat,
            skip_geosciml=skip_geosciml,
            start_page=start_page,
            end_page=end_page,
        )
        print(result.summary())
        if result.skipped:
            skipped += 1
        elif result.success:
            succeeded += 1
            final = result.final_output_dir()
            if final:
                print(f"  -> {final}")
        else:
            failed += 1
        print()

    print(f"Done: {succeeded} succeeded, {skipped} skipped, {failed} failed out of {len(pdf_files)}")
    if failed:
        sys.exit(1)


async def _run_multiple_directories(
    dirs_config: list[dict],
    default_schema: Optional[str],
    default_skip_mindat: bool,
    default_skip_geosciml: bool,
) -> None:
    """Process multiple input directories in sequence.

    Each entry in dirs_config may override schema, skip_mindat, skip_geosciml
    per-directory; missing keys fall back to the supplied defaults.
    """
    total_succeeded = 0
    total_skipped = 0
    total_failed = 0

    for i, entry in enumerate(dirs_config, 1):
        dir_path = entry.get("dir")
        if not dir_path:
            print(f"[{i}/{len(dirs_config)}] Skipping entry with no 'dir' key: {entry}", file=sys.stderr)
            continue

        schema = entry.get("schema", default_schema)
        skip_mindat = entry.get("skip_mindat", default_skip_mindat)
        skip_geosciml = entry.get("skip_geosciml", default_skip_geosciml)

        print(f"\n{'='*60}")
        print(f"[{i}/{len(dirs_config)}] Directory: {dir_path}")
        print(f"{'='*60}")

        input_path = Path(dir_path).resolve()
        if not input_path.is_dir():
            print(f"  Error: directory not found — skipping: {input_path}", file=sys.stderr)
            total_failed += 1
            continue

        pdf_files = sorted(
            list(input_path.glob("*.pdf")) + list(input_path.glob("*.PDF"))
        )
        if not pdf_files:
            print(f"  No PDF files found — skipping: {input_path}", file=sys.stderr)
            continue

        from client.workflow import run_pipeline

        resolved_schema = schema or settings.default_schema_name
        output_path = Path(settings.outputs_dir) / f"{input_path.name}_{resolved_schema}"

        print(f"Input dir : {input_path}")
        print(f"Output dir: {output_path}  (input-dir + schema namespaced)")
        print(f"Schema    : {resolved_schema}")
        print(f"Files     : {len(pdf_files)} PDF(s)\n")

        dir_succeeded = 0
        dir_skipped = 0
        dir_failed = 0

        for pdf_file in pdf_files:
            print(f"--- {pdf_file.name} ---")
            result = await run_pipeline(
                pdf_path=str(pdf_file),
                schema_name=schema,
                skip_mindat=skip_mindat,
                skip_geosciml=skip_geosciml,
                start_page=None,
                end_page=None,
            )
            print(result.summary())
            if result.skipped:
                dir_skipped += 1
            elif result.success:
                dir_succeeded += 1
                final = result.final_output_dir()
                if final:
                    print(f"  -> {final}")
            else:
                dir_failed += 1
            print()

        print(f"  Dir done: {dir_succeeded} succeeded, {dir_skipped} skipped, {dir_failed} failed out of {len(pdf_files)}")
        total_succeeded += dir_succeeded
        total_skipped += dir_skipped
        total_failed += dir_failed

    print(f"\n{'='*60}")
    print(f"All dirs done: {total_succeeded} succeeded, {total_skipped} skipped, {total_failed} failed")
    if total_failed:
        sys.exit(1)


# ── Typer CLI commands ────────────────────────────────────────────────────────

@app.command()
def extract(
    pdf_path: str = typer.Argument(..., help="Path to input PDF file"),
    schema: Optional[str] = typer.Option(
        None,
        "--schema", "-s",
        help=(
            "Extraction schema name (default: from DEFAULT_SCHEMA_NAME env var). "
            "Run `list-schemas` to see available options."
        ),
    ),
    no_mindat: bool = typer.Option(False, "--no-mindat", help="Skip Mindat normalization"),
    no_geosciml: bool = typer.Option(False, "--no-geosciml", help="Skip GeoSciML vocabulary matching"),
    start_page: Optional[int] = typer.Option(None, "--start-page", help="0-based inclusive start page"),
    end_page: Optional[int] = typer.Option(None, "--end-page", help="0-based exclusive end page"),
) -> None:
    """Run the full extraction pipeline on a single PDF."""
    from client.workflow import run_pipeline

    result = asyncio.run(
        run_pipeline(
            pdf_path=pdf_path,
            schema_name=schema,
            skip_mindat=no_mindat,
            skip_geosciml=no_geosciml,
            start_page=start_page,
            end_page=end_page,
        )
    )

    typer.echo(result.summary())

    if result.success:
        typer.echo(f"\nOutput directory: {result.final_output_dir()}")
    else:
        raise typer.Exit(code=1)


@app.command(name="extract-dir")
def extract_dir(
    dir_path: str = typer.Argument(..., help="Directory containing PDF files"),
    schema: Optional[str] = typer.Option(None, "--schema", "-s"),
    no_mindat: bool = typer.Option(False, "--no-mindat"),
    no_geosciml: bool = typer.Option(False, "--no-geosciml"),
) -> None:
    """Run the full extraction pipeline on all PDFs in a directory."""
    asyncio.run(
        _run_directory(
            input_dir=dir_path,
            schema=schema,
            skip_mindat=no_mindat,
            skip_geosciml=no_geosciml,
            start_page=None,
            end_page=None,
        )
    )


@app.command(name="extract-dirs")
def extract_dirs(
    dir_paths: list[str] = typer.Argument(..., help="Two or more directories containing PDF files"),
    schema: Optional[str] = typer.Option(None, "--schema", "-s", help="Schema applied to all directories"),
    no_mindat: bool = typer.Option(False, "--no-mindat"),
    no_geosciml: bool = typer.Option(False, "--no-geosciml"),
) -> None:
    """Run the full extraction pipeline on multiple directories in sequence."""
    dirs_config = [{"dir": d} for d in dir_paths]
    asyncio.run(
        _run_multiple_directories(
            dirs_config=dirs_config,
            default_schema=schema,
            default_skip_mindat=no_mindat,
            default_skip_geosciml=no_geosciml,
        )
    )


@app.command()
def ingest(
    pdf_path: str = typer.Argument(..., help="Path to input PDF"),
    start_page: Optional[int] = typer.Option(None, "--start-page"),
    end_page: Optional[int] = typer.Option(None, "--end-page"),
) -> None:
    """Run only the ingestion step: PDF → Markdown (Docling)."""
    from servers.tools.ingestion import ingest_pdf

    result = ingest_pdf(pdf_path, start_page=start_page, end_page=end_page)

    if result["status"] == "success":
        typer.echo(f"Status:  {result['status']}")
        typer.echo(f"Output:  {result['output_path']}")
        typer.echo(f"Pages:   {result['pages_processed']}")
        typer.echo("\nPreview:")
        typer.echo(result["markdown_preview"])
    else:
        typer.echo(f"Error: {result.get('message')}", err=True)
        raise typer.Exit(code=1)


@app.command(name="list-schemas")
def list_schemas() -> None:
    """List all available extraction schemas (built-in + custom)."""
    schemas = list_available_schemas()
    if not schemas:
        typer.echo("No schemas found.")
        return

    typer.echo("Available schemas:\n")
    for name in schemas:
        try:
            meta = get_schema_metadata(name)
            typer.echo(f"  {name}")
            typer.echo(f"    {meta['description']}")
        except Exception:
            typer.echo(f"  {name}  (could not load metadata)")
    typer.echo(
        "\nTo add a custom schema: create schemas/custom/<name>.py "
        "and see schemas/custom/README.md"
    )


if __name__ == "__main__":
    # ── Temporary multi-dir run (CLI delegation commented out) ────────────────
    # Each input directory is run twice: once with the default schema
    # (generic_geology) and once with its domain-specific custom schema.
    asyncio.run(
        _run_multiple_directories(
            dirs_config=[
                # usgs directory — default schema first, then custom
                # {"dir": "data/inputs/usgs",   "schema": "generic_geology"},
                # {"dir": "data/inputs/usgs",   "schema": "usgs_deposit_model"},
                
                # basalt directory — default schema first, then custom
                {"dir": "data/inputs/basalt", "schema": "generic_geology"},
                # {"dir": "data/inputs/basalt", "schema": "lunar_basalt"},
            ],
            default_schema=SCHEMA,
            default_skip_mindat=SKIP_MINDAT,
            default_skip_geosciml=SKIP_GEOSCIML,
        )
    )

    # ── Restore below when done testing ──────────────────────────────────────
    # if len(sys.argv) == 1:
    #     if DIRS_CONFIG:
    #         asyncio.run(
    #             _run_multiple_directories(
    #                 dirs_config=DIRS_CONFIG,
    #                 default_schema=SCHEMA,
    #                 default_skip_mindat=SKIP_MINDAT,
    #                 default_skip_geosciml=SKIP_GEOSCIML,
    #             )
    #         )
    #     else:
    #         asyncio.run(
    #             _run_directory(
    #                 input_dir=INPUT_DIR,
    #                 schema=SCHEMA,
    #                 skip_mindat=SKIP_MINDAT,
    #                 skip_geosciml=SKIP_GEOSCIML,
    #                 start_page=START_PAGE,
    #                 end_page=END_PAGE,
    #             )
    #         )
    # else:
    #     app()
