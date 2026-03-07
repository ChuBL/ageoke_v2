# servers/geo_server.py
"""
Single unified FastMCP server for all geological data extraction tools.

Consolidates the 4 legacy servers (server_ocr, server_preprocessor,
server_mindat, server_geosciml) into one FastMCP instance.

Tool implementations live in servers/tools/ — this file is only the
registration layer that wires them into the MCP protocol.

Run as:
    python -m servers.geo_server
    fastmcp run servers/geo_server.py
"""
from typing import Optional

from fastmcp import FastMCP

from utils import settings, setup_phoenix

# Initialise tracing on server startup
setup_phoenix()

mcp = FastMCP(
    name="GeoKnowledgeExtractor",
    instructions=(
        "Geological data extraction pipeline. "
        "Available tools (run in order): "
        "ingest_pdf → extract_structured → normalize_mindat → match_geosciml"
    ),
)


# ── Tool: ingest_pdf ──────────────────────────────────────────────────────────

@mcp.tool()
def ingest_pdf(
    pdf_path: str,
    output_dir: Optional[str] = None,
    start_page: Optional[int] = None,
    end_page: Optional[int] = None,
) -> dict:
    """
    Parse a PDF to structured Markdown using Docling (layout-aware).

    Replaces the legacy Tesseract OCR step. Preserves table structure,
    section headers, and column layout — giving the extraction LLM
    much better context about document structure.

    Args:
        pdf_path:   Absolute or relative path to the input PDF.
        output_dir: Override output directory (optional).
        start_page: 0-based inclusive start page (optional).
        end_page:   0-based exclusive end page (optional).

    Returns:
        {status, output_path, pages_processed, markdown_preview}
    """
    from servers.tools.ingestion import ingest_pdf as _ingest

    return _ingest(pdf_path, output_dir, start_page, end_page)


# ── Tool: extract_structured ──────────────────────────────────────────────────

@mcp.tool()
async def extract_structured(
    input_path: str,
    schema_name: Optional[str] = None,
    output_dir: Optional[str] = None,
    num_candidates: Optional[int] = None,
) -> dict:
    """
    Extract structured geological data from a Markdown file.

    Uses Instructor + Azure OpenAI GPT-4o. Generates N candidate extractions,
    then a separate LLM call selects the geologically best one.

    The extraction schema is determined by schema_name (default: generic_geology).
    New schemas are added by creating a file in schemas/custom/ — no server changes.

    Args:
        input_path:     Path to .md file produced by ingest_pdf.
        schema_name:    Schema identifier, e.g. "usgs_deposit_model", "lunar_basalt".
                        Run `ageoke list-schemas` to see available options.
        output_dir:     Override output directory (optional).
        num_candidates: Override number of candidate versions to generate (optional).

    Returns:
        {status, output_path, candidates_path, selected_version, schema_used}
    """
    from servers.tools.extraction import extract_structured as _extract

    return await _extract(input_path, schema_name, output_dir, num_candidates)


# ── Tool: normalize_mindat ────────────────────────────────────────────────────

@mcp.tool()
async def normalize_mindat(
    input_dir: str,
    schema_name: Optional[str] = None,
    output_dir: Optional[str] = None,
) -> dict:
    """
    Extract mineral and rock names from extracted JSON files and match
    them to Mindat database IDs.

    Which fields are processed is determined by the schema's mineralogy_fields
    and rock_fields metadata — not hardcoded. This makes it work with any schema.

    Args:
        input_dir:   Directory containing extracted JSON files.
        schema_name: Schema used to read field configuration (optional).
        output_dir:  Override output directory (optional).

    Returns:
        {status, processed_count, failed_count, failed_files, output_dir, schema_used}
    """
    from servers.tools.mindat_matcher import normalize_mindat as _normalize

    return await _normalize(input_dir, schema_name, output_dir)


# ── Tool: match_geosciml ──────────────────────────────────────────────────────

@mcp.tool()
async def match_geosciml(
    input_dir: str,
    schema_name: Optional[str] = None,
    output_dir: Optional[str] = None,
) -> dict:
    """
    Match geological field values to GeoSciML controlled vocabulary terms.

    Two-level matching: first selects relevant TTL vocabulary files,
    then selects specific terms within those files.

    Which fields are matched is determined by the schema's geosciml_fields
    metadata — not hardcoded. This makes it work with any schema.

    Args:
        input_dir:   Directory containing Mindat-normalized JSON files.
        schema_name: Schema used to read field configuration (optional).
        output_dir:  Override output directory (optional).

    Returns:
        {status, processed_count, failed_count, failed_files, output_dir, schema_used}
    """
    from servers.tools.geosciml_matcher import match_geosciml as _match

    return await _match(input_dir, schema_name, output_dir)


if __name__ == "__main__":
    mcp.run(transport="stdio")
