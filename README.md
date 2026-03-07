# ageoke v2 — Adaptive Geological Knowledge Extraction

A layout-aware pipeline for extracting structured geological data from PDF publications. Converts unstructured geoscience reports into validated JSON using [Docling](https://github.com/DS4SD/docling) for PDF parsing, [Instructor](https://python.useinstructor.com/) + Azure OpenAI for structured LLM extraction, and optional normalization against the [Mindat](https://www.mindat.org/) mineral database and [GeoSciML](https://www.seegrid.csiro.au/wiki/CGIModel/GeoSciML) controlled vocabularies.

---

## Features

- **Layout-aware PDF parsing** — Docling preserves headers, tables, and document structure as semantic Markdown, replacing brittle OCR approaches
- **Multi-candidate LLM extraction** — generates 3 extraction candidates per document, then uses a second LLM call to select the best result
- **Pluggable schemas** — built-in USGS Bulletin 1693 deposit model schema; add new schemas by dropping a single Python file in `schemas/custom/`
- **Mindat normalization** — maps extracted mineral and rock names to canonical Mindat entries
- **GeoSciML vocabulary matching** — aligns geological terms to IUGS/CGI controlled vocabularies (age, lithology, tectonic setting, etc.)
- **MCP server architecture** — tools exposed via [FastMCP](https://github.com/jlowin/fastmcp) for integration with AI assistants or other MCP clients
- **Observable** — full LLM call tracing via [Arize Phoenix](https://phoenix.arize.com/)
- **Configurable** — all temperatures, retries, timeouts, and directory paths are env-overridable via `.env`

---

## Architecture

```
PDF
 └─► Docling (ingestion)          → Markdown
      └─► Instructor (extraction) → Pydantic JSON  (3 candidates → best selected)
           ├─► Mindat API         → normalized mineral/rock names
           └─► GeoSciML vocab     → controlled-vocabulary term alignment
                └─► data/outputs/<doc>/
```

The pipeline runs as a sequential async workflow (`client/workflow.py`). Tools are also exposed through a single FastMCP server (`servers/geo_server.py`) for MCP client integration.

---

## Requirements

- Python ≥ 3.12
- Azure OpenAI deployment (GPT-4o recommended)
- Mindat API key (optional, for mineral normalization)
- Arize Phoenix endpoint (optional, for tracing)

---

## Installation

```bash
git clone https://github.com/<your-org>/ageoke_v2.git
cd ageoke_v2

uv venv && uv sync
source .venv/bin/activate
```

### Environment

Copy `.env.example` to `.env` and fill in your credentials:

```env
AZURE_DEPLOYMENT_NAME=gpt-4o
AZURE_OPENAI_API_VERSION=2024-08-01-preview
AZURE_OPENAI_API_ENDPOINT=https://<your-resource>.openai.azure.com/
AZURE_OPENAI_API_KEY=<your-key>

# Optional
MINDAT_API_KEY=<your-key>
PHOENIX_COLLECTOR_ENDPOINT=http://localhost:6006/v1/traces
```

---

## Usage

### Script mode (recommended for batch runs)

Edit the parameters at the top of `main.py`, then run:

```python
# main.py — edit these before running
INPUT_DIR   = "data/inputs/my_papers"   # folder containing PDFs
SCHEMA      = None                       # None = use default (usgs_deposit_model)
SKIP_MINDAT = False
SKIP_GEOSCIML = False
START_PAGE  = None                       # 0-based, or None for full document
END_PAGE    = None
```

```bash
python main.py
```

Outputs are written to `data/outputs/<input_folder_name>_output/` — one JSON file per PDF.

### CLI mode

#### Extract a single PDF

```bash
# Full pipeline with default schema (USGS deposit model)
python main.py extract data/inputs/usgs_bulletin.pdf

# Skip Mindat and GeoSciML steps
python main.py extract data/inputs/report.pdf --no-mindat --no-geosciml

# Use a custom schema, process specific pages only
python main.py extract data/inputs/bgs_log.pdf --schema bgs_borehole --start-page 0 --end-page 5
```

#### Batch extraction

```bash
python main.py extract-dir data/inputs/my_papers/ --schema usgs_deposit_model
```

Outputs are written to `data/outputs/my_papers_output/`.

#### Ingestion only (PDF → Markdown)

```bash
python main.py ingest data/inputs/report.pdf
```

#### List available schemas

```bash
python main.py list-schemas
```

---

## Output

Final results are written to `data/outputs/<folder_name>_output/` — one JSON file per PDF. Intermediate files from each pipeline stage are stored under `data/outputs/intermediate/`:

| Location | Contents |
|---|---|
| `data/outputs/intermediate/ingested/` | Docling-parsed Markdown |
| `data/outputs/intermediate/extracted/<stem>/` | 3 LLM candidates + best-selected JSON |
| `data/outputs/intermediate/mindat_matched/` | Extraction with normalized mineral/rock names |
| `data/outputs/<folder>_output/` | **Final output** with GeoSciML vocabulary alignments |

---

## Adding a Custom Schema

1. Create `schemas/custom/my_source.py` with a `SchemaModel` class extending `BaseExtractionSchema`
2. Implement `schema_metadata()` — declare which fields to send to Mindat and GeoSciML matching
3. Run `python main.py list-schemas` to confirm it is auto-discovered

See [schemas/usgs_deposit_model.py](schemas/usgs_deposit_model.py) for a complete example and `schemas/custom/README.md` for field conventions.

---

## Built-in Schemas

| Schema name | Description |
|---|---|
| `usgs_deposit_model` | USGS Bulletin 1693 descriptive mineral deposit models (20 fields) |
| `generic_geology` | Minimal fallback schema for unstructured geological text |

---

## Configuration Reference

All settings live in `utils/config.py` (Pydantic `BaseSettings`) and can be overridden via environment variables or `.env`:

| Variable | Default | Description |
|---|---|---|
| `DEFAULT_SCHEMA_NAME` | `usgs_deposit_model` | Schema used when `--schema` is not specified |
| `NUM_EXTRACTION_CANDIDATES` | `3` | LLM candidates generated per document |
| `EXTRACTION_TEMPERATURE` | `0.2` | Temperature for extraction calls |
| `MAX_RETRIES` | `3` | Retries per LLM call |
| `LLM_TIMEOUT_SECONDS` | `120` | Per-call timeout |
| `FILE_PROCESSING_TIMEOUT_SECONDS` | `600` | Per-file pipeline timeout |

---

## MCP Server

The FastMCP server exposes pipeline tools for integration with MCP-compatible AI assistants:

```bash
python servers/geo_server.py
```

---

## Development

```bash
# Verify schema registry
.venv/bin/python -c "from utils.schema_registry import list_available_schemas; print(list_available_schemas())"

# Run tests
pytest
```

---

## License

See [LICENSE](LICENSE).