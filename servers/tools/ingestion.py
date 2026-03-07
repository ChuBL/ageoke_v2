# servers/tools/ingestion.py
"""
Docling-based PDF ingestion tool.

Replaces server_ocr.py (Tesseract) with layout-aware parsing.

Key improvement over legacy:
  - Docling preserves table structure, section headers, and column layout.
  - Output is structured Markdown, not flat text with "--- PAGE N ---" markers.
  - The LLM in the extraction step gets much better context about document structure.

All configuration (paths, DPI equivalent settings) comes from utils.settings.
"""
from pathlib import Path
from typing import Optional

from utils import settings, save_text, ensure_dir


def _get_converter():
    """
    Build and return a Docling DocumentConverter.
    Lazy-imported so server startup is not slowed when this tool isn't used.
    """
    from docling.document_converter import DocumentConverter, PdfFormatOption
    from docling.datamodel.pipeline_options import PdfPipelineOptions

    pipeline_options = PdfPipelineOptions()
    pipeline_options.do_ocr = True               # Enable OCR for scanned pages
    pipeline_options.do_table_structure = True   # Preserve table structure in output

    return DocumentConverter(
        format_options={"pdf": PdfFormatOption(pipeline_options=pipeline_options)}
    )


def _pdf_to_markdown(
    pdf_path: Path,
    start_page: Optional[int],
    end_page: Optional[int],
) -> tuple[str, int]:
    """
    Convert PDF to Markdown using Docling.

    Args:
        pdf_path:   Path to the input PDF.
        start_page: 0-based inclusive start page (None = from beginning).
        end_page:   0-based exclusive end page (None = to end).

    Returns:
        (markdown_text, pages_processed)
    """
    from docling.datamodel.document import ConversionResult

    converter = _get_converter()

    # Docling uses 1-based page numbers internally
    kwargs: dict = {}
    if start_page is not None or end_page is not None:
        from docling.datamodel.base_models import DocumentLimits

        limits = DocumentLimits()
        if start_page is not None:
            limits.page_range_start = start_page + 1  # convert 0-based → 1-based
        if end_page is not None:
            # end_page is exclusive (0-based) → inclusive 1-based = end_page
            limits.page_range_end = end_page
        kwargs["limits"] = limits

    result: ConversionResult = converter.convert(str(pdf_path), **kwargs)

    if result.status.value != "success":
        raise RuntimeError(
            f"Docling conversion returned status '{result.status}' for {pdf_path}"
        )

    markdown_text = result.document.export_to_markdown()
    pages_processed = len(result.document.pages)

    return markdown_text, pages_processed


def ingest_pdf(
    pdf_path: str,
    output_dir: Optional[str] = None,
    start_page: Optional[int] = None,
    end_page: Optional[int] = None,
    save_debug: bool = True,
) -> dict:
    """
    Parse a PDF file to structured Markdown using Docling and save the result.

    Args:
        pdf_path:   Absolute or relative path to the input PDF.
        output_dir: Directory to save the .md file.
                    Defaults to settings.outputs_dir / settings.ingested_subdir.
        start_page: 0-based inclusive start page (optional).
        end_page:   0-based exclusive end page (optional).
        save_debug: If True, also saves a copy to settings.debug_dir/ingested/
                    for inspection.

    Returns:
        dict with keys:
          status ("success" | "error")
          output_path  — path to saved .md file
          pages_processed
          markdown_preview — first 500 chars of output
          message — present only on error
    """
    pdf_path_obj = Path(pdf_path)

    if not pdf_path_obj.exists():
        return {"status": "error", "message": f"File not found: {pdf_path}"}

    if pdf_path_obj.suffix.lower() not in (".pdf",):
        return {"status": "error", "message": f"Not a PDF file: {pdf_path}"}

    stem = pdf_path_obj.stem

    # Resolve output directory
    out_dir = (
        Path(output_dir)
        if output_dir
        else Path(settings.outputs_dir) / settings.intermediate_subdir / settings.ingested_subdir
    )
    ensure_dir(out_dir)
    output_path = out_dir / f"{stem}.md"

    try:
        markdown_text, pages_processed = _pdf_to_markdown(
            pdf_path_obj, start_page, end_page
        )

        # Ensure valid UTF-8
        markdown_text = markdown_text.encode("utf-8", errors="replace").decode("utf-8")

        save_text(markdown_text, output_path)

        if save_debug:
            debug_path = (
                Path(settings.debug_dir) / settings.ingested_subdir / f"{stem}.md"
            )
            save_text(markdown_text, debug_path)

        preview = (
            markdown_text[:500] + "\n\n[... truncated ...]"
            if len(markdown_text) > 500
            else markdown_text
        )

        return {
            "status": "success",
            "output_path": str(output_path),
            "pages_processed": pages_processed,
            "markdown_preview": preview,
        }

    except Exception as exc:
        return {"status": "error", "message": str(exc)}
