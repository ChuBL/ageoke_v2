# utils/config.py
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # ── Azure OpenAI ──────────────────────────────────────────────────────────
    deployment_name: str = Field(validation_alias="AZURE_DEPLOYMENT_NAME")
    api_version: str = Field(validation_alias="AZURE_OPENAI_API_VERSION")
    azure_endpoint: str = Field(validation_alias="AZURE_OPENAI_API_ENDPOINT")
    api_key: str = Field(validation_alias="AZURE_OPENAI_API_KEY")

    # ── External APIs ─────────────────────────────────────────────────────────
    mindat_api_key: str = Field(default="", validation_alias="MINDAT_API_KEY")

    # ── Phoenix Tracing ───────────────────────────────────────────────────────
    phoenix_collector_endpoint: str = Field(
        default="https://app.phoenix.arize.com/s/mindat_ai_test", validation_alias="PHOENIX_COLLECTOR_ENDPOINT"
    )
    phoenix_api_key: str = Field(default="", validation_alias="PHOENIX_API_KEY")

    # ── LLM Parameters (formerly hardcoded per-server) ────────────────────────
    temperature: float = 0.7                        # general default
    extraction_temperature: float = 0.2             # structured extraction calls
    comparison_temperature: float = 0.1             # version comparison calls
    geosciml_temperature: float = 0.3               # GeoSciML vocab matching
    mindat_temperature: float = 0.3                 # Mindat name extraction

    # ── Extraction Pipeline Parameters ───────────────────────────────────────
    num_extraction_candidates: int = 3              # number of LLM versions to generate
    max_retries: int = 3                            # retries per LLM call
    llm_timeout_seconds: float = 45.0               # per-call LLM timeout
    file_processing_timeout_seconds: float = 600.0  # per-file processing timeout

    # ── Schema Registry ───────────────────────────────────────────────────────
    default_schema_name: str = "generic_geology"

    # ── Data Directory Paths ──────────────────────────────────────────────────
    data_root: Path = Path("data")
    inputs_dir: Path = Path("data/inputs")
    outputs_dir: Path = Path("data/outputs")
    debug_dir: Path = Path("data/debug")
    vocab_dir: Path = Path("data/vocabularies")
    mindat_cache_dir: Path = Path("data/mindat")

    # ── Output Subdirectory Names (formerly hardcoded strings in each server) ──
    intermediate_subdir: str = "intermediate"   # parent for all non-final outputs
    ingested_subdir: str = "ingested"
    candidates_subdir: str = "candidates"
    extracted_subdir: str = "docling_results"
    mindat_subdir: str = "mindat_matched"
    # geosciml (final stage) writes directly to outputs_dir — no subdir

    # ── GeoSciML Parameters ───────────────────────────────────────────────────
    geosciml_max_file_selections: int = 5
    geosciml_max_term_selections: int = 10

    # ── Mindat Field Targets (overridable per deployment via env) ─────────────
    # These are the default field names to process; schemas can override via metadata
    mindat_mineral_fields: list[str] = ["Mineralogy"]
    mindat_rock_fields: list[str] = ["Rock_Types"]

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )


# Singleton instance — import this everywhere
settings = Settings()
