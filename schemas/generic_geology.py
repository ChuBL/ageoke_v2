# schemas/generic_geology.py
"""
Minimal fallback schema for unknown or mixed geological document formats.

Use when you don't know the source document structure, or when running a
quick extraction pass before deciding on a more specific schema.

Usage:
    python main.py extract doc.pdf --schema generic_geology
"""
from pydantic import Field

from schemas.base import BaseExtractionSchema


class SchemaModel(BaseExtractionSchema):
    """Minimal schema for generic geological documents."""

    title: str = Field(
        default="",
        description="Document or section title.",
    )
    deposit_name: str = Field(
        default="",
        description="Name of the deposit or geological occurrence.",
    )
    location: str = Field(
        default="",
        description="Geographic location or coordinates.",
    )
    commodities: str = Field(
        default="",
        description="Economic commodities (metals, minerals, etc.).",
    )
    mineralogy: str = Field(
        default="",
        description="Mineral assemblage.",
    )
    rock_types: str = Field(
        default="",
        description="Host rock and wall rock types.",
    )
    age: str = Field(
        default="",
        description="Geological age of formation.",
    )
    deposit_type: str = Field(
        default="",
        description="Deposit classification or type.",
    )
    tectonic_setting: str = Field(
        default="",
        description="Tectonic environment.",
    )
    description: str = Field(
        default="",
        description="General free-text description.",
    )
    references: str = Field(
        default="",
        description="References cited.",
    )
    # ai_modification_log inherited from BaseExtractionSchema

    @classmethod
    def schema_metadata(cls) -> dict:
        return {
            "name": "Generic Geology",
            "description": (
                "Minimal fallback schema for unknown or mixed geological document formats."
            ),
            "source_description": (
                "Extract any recognizable geological information. The document format "
                "is not standardized — do your best to identify and populate the most "
                "relevant fields from the available geological content."
            ),
            "mineralogy_fields": ["mineralogy"],
            "rock_fields": ["rock_types"],
            "geosciml_fields": ["age", "deposit_type", "tectonic_setting"],
        }
