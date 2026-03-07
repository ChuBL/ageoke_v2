# schemas/custom/usgs_deposit_model.py
"""
Schema for USGS Bulletin 1693 Descriptive Mineral Deposit Models.

Port of EntryExtractionResponse from the legacy server_preprocessor.py (lines 49–106).
Field names, aliases, and descriptions are preserved exactly from the legacy schema.

Usage:
    from utils.schema_registry import get_schema_class
    SchemaModel = get_schema_class("usgs_deposit_model")
"""
from pydantic import Field

from schemas.base import BaseExtractionSchema


class SchemaModel(BaseExtractionSchema):
    """Structured output for USGS descriptive mineral deposit model entries."""

    # ── Core Identity ──────────────────────────────────────────────────────────
    Model_Index: str = Field(
        default="",
        alias="Model Index",
        description="Short identifier like '13b' (1-2 digits + optional letter).",
    )
    Model_Name: str = Field(
        default="",
        alias="Model Name",
        description="Name of the deposit model.",
    )
    APPROXIMATE_SYNONYM: str = Field(
        default="",
        alias="APPROXIMATE SYNONYM",
        description="Alternative names for the model.",
    )
    DESCRIPTION: str = Field(
        default="",
        description="General description of the deposit model.",
    )
    GENERAL_REFERENCE: str = Field(
        default="",
        alias="GENERAL REFERENCE",
        description="Cited references relevant to the model.",
    )

    # ── Geological Characterization ────────────────────────────────────────────
    Rock_Types: str = Field(
        default="",
        alias="Rock Types",
        description="Host or associated rock types.",
    )
    Textures: str = Field(
        default="",
        description="Observed or inferred textural information.",
    )
    Age_Range: str = Field(
        default="",
        alias="Age Range",
        description="Geological time range of deposit formation.",
    )
    Depositional_Environment: str = Field(
        default="",
        alias="Depositional Environment",
        description="Depositional setting of the deposit.",
    )
    Tectonic_Settings: str = Field(
        default="",
        alias="Tectonic Setting(s)",
        description="Tectonic environment of formation.",
    )
    Associated_Deposit_Types: str = Field(
        default="",
        alias="Associated Deposit Types",
        description="Geologically related deposit types.",
    )
    Mineralogy: str = Field(
        default="",
        description="Mineral content of the deposit.",
    )
    Texture_Structure: str = Field(
        default="",
        alias="Texture/Structure",
        description="Structural or morphological features.",
    )
    Alteration: str = Field(
        default="",
        description="Associated alteration patterns.",
    )
    Ore_Controls: str = Field(
        default="",
        alias="Ore Controls",
        description="Structural or stratigraphic controls on ore.",
    )
    Weathering: str = Field(
        default="",
        description="Weathering characteristics or products.",
    )
    Geochemical_Signature: str = Field(
        default="",
        alias="Geochemical Signature",
        description="Typical geochemical anomalies or indicators.",
    )

    # ── Documentation ──────────────────────────────────────────────────────────
    EXAMPLES: str = Field(
        default="",
        description="Examples of deposits of this type.",
    )
    COMMENTS: str = Field(
        default="",
        description="Additional commentary.",
    )
    DEPOSITS: str = Field(
        default="",
        description="Deposit list or locality examples.",
    )
    # ai_modification_log inherited from BaseExtractionSchema

    class Config:
        populate_by_name = True  # Allow access by Python name OR alias

    @classmethod
    def schema_metadata(cls) -> dict:
        return {
            "name": "USGS Deposit Model",
            "description": (
                "USGS Bulletin 1693 series of descriptive mineral deposit models. "
                "Structured reference documents with standardized labeled sections."
            ),
            "source_description": (
                "This document follows USGS Bulletin 1693 format. Each entry begins "
                "with a Model Index (e.g., '13b') and Model Name, followed by labeled "
                "sections: APPROXIMATE SYNONYM, DESCRIPTION, GENERAL REFERENCE, "
                "Rock Types, Textures, Age Range, Depositional Environment, "
                "Tectonic Setting(s), Associated Deposit Types, Mineralogy, "
                "Texture/Structure, Alteration, Ore Controls, Weathering, "
                "Geochemical Signature, EXAMPLES, COMMENTS, DEPOSITS."
            ),
            # Fields for Mindat mineral/rock normalization
            "mineralogy_fields": ["Mineralogy"],
            "rock_fields": ["Rock_Types"],
            # Fields for GeoSciML vocabulary matching (ported from server_geosciml.py:798–809)
            "geosciml_fields": [
                "Textures",
                "Age_Range",
                "Depositional_Environment",
                "Tectonic_Settings",
                "Associated_Deposit_Types",
                "Texture_Structure",
                "Alteration",
                "Ore_Controls",
                "Weathering",
                "Geochemical_Signature",
            ],
        }
