# schemas/custom/lunar_basalt.py
"""
Schema for Apollo lunar basalt sample catalog documents.

Source format: NASA/Lunar and Planetary Institute sample catalog PDFs, one per
Apollo sample (e.g., 10017, 10020, 10022). Each document follows a standard
structure: title block → Introduction → Petrography → Mineralogy → Chemistry
→ Studies/References. Sample IDs are four- or five-digit numbers (e.g., 10017,
75035, 79155).

This schema is designed for demonstrating schema adaptivity: the same pipeline
that processes USGS Bulletin 1693 deposit model entries (usgs_deposit_model)
can process Apollo basalt catalogs by switching schema — no other code changes.

Usage:
    python main.py extract data/inputs/basalt/10017.pdf --schema lunar_basalt
    python main.py batch data/inputs/basalt/ --schema lunar_basalt
"""
from pydantic import Field

from schemas.base import BaseExtractionSchema


class SchemaModel(BaseExtractionSchema):
    """Structured output for NASA Apollo lunar basalt sample catalog entries."""

    # ── Sample Identity ───────────────────────────────────────────────────────
    Sample_ID: str = Field(
        default="",
        alias="Sample ID",
        description=(
            "Numeric sample identifier (four or five digits), e.g. '10017', '75035'. "
            "Found in the document title and used as the primary identifier."
        ),
    )
    Sample_Name: str = Field(
        default="",
        alias="Sample Name",
        description=(
            "Descriptive name of the sample, e.g. 'Ilmenite Basalt (high K)', "
            "'Mare Basalt', 'Olivine Basalt'."
        ),
    )
    Mission: str = Field(
        default="",
        description=(
            "Apollo mission that collected the sample, e.g. 'Apollo 11', 'Apollo 17'. "
            "Inferred from sample ID range if not stated explicitly."
        ),
    )
    Landing_Site: str = Field(
        default="",
        alias="Landing Site",
        description=(
            "Lunar landing site or collection locality, e.g. 'Sea of Tranquillity', "
            "'Taurus-Littrow', 'Fra Mauro'."
        ),
    )
    Classification: str = Field(
        default="",
        description=(
            "Petrological subtype, e.g. 'high-K ilmenite basalt', 'low-K ilmenite basalt', "
            "'olivine basalt', 'KREEP basalt', 'mare basalt'."
        ),
    )
    Sample_Weight: str = Field(
        default="",
        alias="Sample Weight",
        description="Total mass of the sample as cataloged, e.g. '973 grams', '425 g'.",
    )

    # ── Description ───────────────────────────────────────────────────────────
    Description: str = Field(
        default="",
        description=(
            "General introduction describing the sample: origin, gross morphology "
            "(rounded, vesicular, etc.), and notable characteristics reported in the "
            "Introduction section."
        ),
    )

    # ── Petrography ───────────────────────────────────────────────────────────
    Petrology: str = Field(
        default="",
        description=(
            "Petrographic description from the Petrography section: texture names "
            "(e.g. subophitic, intersertal, poikilitic), grain size, vesicle "
            "characteristics, mineral grain relationships, and any notable micro-textures."
        ),
    )

    # ── Mineralogy ────────────────────────────────────────────────────────────
    Mineralogy: str = Field(
        default="",
        description=(
            "Mineral assemblage of the sample. Include all phases reported in the "
            "Mineralogy section: primary minerals (pyroxene, plagioclase, ilmenite, "
            "olivine), accessory phases (troilite, armalcolite, spinel, apatite, "
            "silica polymorphs), and mesostasis composition. Include modal abundances "
            "if given (e.g. 'pyroxene 50 vol.%, plagioclase 24 vol.%')."
        ),
    )
    Rock_Type: str = Field(
        default="",
        alias="Rock Type",
        description=(
            "Broad rock type classification, e.g. 'Basalt', 'Mare Basalt'. "
            "Use the most general applicable term."
        ),
    )

    # ── Geochronology ─────────────────────────────────────────────────────────
    Age_Crystallization: str = Field(
        default="",
        alias="Age Crystallization",
        description=(
            "Radiometric crystallization age of the sample, e.g. '3.77 Ga', '3.6 b.y.', "
            "'3.59 ± 0.06 b.y.'. Include method if reported (Ar/Ar, Rb/Sr, Sm/Nd)."
        ),
    )
    Age_Exposure: str = Field(
        default="",
        alias="Age Exposure",
        description=(
            "Cosmic-ray exposure age (time on or near the lunar surface), "
            "e.g. '480 Ma', '~130 m.y.', '380–520 m.y.'."
        ),
    )

    # ── Geochemistry ──────────────────────────────────────────────────────────
    Geochemistry: str = Field(
        default="",
        description=(
            "Summary of major- and trace-element chemistry from the Chemistry section. "
            "Include key oxide abundances (TiO2, FeO, Al2O3, MgO, K2O, etc.) and any "
            "notable geochemical characteristics (high-Ti, KREEP signature, REE patterns)."
        ),
    )

    # ── References ────────────────────────────────────────────────────────────
    References: str = Field(
        default="",
        description="Key references cited in the document for this sample.",
    )
    # ai_modification_log inherited from BaseExtractionSchema

    class Config:
        populate_by_name = True  # Allow access by Python name OR alias

    @classmethod
    def schema_metadata(cls) -> dict:
        return {
            "name": "Apollo Lunar Basalt Sample",
            "description": (
                "NASA/LPI Apollo lunar basalt sample catalog entries. Each document "
                "describes one Apollo rock sample with standard sections: Introduction, "
                "Petrography, Mineralogy, Chemistry, and References."
            ),
            "source_description": (
                "The document covers a single Apollo lunar rock sample. The title gives "
                "the sample number (4-5 digits) and rock name. The Introduction section "
                "provides general characteristics and crystallization/exposure ages. "
                "The Petrography section describes texture and grain size. "
                "The Mineralogy section lists mineral phases and may include a modal "
                "abundance table with values from multiple authors. "
                "The Chemistry section reports major element oxides and trace elements."
            ),
            # Mineralogy and Rock_Type go to Mindat normalization
            "mineralogy_fields": ["Mineralogy"],
            "rock_fields": ["Rock_Type"],
            # GeoSciML matching: Age_Crystallization (ICS chart) is the most applicable
            # vocabulary for lunar samples; Petrology for texture terms if any match.
            # Terrestrial deposit/tectonic vocab does not apply to lunar rocks.
            "geosciml_fields": ["Age_Crystallization", "Petrology"],
        }
