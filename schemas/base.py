# schemas/base.py
"""
Abstract base class for all extraction schemas.

Every schema must:
  - Inherit BaseExtractionSchema (which inherits Pydantic BaseModel)
  - Be named SchemaModel in its module (enforced by schema_registry.py)
  - Implement schema_metadata() classmethod

The ai_modification_log field is mandatory across all schemas — it records
OCR corrections the LLM made, which is critical for audit traceability.
"""
from __future__ import annotations

import abc
from typing import ClassVar

from pydantic import BaseModel


class BaseExtractionSchema(BaseModel, abc.ABC):
    """
    Base class for all extraction schemas.

    To add support for a new PDF source:
      1. Create schemas/custom/my_source.py
      2. Define: class SchemaModel(BaseExtractionSchema)
      3. Add fields and implement schema_metadata()
      4. Run: python main.py extract doc.pdf --schema my_source
         (No other files change)
    """

    # Mandatory in all schemas — records LLM OCR corrections for auditability
    ai_modification_log: list[str] = []

    @classmethod
    @abc.abstractmethod
    def schema_metadata(cls) -> dict:
        """
        Return metadata dict used by pipeline tools to:
          - Build the extraction system prompt
          - Determine which fields feed into Mindat normalization
          - Determine which fields feed into GeoSciML vocabulary matching

        Required keys:
            name (str):
                Human-readable schema name.
            description (str):
                Brief description of what kind of document this schema targets.
            source_description (str):
                Injected into the extraction prompt — describes the document
                structure to the LLM (section layout, label patterns, etc.).
            mineralogy_fields (list[str]):
                Field names in this schema that contain mineralogy content.
                These are sent to the Mindat normalization tool.
            rock_fields (list[str]):
                Field names that contain rock type content.
                These are also sent to the Mindat normalization tool.
            geosciml_fields (list[str]):
                Field names whose values should be matched against
                GeoSciML controlled vocabulary terms.

        Example:
            {
                "name": "USGS Deposit Model",
                "description": "USGS Bulletin 1693 deposit model series.",
                "source_description": "Each entry starts with Model Index...",
                "mineralogy_fields": ["Mineralogy"],
                "rock_fields": ["Rock_Types"],
                "geosciml_fields": ["Textures", "Age_Range", ...],
            }
        """
        ...

    @classmethod
    def get_extraction_prompt(cls) -> str:
        """
        Build the LLM system prompt for structured extraction.

        Derives the prompt from schema_metadata() and the model's field list.
        Schemas can override this method to provide a fully custom prompt.
        """
        meta = cls.schema_metadata()
        field_names = [
            name for name in cls.model_fields
            if name != "ai_modification_log"
        ]
        fields_block = "\n".join(f'  - "{f}"' for f in field_names)

        return (
            f"You are a geological data extraction assistant.\n\n"
            f"Document type: {meta['description']}\n"
            f"Document structure: {meta['source_description']}\n\n"
            f"Extract ALL of the following fields from the provided text and return a "
            f"valid JSON object that matches the schema exactly.\n\n"
            f"Rules:\n"
            f"  - Include every field listed below, even if empty (use \"\" for missing text fields)\n"
            f"  - Do not add any fields not listed in the schema\n"
            f"  - Correct obvious OCR errors conservatively (e.g. 'HODEL' → 'MODEL')\n"
            f"  - Record every correction you make in ai_modification_log as a list of strings\n"
            f"  - Return pure JSON with no markdown code fences\n\n"
            f"Required fields:\n{fields_block}\n"
            f"  - \"ai_modification_log\""
        )
