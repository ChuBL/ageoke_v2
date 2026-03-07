# Adding a Custom Extraction Schema

To add support for a new PDF source type, create a single Python file here.
No other files need to change.

## Steps

1. Create `schemas/custom/my_source_name.py`
2. Define a class named `SchemaModel` that inherits `BaseExtractionSchema`
3. Add Pydantic fields for every piece of data you want to extract
4. Implement `schema_metadata()` — tell the pipeline which fields to enrich

## Template

```python
# schemas/custom/my_source_name.py
from pydantic import Field
from schemas.base import BaseExtractionSchema


class SchemaModel(BaseExtractionSchema):
    """Schema for <describe your source here>."""

    # Add your fields here
    field_one: str = Field(default="", description="...")
    field_two: str = Field(default="", description="...")
    # ai_modification_log is inherited automatically

    @classmethod
    def schema_metadata(cls) -> dict:
        return {
            "name": "My Source Name",
            "description": "Brief description of what documents this targets.",
            "source_description": (
                "Describe the layout of the source document to the LLM here. "
                "What sections does it have? What labels are used?"
            ),
            # Fields to send to Mindat mineral normalization (can be empty list)
            "mineralogy_fields": ["field_one"],
            # Fields to send to Mindat rock normalization (can be empty list)
            "rock_fields": [],
            # Fields to match against GeoSciML controlled vocabulary (can be empty list)
            "geosciml_fields": ["field_two"],
        }
```

## Usage

```bash
python main.py extract path/to/document.pdf --schema my_source_name
python main.py list-schemas   # verify it appears
```

## Notes

- The file name (without `.py`) is the schema identifier used with `--schema`
- The class **must** be named `SchemaModel`
- All fields must have `default=""` so missing content doesn't cause validation errors
- `ai_modification_log` is always included automatically — don't redefine it
