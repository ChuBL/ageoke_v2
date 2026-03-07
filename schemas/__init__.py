"""
Schemas package — Pydantic extraction models.

Built-in schema (always available):
  - generic_geology:    Minimal fallback for any geological document format

Domain-specific schemas (auto-discovered from schemas/custom/):
  - usgs_deposit_model: USGS Bulletin 1693 deposit model format (20 fields)
  - lunar_basalt:       Apollo lunar basalt sample catalog format (13 fields)

Adding a new schema:
  - Create schemas/custom/<name>.py with class SchemaModel(BaseExtractionSchema)
  - No other files change; the registry discovers it automatically
"""
