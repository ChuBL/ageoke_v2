"""
GeoSciML utility subpackage.

Provides:
  - extract_ttl_members: Parse GeoSciML / ICS TTL vocabulary files
  - download_geosciml_vocabularies: Download 40+ GeoSciML TTL files
  - generate_vocab_descriptions: Generate LLM descriptions for all TTL files
"""
from .vocab_parser import extract_ttl_members, generate_vocab_descriptions
from .vocab_updater import download_geosciml_vocabularies

__all__ = [
    "extract_ttl_members",
    "generate_vocab_descriptions",
    "download_geosciml_vocabularies",
]
