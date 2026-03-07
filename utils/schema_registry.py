# utils/schema_registry.py
"""
Dynamic schema registry for extraction schemas.

Adding a new PDF source type requires ONLY:
  1. Create schemas/custom/my_source.py with a class named SchemaModel
     that inherits BaseExtractionSchema and implements schema_metadata()
  2. Pass schema_name="my_source" to any pipeline tool

No other files change. The registry auto-discovers schemas/custom/*.py via importlib.
"""
import importlib
from pathlib import Path

# Built-in schemas: name → module path
# Only the generic fallback lives here as a built-in. All domain-specific
# schemas (usgs_deposit_model, lunar_basalt, and any you add) live in
# schemas/custom/ and are auto-discovered — no registry edits required.
_BUILTIN_SCHEMAS: dict[str, str] = {
    "generic_geology": "schemas.generic_geology",
}


def get_schema_class(schema_name: str):
    """
    Load and return the Pydantic SchemaModel class for the given schema name.

    Lookup order:
      1. Built-in schemas (_BUILTIN_SCHEMAS dict)
      2. Auto-discovered custom schemas in schemas/custom/

    Returns:
        A class that is both a Pydantic BaseModel and a BaseExtractionSchema subclass.

    Raises:
        ValueError: if schema_name is not found anywhere in the registry.
    """
    module_path = _BUILTIN_SCHEMAS.get(schema_name)

    if module_path is None:
        # Try auto-discovery in schemas/custom/
        custom_module_path = f"schemas.custom.{schema_name}"
        try:
            module = importlib.import_module(custom_module_path)
        except ModuleNotFoundError:
            available = list_available_schemas()
            raise ValueError(
                f"Schema '{schema_name}' not found. "
                f"Available schemas: {available}"
            )
    else:
        module = importlib.import_module(module_path)

    schema_class = getattr(module, "SchemaModel", None)
    if schema_class is None:
        raise AttributeError(
            f"Schema module '{module_path}' must define a class named 'SchemaModel'."
        )

    return schema_class


def get_schema_metadata(schema_name: str) -> dict:
    """
    Return the metadata dict from the named schema.
    Used by tools to discover field targets without instantiating a model.
    """
    return get_schema_class(schema_name).schema_metadata()


def list_available_schemas() -> list[str]:
    """Return all registered schema names (built-ins + auto-discovered custom)."""
    return list(_BUILTIN_SCHEMAS.keys()) + _discover_custom_schemas()


def _discover_custom_schemas() -> list[str]:
    """Scan schemas/custom/ for .py files and return their stem names."""
    custom_dir = Path("schemas") / "custom"
    if not custom_dir.exists():
        return []
    return [
        p.stem
        for p in sorted(custom_dir.glob("*.py"))
        if p.stem != "__init__" and not p.stem.startswith("_")
    ]
