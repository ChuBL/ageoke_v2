# utils/file_io.py
"""
Centralized file I/O helpers. All file reads and writes go through here.

Key feature: save_json() uses atomic temp-file + rename to prevent
output corruption if the process crashes mid-write.
"""
import json
import os
import tempfile
from pathlib import Path
from typing import Any


def save_json(data: Any, path: Path, indent: int = 2) -> Path:
    """
    Atomically write data as JSON to path.

    Uses a temp file in the same directory followed by os.replace() so
    the write is atomic on POSIX systems — a crash mid-write leaves the
    previous file intact rather than producing a truncated output.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    tmp_fd, tmp_path = tempfile.mkstemp(dir=path.parent, prefix=".tmp_", suffix=".json")
    try:
        with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=indent, ensure_ascii=False)
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise

    return path


def load_json(path: Path) -> Any:
    """Load and return parsed JSON from path."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"JSON file not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_text(text: str, path: Path) -> Path:
    """Write UTF-8 text to path, creating parent dirs as needed."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    return path


def load_text(path: Path) -> str:
    """Read and return UTF-8 text from path."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Text file not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def ensure_dir(path: Path) -> Path:
    """Create directory and all parents if they don't exist. Returns path."""
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path
