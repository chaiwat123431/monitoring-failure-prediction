"""Loads the trained model artifact once, at startup (PLANNING.md AD-21/AD-24).

A missing file is a normal, handled state — scripts/train.py may simply not have been run yet —
not a startup crash. Any other failure while reading a file that does exist (a truncated/corrupt
artifact, an incompatible joblib pickle) is a real problem and is left to propagate rather than
being folded into the same "no model" state.
"""

from pathlib import Path

import joblib


def load_model(path: Path) -> tuple[object | None, dict | None]:
    """Returns (model, metadata) from the joblib artifact, or (None, None) if `path` doesn't
    exist yet."""
    try:
        artifact = joblib.load(path)
    except FileNotFoundError:
        return None, None
    return artifact["model"], artifact["metadata"]
