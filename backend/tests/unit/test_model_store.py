from pathlib import Path

from app.ml.model_store import load_model


def test_missing_file_returns_none_none_not_an_exception(tmp_path: Path):
    """AD-24: a missing model artifact is a handled state, not a startup crash."""
    model, metadata = load_model(tmp_path / "does-not-exist.joblib")

    assert model is None
    assert metadata is None


def test_existing_file_returns_the_model_and_metadata(tmp_path: Path):
    import joblib

    path = tmp_path / "isolation_forest.joblib"
    joblib.dump({"model": "fake-model", "metadata": {"trained_at": "2024-01-01"}}, path)

    model, metadata = load_model(path)

    assert model == "fake-model"
    assert metadata == {"trained_at": "2024-01-01"}
