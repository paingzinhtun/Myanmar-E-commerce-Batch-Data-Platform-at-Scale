from data.sample_generation.generate_myanmar_ecommerce_data import generate_dataset
from src.utils.config import get_settings


def test_generate_dataset_creates_expected_files(tmp_path, monkeypatch):
    settings = get_settings()
    monkeypatch.setattr(settings, "raw_data_dir", tmp_path / "raw")
    monkeypatch.setattr(settings, "sample_generation_dir", tmp_path / "sample_generation")

    generate_dataset("2026-04-10", days=1, row_scale=50)

    assert (settings.raw_data_dir / "master" / "customers.csv").exists()
    assert (settings.raw_data_dir / "batches" / "batch_date=2026-04-10" / "orders.csv").exists()
