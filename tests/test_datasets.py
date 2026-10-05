import asyncio
import csv

from core import CatalogSource
from datasets import DatasetEngine, DatasetRequest


def test_count_dataset_reuses_local_qualified_field_and_writes_manifest(tmp_path):
    engine = DatasetEngine(storage_path=tmp_path)
    engine.metadata.upsert_sources([
        CatalogSource(
            catalog="gaia_dr3",
            source_id="source-1",
            ra=10.0,
            dec=20.0,
            positional_error_arcsec=0.1,
            data={"parallax": 2.0},
            metadata={"wavelength": "optical", "physical": {"parallax": 2.0}},
            provenance={"provider": "tap", "retrieved_at": "2026-01-01T00:00:00Z"},
            epoch=2016.0,
        )
    ])
    request = DatasetRequest.model_validate({
        "name": "local-reuse",
        "profile": "stellar",
        "count": 1,
        "catalogs": ["gaia_dr3"],
        "fields": ["gaia_dr3.parallax"],
        "filters": {"min_gaia_dr3.parallax": 1},
        "output_format": "csv",
    })

    result = asyncio.run(engine.fulfill(request, dataset_id="offlinebuild"))

    assert result["status"] == "completed"
    assert result["total_sources"] == 1
    assert result["reused_local_records"] == 1
    assert result["fetched_rows"] == 0
    assert engine.get_dataset("offlinebuild")["manifest_path"] == result["manifest_path"]
    with open(result["export_path"], newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["gaia_dr3.parallax"] == "2.0"
