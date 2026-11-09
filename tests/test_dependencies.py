"""Tests for async_titiler.dependencies"""

from fastapi import Depends, FastAPI
from starlette.testclient import TestClient

from async_titiler.dependencies import GeoZarrDatasetParams, GeoZarrLayerParams
from titiler.core.resources.responses import JSONResponse


def test_geo_zarr_dataset_params():
    """Test GeoZarrDatasetParams handling."""
    app = FastAPI()

    @app.get("/", response_class=JSONResponse)
    def dataset(params=Depends(GeoZarrDatasetParams)):
        """return params."""
        return params

    @app.get("/nan", response_class=JSONResponse)
    def is_nan(params=Depends(GeoZarrDatasetParams)):
        """return params."""
        return str(params.nodata)

    client = TestClient(app)

    response = client.get("/")
    assert response.status_code == 200
    resp = response.json()
    assert resp["nodata"] is None
    assert resp["resampling_method"] is None
    assert resp["reproject_method"] is None

    response = client.get("/", params={"nodata": 0})
    assert response.status_code == 200
    resp = response.json()
    assert resp["nodata"] == 0

    response = client.get("/nan", params={"nodata": "nan"})
    assert response.status_code == 200
    resp = response.json()
    assert resp == "nan"

    response = client.get("/nan", params={"nodata": "-inf"})
    assert response.status_code == 200
    resp = response.json()
    assert resp == "-inf"


def test_geo_zarr_layer_params():
    """Test GeoZarrLayerParams handling."""
    app = FastAPI()

    @app.get("/", response_class=JSONResponse)
    def dataset(params=Depends(GeoZarrLayerParams)):
        """return params."""
        return params

    @app.get("/methods", response_class=JSONResponse)
    def methods(params=Depends(GeoZarrLayerParams)):
        """return params."""
        return [(r[0], r[1].__name__) for r in params.reduce]

    client = TestClient(app)

    response = client.get("/", params={"variables": "var1", "sel": "time=2023-01-01"})
    assert response.status_code == 200
    resp = response.json()
    assert resp["sel"] == [
        {"dimension": "time", "values": "2023-01-01", "method": None}
    ]

    response = client.get(
        "/", params={"variables": "var1", "sel": "time=nearest::2023-01-01"}
    )
    assert response.status_code == 200
    resp = response.json()
    assert resp["sel"] == [
        {"dimension": "time", "values": "2023-01-01", "method": "nearest"}
    ]

    response = client.get(
        "/",
        params=(
            ("variables", "var1"),
            ("sel", "time=2023-01-01"),
            ("sel", "time=2023-01-02"),
        ),
    )
    assert response.status_code == 200
    resp = response.json()
    assert resp["sel"] == [
        {"dimension": "time", "values": ["2023-01-01", "2023-01-02"], "method": None}
    ]

    response = client.get(
        "/methods", params={"variables": "var1", "reduce": "time::mean"}
    )
    assert response.status_code == 200
    resp = response.json()
    assert resp == [["time", "mean"]]
