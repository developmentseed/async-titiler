"""Tests for async_titiler.io"""

import os

import pytest
from async_geotiff import GeoTIFF

from async_titiler.io import DatasetPathParams

PREFIX = os.path.join(os.path.dirname(__file__), "fixtures")

COG_PATH = os.path.join(PREFIX, "cog_uint8_rgb_nodata.tif")
COG_FILE = f"file://{COG_PATH}"
COG_URL_HTTP = "https://raw.githubusercontent.com/developmentseed/geotiff-test-data/refs/heads/main/rasterio_generated/fixtures/cog_uint8_rgb_nodata.tif"


@pytest.mark.parametrize(
    "url",
    [
        COG_PATH,
        COG_FILE,
        COG_URL_HTTP,
    ],
)
@pytest.mark.asyncio
async def test_dataset_path_params_returns_geotiff(url):
    """DatasetPathParams should return a GeoTIFF object."""
    result = await DatasetPathParams(url=url)
    assert isinstance(result, GeoTIFF)
    assert result.bounds is not None
    assert result.crs is not None
    assert result.width > 0
    assert result.height > 0


@pytest.mark.asyncio
async def test_dataset_path_params_invalid_url():
    """DatasetPathParams should raise on a non-existent file."""
    with pytest.raises(Exception):  # noqa: B017
        await DatasetPathParams(url="file:///nonexistent/path/file.tif")
