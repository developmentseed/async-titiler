"""TileMatrixSet tools."""

from __future__ import annotations

import math
from threading import Lock
from typing import TYPE_CHECKING, Any

from async_geotiff import GeoTIFF
from cachetools import TTLCache, cached
from cachetools.keys import hashkey
from morecantile.models import CRS_to_uri, TileMatrixSet, TMSBoundingBox
from morecantile.utils import meters_per_unit
from pyproj import CRS as pyprojCRS
from pyproj.exceptions import CRSError

from .io import Dataset

if TYPE_CHECKING:
    import zarr
    from async_geotiff import GeoTIFF


ttl_cache: TTLCache = TTLCache(maxsize=512, ttl=300)


def _pyproj_crs_to_tms_crs(crs: pyprojCRS) -> str | dict:
    if crs.to_authority(min_confidence=20):
        crs_data: Any = CRS_to_uri(crs)

        # Some old Proj version might not support URI
        # so we fall back to wkt
        try:
            pyprojCRS.from_user_input(crs_data)
        except CRSError:
            crs_data = {"wkt": crs.to_json_dict()}

    else:
        crs_data = {"wkt": crs.to_json_dict()}

    return crs_data


@cached(
    ttl_cache,
    key=lambda dst: hashkey(dst.url),
    lock=Lock(),
)
def tms_from_dataset(dst: Dataset) -> TileMatrixSet:
    """Return a TileMatrixSet from a Dataset."""
    if isinstance(dst.dataset, GeoTIFF):
        if dst.dataset.crs is None:
            raise ValueError("GeoTIFF has no CRS")

        if dst.dataset.bounds is None:
            raise ValueError("GeoTIFF has no bounds")

        if dst.dataset.transform is None:
            raise ValueError("GeoTIFF has no transform")

        crs_data = _pyproj_crs_to_tms_crs(dst.dataset.crs)
        mpu = meters_per_unit(dst.dataset.crs)
        screen_pixel_size = 0.28e-3

        matrices = []
        for level, ovr in enumerate(
            list(reversed(dst.dataset.overviews)) + [dst.dataset]
        ):
            matrix = {
                "id": str(level),
                "scaleDenominator": ovr.transform.a * mpu / screen_pixel_size,
                "cornerOfOrigin": "topLeft",
                "pointOfOrigin": (ovr.bounds[0], ovr.bounds[3]),
                "cellSize": ovr.transform.a,
                "tileWidth": ovr.tile_width,
                "tileHeight": ovr.tile_height,
                "matrixWidth": math.ceil(ovr.width / ovr.tile_width),
                "matrixHeight": math.ceil(ovr.height / ovr.tile_height),
            }
            matrices.append(matrix)

        return TileMatrixSet(
            id="LocalTileMatrixSet",
            orderedAxes=["X", "Y"],
            crs=crs_data,
            boundingBox=TMSBoundingBox(
                lowerLeft=(dst.dataset.bounds[0], dst.dataset.bounds[1]),
                upperRight=(dst.dataset.bounds[2], dst.dataset.bounds[3]),
            ),
            tileMatrices=matrices,
        )

    elif isinstance(dst.dataset, zarr.AsyncGroup):
        pass

    raise ValueError(f"Unsupported dataset type: {type(dst.dataset)}")
