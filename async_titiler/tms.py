"""TileMatrixSet tools."""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any

from async_geotiff import GeoTIFF
from morecantile.models import CRS_to_uri, TileMatrixSet, TMSBoundingBox
from morecantile.utils import meters_per_unit
from pyproj import CRS as pyprojCRS
from pyproj.exceptions import CRSError

if TYPE_CHECKING:
    import zarr
    from async_geotiff import GeoTIFF


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


# TODO: add cache
def tms_from_dataset(dataset: GeoTIFF | zarr.AsyncGroup) -> TileMatrixSet:
    """Return a TileMatrixSet from a Dataset."""
    if isinstance(dataset, GeoTIFF):
        if dataset.crs is None:
            raise ValueError("GeoTIFF has no CRS")

        if dataset.bounds is None:
            raise ValueError("GeoTIFF has no bounds")

        if dataset.transform is None:
            raise ValueError("GeoTIFF has no transform")

        crs_data = _pyproj_crs_to_tms_crs(dataset.crs)
        mpu = meters_per_unit(dataset.crs)
        screen_pixel_size = 0.28e-3

        matrices = []
        for level, ovr in enumerate(list(reversed(dataset.overviews)) + [dataset]):
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
            id="Native",
            orderedAxes=["X", "Y"],
            crs=crs_data,
            boundingBox=TMSBoundingBox(
                lowerLeft=(dataset.bounds[0], dataset.bounds[1]),
                upperRight=(dataset.bounds[2], dataset.bounds[3]),
            ),
            tileMatrices=matrices,
        )

    elif isinstance(dataset, zarr.AsyncGroup):
        pass

    raise ValueError(f"Unsupported dataset type: {type(dataset)}")
