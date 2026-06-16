"""Async-Titiler STAC Reader."""

import posixpath
from contextlib import asynccontextmanager
from typing import Any
from urllib.parse import urlparse

import attr
import pystac
import zarr
from async_geotiff import GeoTIFF
from rio_tiler.experimental import async_stac
from rio_tiler.experimental.geotiff import Reader as AsyncGeoTiFFReader
from rio_tiler.experimental.zarr import GeoZarrReader as AsyncGeoZarrReader
from rio_tiler.io import AsyncBaseReader
from rio_tiler.types import AssetInfo, AssetWithOptions
from zarr.storage import ObjectStore

from ._obstore import _get_store

_VALID_TYPE = {
    "image/tiff; application=geotiff",
    "image/tiff; application=geotiff; profile=cloud-optimized",
    "image/tiff; profile=cloud-optimized; application=geotiff",
    pystac.MediaType.COG,
    "image/vnd.stac.geotiff; cloud-optimized=true",
    "image/tiff",
    "image/x.geotiff",
    "application/x-zarr",
    "application/vnd.zarr",
    "application/vnd+zarr",
    "application/vnd.zarr; version=3",
    "application/vnd.zarr; version=3; profile=multiscales",
}


# NOTE: We wrapp the readers in asynccontextmanager to enable the use of `url` as input parameter
# because both AsyncGeoTiFFReader and AsyncGeoZarrReader require a GeoTIFF or Zarr objects.
# The `url` is used to create a store and open the dataset, which is then passed to the reader.
#
# Within the STACReader, the method will then call:
# ```
# with self.reader(url) as src:
#     ...
# ```
# in this block, the src will be the AsyncGeoTiFFReader or AsyncGeoZarrReader object, which can be used to read the data.
#
@asynccontextmanager
async def TIFFReader(url: str, **kwargs: Any) -> AsyncGeoTiFFReader:  # type: ignore
    """Async context manager for STACReader."""
    store = await _get_store(url)
    parsed = urlparse(url)
    geotiff_ds = await GeoTIFF.open(posixpath.basename(parsed.path), store=store)
    async with AsyncGeoTiFFReader(input=geotiff_ds, **kwargs) as src:
        yield src


@asynccontextmanager
async def ZARRReader(url: str, **kwargs: Any) -> AsyncGeoZarrReader:  # type: ignore
    """Async context manager for STACReader."""
    if not url.endswith("/"):
        url += "/"

    store = await _get_store(url)
    zarr_store = ObjectStore(store=store, read_only=True)
    zarr_ds = await zarr.api.asynchronous.open_group(store=zarr_store, mode="r")
    async with AsyncGeoZarrReader(input=zarr_ds, **kwargs) as src:
        yield src


@attr.s
class AsyncSTACReader(async_stac.AsyncSTACReader):
    """Custom  Async STAC Reader with support for Zarr and GeoTIFF."""

    reader: type[AsyncBaseReader] = attr.ib(default=TIFFReader)
    include_asset_types: set[str] = attr.ib(default=_VALID_TYPE)

    def _get_reader(self, asset_info: AssetInfo) -> type[AsyncBaseReader]:
        """Get Asset Reader and options."""
        if media_type := asset_info["media_type"]:
            # For Zarr bands = variable
            if media_type.split(";")[0].strip() in [
                "application/x-zarr",
                "application/vnd.zarr",
                "application/vnd+zarr",
            ]:
                return ZARRReader  # type: ignore

        return TIFFReader  # type: ignore

    def _get_options(
        self,
        asset: AssetWithOptions,
        metadata: pystac.Asset,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        method_options: dict[str, Any] = {}
        reader_options: dict[str, Any] = {}

        # Indexes
        if indexes := asset.get("indexes"):
            method_options["indexes"] = indexes
        # Expression
        if expr := asset.get("expression"):
            method_options["expression"] = expr
        # Variables
        if vars := asset.get("variables"):
            method_options["variables"] = vars
        # Sel (dimension selection)
        if vars := asset.get("sel"):
            method_options["sel"] = vars
        # Bands
        if bands := asset.get("bands"):
            stac_bands = (
                metadata.extra_fields.get("bands")
                or metadata.extra_fields.get("eo:bands")  # V1.0
            )
            if not stac_bands:
                raise ValueError(
                    "Asset does not have 'bands' metadata, unable to use 'bands' option"
                )
            # For Zarr bands = variable
            media_type = (
                metadata.media_type.split(";")[0].strip() if metadata.media_type else ""
            )
            zarr_media_types = [
                "application/x-zarr",
                "application/vnd.zarr",
                "application/vnd+zarr",
            ]
            if media_type in zarr_media_types:
                common_to_variable = {
                    b.get("eo:common_name") or b.get("common_name") or b["name"]: b[
                        "name"
                    ]
                    for b in stac_bands
                }
                method_options["variables"] = [
                    common_to_variable.get(v, v) for v in bands
                ]

            # For COG bands = indexes
            else:
                # There is no standard for precedence between 'eo:common_name' and 'name'
                # in STAC specification, so we will use 'eo:common_name' if it exists,
                # otherwise fallback to 'name', and if not exist use the band index as last resource.
                common_to_variable = {
                    b.get("eo:common_name")
                    or b.get("common_name")
                    or b.get("name")
                    or str(ix): ix
                    for ix, b in enumerate(stac_bands, 1)
                }
                band_indexes: list[int] = []
                for b in bands:
                    if idx := common_to_variable.get(b):
                        band_indexes.append(idx)
                    else:
                        raise ValueError(
                            f"Band '{b}' not found in asset metadata, unable to use 'bands' option"
                        )

                    method_options["indexes"] = band_indexes

        return reader_options, method_options
