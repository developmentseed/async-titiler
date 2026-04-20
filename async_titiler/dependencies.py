"""async-titiler IO dependencies."""

import posixpath
from dataclasses import dataclass
from typing import Annotated
from urllib.parse import urlparse

import zarr
from async_geotiff import GeoTIFF
from fastapi import Query
from zarr.storage import ObjectStore

from titiler.core.dependencies import DefaultDependency, ExpressionParams

from ._obstore import _get_store


async def GeoTIFFPathParams(
    url: Annotated[str, Query(description="GeoTIFF file URL")],
) -> GeoTIFF:
    """Create dataset path from args"""
    store = await _get_store(url)

    parsed = urlparse(url)
    filename = posixpath.basename(parsed.path)
    return await GeoTIFF.open(filename, store=store)


async def GeoZARRPathParams(
    url: Annotated[str, Query(description="GeoZarr store URL")],
) -> zarr.AsyncGroup:
    """Create dataset path from args"""
    store = await _get_store(url)
    zarr_store = ObjectStore(store=store, read_only=True)
    return await zarr.api.asynchronous.open_group(store=zarr_store, mode="r")


@dataclass
class VariablesParams(DefaultDependency):
    """Zarr Dataset Options."""

    variables: Annotated[
        list[str],
        Query(description="Zarr Array name."),
    ]


@dataclass
class LayerParams(ExpressionParams, VariablesParams):
    """variable + expression."""
