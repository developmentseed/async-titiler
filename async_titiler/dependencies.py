"""async-titiler IO dependencies."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Annotated

from fastapi import Query

from titiler.core.dependencies import DefaultDependency, ExpressionParams

from .io import _get_geotiff, _get_geozarr

if TYPE_CHECKING:
    import zarr
    from async_geotiff import GeoTIFF


async def GeoTIFFPathParams(
    url: Annotated[str, Query(description="GeoTIFF file URL")],
) -> GeoTIFF:
    """Create dataset path from args"""
    return await _get_geotiff(url)


async def GeoZARRPathParams(
    url: Annotated[str, Query(description="GeoZarr store URL")],
) -> zarr.AsyncGroup:
    """Create dataset path from args"""
    return await _get_geozarr(url)


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
