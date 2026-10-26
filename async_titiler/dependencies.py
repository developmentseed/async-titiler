"""async-titiler IO dependencies."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated

from fastapi import Query

from titiler.core.dependencies import DefaultDependency, ExpressionParams

from .io import Dataset, _get_geotiff, _get_geozarr


async def GeoTIFFPathParams(
    url: Annotated[str, Query(description="GeoTIFF file URL")],
) -> Dataset:
    """Create dataset path from args"""
    geotiff = await _get_geotiff(url)
    return Dataset(url=url, dataset=geotiff)


async def GeoZARRPathParams(
    url: Annotated[str, Query(description="GeoZarr store URL")],
) -> Dataset:
    """Create dataset path from args"""
    geozarr = await _get_geozarr(url)
    return Dataset(url=url, dataset=geozarr)


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
