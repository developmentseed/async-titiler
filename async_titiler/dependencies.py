"""async-titiler IO dependencies."""

import posixpath
from dataclasses import dataclass
from typing import Annotated, Any, cast
from urllib.parse import urlparse

import httpx2 as httpx
import pystac
import zarr
from async_geotiff import GeoTIFF
from cache import AsyncTTL
from fastapi import Query
from pydantic import AfterValidator
from rio_tiler.types import AssetType, AssetWithOptions
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
    if not url.endswith("/"):
        url += "/"
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


@AsyncTTL(time_to_live=300)
async def fetch(url: str) -> dict[str, Any]:
    """Fetch STAC items."""
    parsed = urlparse(url)
    if parsed.scheme in ["https", "http", "ftp"]:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url)
            resp.raise_for_status()
            return resp.json()

    raise ValueError(f"Invalid STAC URL: {url}. Must be a valid HTTP/HTTPS/FTP URL.")


async def STACPathParams(
    url: Annotated[str, Query(description="Stac Item URL")],
) -> pystac.Item:
    """Create dataset path from args"""
    item = await fetch(url)
    return pystac.Item.from_dict(item)


VALID_ASSET_OPTIONS = {"bidx", "expression", "bands", "variables", "sel"}


def _parse_option(key: str, value: str) -> tuple[str, Any]:
    """Parse a single asset option key=value pair into (opts_key, opts_value)."""
    if key == "bidx":
        try:
            return ("indexes", list(map(int, value.split(","))))
        except ValueError:
            raise ValueError(
                f"Invalid bidx value '{value}'. "
                f"Expected comma-separated integers, e.g. 'bidx=1' or 'bidx=1,2,3'"
            ) from None

    if key == "expression":
        return ("expression", value)

    if key == "bands":
        return ("bands", value.split(","))

    # custom part for Stac/GeoZarrReader
    if key == "variables":
        return ("variables", value.split(","))

    if key == "sel":
        return ("sel", value.split(","))

    raise ValueError(
        f"Unknown asset option '{key}'. "
        f"Valid options: {', '.join(sorted(VALID_ASSET_OPTIONS))}"
    )


def _parse_asset(values: list[str]) -> list[AssetType]:
    """Parse assets with optional parameter.

    Format: ``asset_name`` or ``asset_name|key=value|key=value``

    Supported options:
        - ``bidx=1,2`` — band indexes
        - ``expression=...`` — band math expression
        - ``bands=red,green`` — band names
        - ``variables=vv,vh`` — variable names (for GeoZarr)
        - ``sel=time=2022-02-01`` — dimension selection (for GeoZarr)

    Raises:
        ValueError: If an option is missing a ``key=value`` pair or uses an unknown key.
    """
    assets: list[AssetType] = []
    for v in values:
        # asset with options
        if "|" in v:
            asset_name, params = v.split("|", 1)
            opts: dict[str, Any] = {"name": asset_name}
            for option in params.split("|"):
                if "=" not in option:
                    raise ValueError(
                        f"Invalid asset option '{option}' in '{v}'. "
                        f"Options must be in 'key=value' format. "
                        f"Valid keys: {', '.join(sorted(VALID_ASSET_OPTIONS))}. "
                        f"Example: '{asset_name}|bidx=1' or '{asset_name}|variables=vv,vh'"
                    )

                key, value = option.split("=", 1)
                try:
                    opts_key, opts_value = _parse_option(key, value)
                except ValueError as e:
                    raise ValueError(f"Error parsing asset '{v}': {e}") from e

                opts[opts_key] = opts_value

            asset = cast(AssetWithOptions, opts)
            assets.append(asset)

        # asset without options
        else:
            assets.append({"name": v})

    return assets


@dataclass
class AssetsParams(DefaultDependency):
    """Assets parameters."""

    assets: Annotated[
        list[str],
        AfterValidator(_parse_asset),
        Query(
            title="Asset names",
            description="Asset's names.",
            openapi_examples={
                "user-provided": {"value": None},
                "one-asset": {
                    "description": "Return results for asset `data`.",
                    "value": ["data"],
                },
                "multi-assets": {
                    "description": "Return results for assets `data` and `cog`.",
                    "value": ["data", "cog"],
                },
                "multi-assets-with-options": {
                    "description": "Return results for assets `data` and `cog`.",
                    "value": ["data|bidx=1", "cog|bidx=1,2"],
                },
            },
        ),
    ]


@dataclass
class AssetsExprParams(ExpressionParams, AssetsParams):
    """Assets and Expression parameters."""

    asset_as_band: Annotated[
        bool | None,
        Query(
            title="Consider asset as a 1 band dataset",
            description="Asset as Band",
        ),
    ] = None
