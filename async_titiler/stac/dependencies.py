"""async-titiler STAC dependencies."""

import json
from dataclasses import dataclass, field
from typing import Annotated, Any, Literal, NotRequired, TypedDict, cast

import httpx2 as httpx
import pystac
from cache import AsyncTTL
from fastapi import Path, Query
from pydantic import AfterValidator
from rio_tiler.types import AssetType, AssetWithOptions
from starlette.requests import Request

from titiler.core.dependencies import DefaultDependency, ExpressionParams

from .settings import ItemsSettings

items_config = ItemsSettings()


class APIParams(TypedDict):
    """STAC API Parameters."""

    url: str
    headers: NotRequired[dict]


class Search(TypedDict, total=False):
    """STAC Search Parameters."""

    collections: list[str] | None
    ids: list[str] | None
    bbox: list[float] | None
    datetime: str | None
    filter: str | dict | None
    filter_lang: Literal["cql2-text", "cql2-json"]


@dataclass(init=False)
class BackendParams(DefaultDependency):
    """backend parameters."""

    api_params: APIParams = field(init=False)

    def __init__(self, request: Request):
        """Initialize BackendParams

        Note: Because we don't want `api_params` to appear in the documentation we use a dataclass with a custom `__init__` method.
        FastAPI will use the `__init__` method but will exclude Request in the documentation making `api_params` an invisible dependency.
        """
        self.api_params = APIParams(
            url=request.app.state.stac_url,
            # possibly add headers
        )


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


@dataclass
class STACAPIExtensionParams(DefaultDependency):
    """STACAPI advanced search parameters: forwared to Backend.get_assets method."""

    sortby: Annotated[
        str | None,
        Query(
            description="An array of property names, prefixed by either '+' for ascending or '-' for descending. If no prefix is provided, '+' is assumed.",
            openapi_examples={
                "user-provided": {"value": None},
                "resolution": {"value": "-gsd"},
                "resolution-and-dates": {"value": "-gsd,-datetime"},
            },
        ),
    ] = None
    limit: Annotated[
        int | None,
        Query(
            description=f"Limit the number of items per page search (default: {items_config.items_per_page})"
        ),
    ] = None
    max_items: Annotated[
        int | None,
        Query(
            description=f"Limit the number of total items (default: {items_config.max_items})"
        ),
    ] = None

    def __post_init__(self):
        """Post Init."""
        if self.sortby:
            self.sortby = self.sortby.split(",")  # type: ignore


@AsyncTTL(time_to_live=300)
async def get_stac_item(
    url: str,
    collection_id: str,
    item_id: str,
    headers: dict | None = None,
) -> dict[str, Any]:
    """Fetch STAC items."""
    url = f"{url}/collections/{collection_id}/items/{item_id}"
    async with httpx.AsyncClient() as client:
        resp = await client.get(url)
        resp.raise_for_status()
        return resp.json()


async def STACItemParams(
    request: Request,
    collection_id: Annotated[
        str,
        Path(description="STAC Collection Identifier"),
    ],
    item_id: Annotated[str, Path(description="STAC Item Identifier")],
) -> pystac.Item:
    """Create dataset path from args"""
    # NOTE: here we can customize the forwarded headers to the STAC API,
    # for example to add authentication headers if needed.
    headers: dict[str, Any] = {}
    item = await get_stac_item(
        request.app.state.stac_url,
        collection_id,
        item_id,
        headers=headers,
    )
    return pystac.Item.from_dict(item)


def STACCollectionParams(
    collection_id: Annotated[
        str,
        Path(description="STAC Collection Identifier."),
    ],
    ids: Annotated[
        str | None,
        Query(
            description="Array of Item ids",
            openapi_examples={
                "user-provided": {"value": None},
                "multiple-items": {"value": "item1,item2"},
            },
        ),
    ] = None,
    bbox: Annotated[
        str | None,
        Query(
            description="Filters items intersecting this bounding box",
            openapi_examples={
                "user-provided": {"value": None},
                "Montreal": {"value": "-73.896103,45.364690,-73.413734,45.674283"},
            },
        ),
    ] = None,
    datetime: Annotated[
        str | None,
        Query(
            description="""Filters items that have a temporal property that intersects this value.\n
Either a date-time or an interval, open or closed. Date and time expressions adhere to RFC 3339. Open intervals are expressed using double-dots.""",
            openapi_examples={
                "user-defined": {"value": None},
                "datetime": {"value": "2018-02-12T23:20:50Z"},
                "closed-interval": {
                    "value": "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
                },
                "open-interval-from": {"value": "2018-02-12T00:00:00Z/.."},
                "open-interval-to": {"value": "../2018-03-18T12:31:12Z"},
            },
        ),
    ] = None,
    filter_expr: Annotated[
        str | None,
        Query(
            alias="filter",
            description="""A CQL2 filter expression for filtering items.\n
Supports `CQL2-JSON` as defined in https://docs.ogc.org/is/21-065r2/21-065r2.htmln
Remember to URL encode the CQL2-JSON if using GET""",
            openapi_examples={
                "user-provided": {"value": None},
                "landsat8-item": {
                    "value": "id='LC08_L1TP_060247_20180905_20180912_01_T1_L1TP' AND collection='landsat8_l1tp'"  # noqa: E501
                },
            },
        ),
    ] = None,
    filter_lang: Annotated[
        Literal["cql2-text", "cql2-json"],
        Query(
            alias="filter-lang",
            description="CQL2 Language (cql2-text, cql2-json). Defaults to cql2-text.",
        ),
    ] = "cql2-text",
) -> Search:
    """factory's `path_dependency`"""
    if filter_expr and filter_lang == "cql2-json":
        try:
            filter_expr = json.loads(filter_expr)  # type: ignore
        except json.JSONDecodeError as e:
            raise ValueError("filter expression is not valid JSON") from e

    return Search(
        collections=[collection_id],
        ids=ids.split(",") if ids else None,
        bbox=[float(v) for v in bbox.split(",")] if bbox else None,
        datetime=datetime,
        filter=filter_expr,
        filter_lang=filter_lang,
    )
