"""async-titiler STAC Mosaic Backend."""

import sys
from collections.abc import Sequence
from typing import Any, TypedDict, cast

import attr
import pystac
import rustac
from cache import AsyncTTL
from geojson_pydantic import Point, Polygon
from geojson_pydantic.geometries import Geometry
from morecantile import Tile, TileMatrixSet
from rasterio.crs import CRS
from rasterio.warp import transform, transform_bounds
from rio_tiler.constants import STAC_ALTERNATE_KEY, WEB_MERCATOR_TMS, WGS84_CRS
from rio_tiler.errors import InvalidAssetName, MissingAssets
from rio_tiler.io import AsyncBaseReader, AsyncMultiBaseReader
from rio_tiler.mosaic.backend import AsyncBaseBackend, MosaicInfo
from rio_tiler.types import AssetInfo, AssetType, BBox
from rio_tiler.utils import CRS_to_uri

from .dependencies import APIParams, Search
from .readers import TIFFReader, ZARRReader
from .settings import ItemsSettings

if sys.version_info >= (3, 15):
    from typing import TypedDict
else:
    from typing_extensions import TypedDict


items_config = ItemsSettings()


class Item(TypedDict, extra_items=True):  # type: ignore[call-arg]
    """Simple STAC Item model."""

    id: str
    collection: str
    bbox: tuple[float, float, float, float]
    properties: dict | None
    assets: dict[str, dict[str, Any]]


@attr.s
class AsyncSimpleSTACReader(AsyncMultiBaseReader):
    """Simplified STAC Reader."""

    input: Item = attr.ib()

    tms: TileMatrixSet = attr.ib(default=WEB_MERCATOR_TMS)
    minzoom: int = attr.ib(default=None)
    maxzoom: int = attr.ib(default=None)

    assets: Sequence[str] = attr.ib(init=False)
    default_assets: Sequence[str] | None = attr.ib(default=None)

    reader: type[AsyncBaseReader] = attr.ib(default=TIFFReader)
    reader_options: dict = attr.ib(factory=dict)

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

    def __attrs_post_init__(self) -> None:
        """Set reader spatial infos and list of valid assets."""
        self.bounds = self.input["bbox"]
        self.crs = WGS84_CRS  # Per specification STAC items are in WGS84

        self.minzoom = self.minzoom if self.minzoom is not None else self._minzoom
        self.maxzoom = self.maxzoom if self.maxzoom is not None else self._maxzoom

        self.assets = list(self.input["assets"])
        if not self.assets:
            raise MissingAssets(
                "No valid asset found. Asset's media types not supported"
            )

    def _get_asset_info(self, asset: AssetType) -> AssetInfo:  # noqa: C901
        """Validate asset names and return asset's url.

        Args:
            asset (str): STAC asset name.

        Returns:
            str: STAC asset href.

        """
        if isinstance(asset, str):
            asset = {"name": asset}

        if not asset.get("name"):
            raise ValueError("asset dictionary does not have `name` key")

        asset_name = asset["name"]
        if asset_name not in self.assets:
            raise InvalidAssetName(
                f"'{asset_name}' is not valid, should be one of {self.assets}"
            )

        asset_info = self.input["assets"][asset_name]

        method_options: dict[str, Any] = {}
        reader_options: dict[str, Any] = {}
        if isinstance(asset, dict):
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
                stac_bands = asset_info.get("bands") or asset_info.get("eo:bands")
                if not stac_bands:
                    raise ValueError(
                        "Asset does not have 'bands' metadata, unable to use 'bands' option"
                    )

                # For Zarr bands = variable
                media_type = (
                    asset_info.get("type").split(";")[0].strip()  # type: ignore
                    if asset_info.get("type")
                    else ""
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

        info = AssetInfo(
            url=asset_info["href"],
            name=asset_name,
            media_type=asset_info.get("type"),
            reader_options=reader_options,
            method_options=method_options,
        )

        if STAC_ALTERNATE_KEY and "alternate" in asset_info:
            if alternate := asset_info["alternate"].get(STAC_ALTERNATE_KEY):
                info["url"] = alternate["href"]

        if (
            bands := asset_info.get("raster:bands")
        ) and "expression" not in method_options:
            stats = [
                (b["statistics"]["minimum"], b["statistics"]["maximum"])
                for b in bands
                if {"minimum", "maximum"}.issubset(b.get("statistics", {}))
            ]
            if len(stats) == len(bands):
                info["dataset_statistics"] = stats

        return info


@attr.s
class AsyncSTACAPIBackend(AsyncBaseBackend):
    """STACAPI Mosaic Backend."""

    # STAC API URL
    input: Search = attr.ib()
    api_params: APIParams = attr.ib()

    # Because we are not using mosaicjson we are not limited to the WebMercator TMS
    tms: TileMatrixSet = attr.ib(default=WEB_MERCATOR_TMS)

    # Use Custom STAC reader (outside init)
    reader: type[AsyncSimpleSTACReader] = attr.ib(default=AsyncSimpleSTACReader)
    reader_options: dict = attr.ib(factory=dict)

    # default values for bounds
    bounds: BBox = attr.ib(default=(-180, -90, 180, 90))
    crs: CRS = attr.ib(default=WGS84_CRS)

    client: rustac.ApiClient = attr.ib(init=False)

    _backend_name = "STACAPI"

    def __attrs_post_init__(self):
        """Post Init."""
        self.client = rustac.ApiClient(
            f"{self.api_params['url']}",
            headers=self.api_params.get("headers"),
        )

        if bbox := self.input.get("bbox"):
            self.bounds = tuple(bbox)

    @property
    def minzoom(self) -> int:
        """Return minzoom."""
        return self.tms.minzoom

    @property
    def maxzoom(self) -> int:
        """Return maxzoom."""
        return self.tms.maxzoom

    # in STACAPI backend assets are STAC Items as dict
    def asset_name(self, asset: dict) -> str:
        """Get asset name."""
        return f"{asset['collection']}/{asset['id']}"

    async def assets_for_tile(
        self, x: int, y: int, z: int, **kwargs: Any
    ) -> list[Item]:
        """Retrieve assets for tile."""
        bbox = self.tms.bounds(Tile(x, y, z))
        return await self.get_assets(Polygon.from_bounds(*bbox), **kwargs)

    async def assets_for_point(
        self,
        lng: float,
        lat: float,
        coord_crs: CRS = WGS84_CRS,
        **kwargs: Any,
    ) -> list[Item]:
        """Retrieve assets for point."""
        if coord_crs != WGS84_CRS:
            xs, ys = transform(coord_crs, WGS84_CRS, [lng], [lat])
            lng, lat = xs[0], ys[0]

        return await self.get_assets(
            Point(type="Point", coordinates=(lng, lat)), **kwargs
        )

    async def assets_for_bbox(
        self,
        xmin: float,
        ymin: float,
        xmax: float,
        ymax: float,
        coord_crs: CRS = WGS84_CRS,
        **kwargs: Any,
    ) -> list[Item]:
        """Retrieve assets for bbox."""
        if coord_crs != WGS84_CRS:
            xmin, ymin, xmax, ymax = transform_bounds(
                coord_crs,
                WGS84_CRS,
                xmin,
                ymin,
                xmax,
                ymax,
            )

        return await self.get_assets(
            Polygon.from_bounds(xmin, ymin, xmax, ymax), **kwargs
        )

    async def get_assets(
        self,
        geom: Geometry,
        sortby: list[dict] | None = None,
        limit: int | None = None,
        max_items: int | None = None,
        fields: list[str] | None = None,
    ) -> list[Item]:
        """Find assets."""

        search_query = {
            **self.input,
            "sortby": sortby,
            "limit": limit or items_config.items_per_page,
            "max_items": max_items or items_config.max_items,
        }
        fields = fields or ["assets", "id", "bbox", "collection"]

        params = {
            **search_query,
            "intersects": geom.model_dump(exclude_none=True, mode="json"),
            "include": fields,
        }
        params.pop("bbox", None)
        results = await self.client.search(**params)
        return [cast(Item, itm) for itm in results]

    @AsyncTTL(time_to_live=300, skip_args=1)
    async def _get_collection(self, collection_id) -> pystac.Collection:
        collection = await self.client.get_collection(collection_id)
        return pystac.Collection.from_dict(collection)

    async def info(self) -> MosaicInfo:  # type: ignore
        """Mosaic info."""
        renders: dict[str, Any] = {}

        if collections := self.input.get("collections", []):
            if len(collections) == 1:
                collection = await self._get_collection(collections[0])
                if not self.input.get("bbox") and collection.extent.spatial:
                    self.bounds = tuple(collection.extent.spatial.bboxes[0])
                    self.crs = WGS84_CRS

                renders = collection.extra_fields.get("renders", {})

        return MosaicInfo(
            bounds=self.bounds,
            crs=CRS_to_uri(self.crs) or self.crs.to_wkt(),
            renders=renders,  # type: ignore
        )
