"""async-titiler factory."""

from collections.abc import Callable
from typing import Annotated, Literal
import logging

import jinja2
import rasterio
from attrs import define
from fastapi import Body, Depends, Path, Query
from pydantic import Field
from geojson_pydantic.features import Feature, FeatureCollection
from rio_tiler.experimental._async import AsyncBaseReader
from rio_tiler.experimental._async import Reader as AsyncReader
from rio_tiler.constants import WGS84_CRS
from rio_tiler.utils import CRS_to_uri
from starlette.responses import Response
from starlette.templating import Jinja2Templates
from morecantile import tms as morecantile_tms
from morecantile.defaults import TileMatrixSets

from titiler.core.dependencies import (
    CoordCRSParams,
    DstCRSParams,
    OGCMapsParams,
    DefaultDependency,
)
from titiler.core.resources.enums import ImageType
from titiler.core.models.responses import (
    Point,
    Statistics,
    StatisticsGeoJSON,
)
from titiler.core.resources.responses import GeoJSONResponse, JSONResponse
from titiler.core.factory import TilerFactory, img_endpoint_params

from .io import DatasetPathParams

jinja2_env = jinja2.Environment(
    autoescape=jinja2.select_autoescape(["html"]),
    loader=jinja2.ChoiceLoader([jinja2.PackageLoader(__package__, "templates")]),
)
DEFAULT_TEMPLATES = Jinja2Templates(env=jinja2_env)


from rio_tiler.experimental._async import Reader as AsyncReader
from rio_tiler.experimental._async import AsyncBaseReader

from async_geotiff import GeoTIFF

logger = logging.getLogger(__name__)


@define(kw_only=True)
class AsyncTilerFactory(TilerFactory):
    """Async Tiler Factory."""

    reader: type[AsyncBaseReader] = AsyncReader

    # Path Dependency
    path_dependency: Callable[..., GeoTIFF] = DatasetPathParams

    # Tile/Tilejson/WMTS Dependencies
    tile_dependency: type[DefaultDependency] = DefaultDependency

    ############################################################################
    # /statistics
    ############################################################################
    def statistics(self):
        """add statistics endpoints."""

        # GET endpoint
        @self.router.get(
            "/statistics",
            response_class=JSONResponse,
            response_model=Statistics,
            responses={
                200: {
                    "content": {"application/json": {}},
                    "description": "Return dataset's statistics.",
                }
            },
            operation_id=f"{self.operation_prefix}getStatistics",
        )
        async def statistics(
            src_path=Depends(self.path_dependency),
            reader_params=Depends(self.reader_dependency),
            layer_params=Depends(self.layer_dependency),
            dataset_params=Depends(self.dataset_dependency),
            image_params=Depends(self.img_preview_dependency),
            post_process=Depends(self.process_dependency),
            stats_params=Depends(self.stats_dependency),
            histogram_params=Depends(self.histogram_dependency),
        ):
            """Get Dataset statistics."""
            src_dst = self.reader(src_path, **reader_params.as_dict())
            image = await src_dst.preview(
                **layer_params.as_dict(),
                **image_params.as_dict(),
                **dataset_params.as_dict(),
            )

            if post_process:
                image = post_process(image)

            return image.statistics(
                **stats_params.as_dict(),
                hist_options=histogram_params.as_dict(),
            )

        # POST endpoint
        @self.router.post(
            "/statistics",
            response_model=StatisticsGeoJSON,
            response_model_exclude_none=True,
            response_class=GeoJSONResponse,
            responses={
                200: {
                    "content": {"application/geo+json": {}},
                    "description": "Return dataset's statistics from feature or featureCollection.",
                }
            },
            operation_id=f"{self.operation_prefix}postStatisticsForGeoJSON",
        )
        async def geojson_statistics(
            geojson: Annotated[
                FeatureCollection | Feature,
                Body(description="GeoJSON Feature or FeatureCollection."),
            ],
            src_path=Depends(self.path_dependency),
            reader_params=Depends(self.reader_dependency),
            coord_crs=Depends(CoordCRSParams),
            dst_crs=Depends(DstCRSParams),
            layer_params=Depends(self.layer_dependency),
            dataset_params=Depends(self.dataset_dependency),
            image_params=Depends(self.img_part_dependency),
            post_process=Depends(self.process_dependency),
            stats_params=Depends(self.stats_dependency),
            histogram_params=Depends(self.histogram_dependency),
            env=Depends(self.environment_dependency),
        ):
            """Get Statistics from a geojson feature or featureCollection."""
            fc = geojson
            if isinstance(fc, Feature):
                fc = FeatureCollection(type="FeatureCollection", features=[geojson])

                src_dst = self.reader(src_path, **reader_params.as_dict())
                for feature in fc.features:
                    shape = feature.model_dump(exclude_none=True)
                    image = await src_dst.feature(
                        shape,
                        shape_crs=coord_crs or WGS84_CRS,
                        dst_crs=dst_crs,
                        align_bounds_with_dataset=True,
                        **layer_params.as_dict(),
                        **image_params.as_dict(),
                        **dataset_params.as_dict(),
                    )

                    # Get the coverage % array
                    coverage_array = image.get_coverage_array(
                        shape,
                        shape_crs=coord_crs or WGS84_CRS,
                    )

                    if post_process:
                        image = post_process(image)

                    stats = image.statistics(
                        **stats_params.as_dict(),
                        hist_options=histogram_params.as_dict(),
                        coverage=coverage_array,
                    )

                    feature.properties = feature.properties or {}
                    feature.properties.update({"statistics": stats})

            return fc.features[0] if isinstance(geojson, Feature) else fc

    ############################################################################
    # /tiles
    ############################################################################
    def tile(self):  # noqa: C901
        """Register /tiles endpoint."""

        available_tms = tuple(self.supported_tms.list())

        @self.router.get(
            "/tiles/{tileMatrixSetId}/{z}/{x}/{y}",
            operation_id=f"{self.operation_prefix}getTile",
            **img_endpoint_params,
        )
        @self.router.get(
            "/tiles/{tileMatrixSetId}/{z}/{x}/{y}.{format}",
            operation_id=f"{self.operation_prefix}getTileWithFormat",
            **img_endpoint_params,
        )
        async def tile(
            z: Annotated[
                int,
                Path(
                    description="Identifier (Z) selecting one of the scales defined in the TileMatrixSet and representing the scaleDenominator the tile.",
                ),
            ],
            x: Annotated[
                int,
                Path(
                    description="Column (X) index of the tile on the selected TileMatrix. It cannot exceed the MatrixHeight-1 for the selected TileMatrix.",
                ),
            ],
            y: Annotated[
                int,
                Path(
                    description="Row (Y) index of the tile on the selected TileMatrix. It cannot exceed the MatrixWidth-1 for the selected TileMatrix.",
                ),
            ],
            tileMatrixSetId: Annotated[
                Literal[available_tms],
                Path(
                    description="Identifier selecting one of the TileMatrixSetId supported."
                ),
            ],
            format: Annotated[
                ImageType | None,
                Field(
                    description="Default will be automatically defined if the output image needs a mask (png) or not (jpeg)."
                ),
            ] = None,
            tilesize: Annotated[
                int | None,
                Query(gt=0, description="Tilesize in pixels."),
            ] = None,
            src_path=Depends(self.path_dependency),
            reader_params=Depends(self.reader_dependency),
            tile_params=Depends(self.tile_dependency),
            layer_params=Depends(self.layer_dependency),
            dataset_params=Depends(self.dataset_dependency),
            post_process=Depends(self.process_dependency),
            colormap=Depends(self.colormap_dependency),
            render_params=Depends(self.render_dependency),
            env=Depends(self.environment_dependency),
        ):
            """Create map tile from a dataset."""
            tms = self.supported_tms.get(tileMatrixSetId)
            logger.info(f"opening data with reader: {self.reader}")
            src_dst = self.reader(src_path, tms=tms, **reader_params.as_dict())
            image = await src_dst.tile(
                x,
                y,
                z,
                tilesize=tilesize,
                **tile_params.as_dict(),
                **layer_params.as_dict(),
                **dataset_params.as_dict(),
            )
            dst_colormap = getattr(src_dst, "colormap", None)

            if post_process:
                image = post_process(image)

            content, media_type = self.render_func(
                image,
                output_format=format,
                colormap=colormap or dst_colormap,
                **render_params.as_dict(),
            )

            headers: dict[str, str] = {}
            if image.bounds is not None:
                headers["Content-Bbox"] = ",".join(map(str, image.bounds))
            if uri := CRS_to_uri(image.crs):
                headers["Content-Crs"] = f"<{uri}>"

            return Response(content, media_type=media_type, headers=headers)

    ############################################################################
    # /point
    ############################################################################
    def point(self):
        """Register /point endpoints."""

        @self.router.get(
            "/point/{lon},{lat}",
            response_model=Point,
            response_class=JSONResponse,
            responses={200: {"description": "Return a value for a point"}},
            operation_id=f"{self.operation_prefix}getDataForPoint",
        )
        async def point(
            lon: Annotated[float, Path(description="Longitude")],
            lat: Annotated[float, Path(description="Latitude")],
            src_path=Depends(self.path_dependency),
            reader_params=Depends(self.reader_dependency),
            coord_crs=Depends(CoordCRSParams),
            layer_params=Depends(self.layer_dependency),
            dataset_params=Depends(self.dataset_dependency),
            env=Depends(self.environment_dependency),
        ):
            """Get Point value for a dataset."""
            with rasterio.Env(**env):
                logger.info(f"opening data with reader: {self.reader}")
                src_dst = self.reader(src_path, **reader_params.as_dict())
                pts = await src_dst.point(
                    lon,
                    lat,
                    coord_crs=coord_crs or WGS84_CRS,
                    **layer_params.as_dict(),
                    **dataset_params.as_dict(),
                )

            return {
                "coordinates": [lon, lat],
                "values": pts.array.tolist(),
                "band_names": pts.band_names,
                "band_descriptions": pts.band_descriptions,
            }

    ############################################################################
    # /preview (Optional)
    ############################################################################
    def preview(self):
        """Register /preview endpoint."""

        @self.router.get(
            "/preview",
            operation_id=f"{self.operation_prefix}getPreview",
            **img_endpoint_params,
        )
        @self.router.get(
            "/preview.{format}",
            operation_id=f"{self.operation_prefix}getPreviewWithFormat",
            **img_endpoint_params,
        )
        @self.router.get(
            "/preview/{width}x{height}.{format}",
            operation_id=f"{self.operation_prefix}getPreviewWithSizeAndFormat",
            **img_endpoint_params,
        )
        async def preview(
            format: Annotated[
                ImageType | None,
                Field(
                    description="Default will be automatically defined if the output image needs a mask (png) or not (jpeg)."
                ),
            ] = None,
            src_path=Depends(self.path_dependency),
            reader_params=Depends(self.reader_dependency),
            layer_params=Depends(self.layer_dependency),
            dataset_params=Depends(self.dataset_dependency),
            image_params=Depends(self.img_preview_dependency),
            dst_crs=Depends(DstCRSParams),
            post_process=Depends(self.process_dependency),
            colormap=Depends(self.colormap_dependency),
            render_params=Depends(self.render_dependency),
            env=Depends(self.environment_dependency),
        ):
            """Create preview of a dataset."""
            with rasterio.Env(**env):
                logger.info(f"opening data with reader: {self.reader}")
                src_dst = self.reader(src_path, **reader_params.as_dict())
                image = await src_dst.preview(
                    **layer_params.as_dict(),
                    **image_params.as_dict(exclude_none=False),
                    **dataset_params.as_dict(),
                    dst_crs=dst_crs,
                )
                dst_colormap = getattr(src_dst, "colormap", None)

            if post_process:
                image = post_process(image)

            content, media_type = self.render_func(
                image,
                output_format=format,
                colormap=colormap or dst_colormap,
                **render_params.as_dict(),
            )

            headers: dict[str, str] = {}
            if image.bounds is not None:
                headers["Content-Bbox"] = ",".join(map(str, image.bounds))
            if uri := CRS_to_uri(image.crs):
                headers["Content-Crs"] = f"<{uri}>"

            return Response(content, media_type=media_type, headers=headers)

    ############################################################################
    # /bbox and /feature (Optional)
    ############################################################################
    def part(self):  # noqa: C901
        """Register /bbox and `/feature` endpoints."""

        # GET endpoints
        @self.router.get(
            "/bbox/{minx},{miny},{maxx},{maxy}.{format}",
            operation_id=f"{self.operation_prefix}getDataForBoundingBoxWithFormat",
            **img_endpoint_params,
        )
        @self.router.get(
            "/bbox/{minx},{miny},{maxx},{maxy}/{width}x{height}.{format}",
            operation_id=f"{self.operation_prefix}getDataForBoundingBoxWithSizesAndFormat",
            **img_endpoint_params,
        )
        async def bbox_image(
            minx: Annotated[float, Path(description="Bounding box min X")],
            miny: Annotated[float, Path(description="Bounding box min Y")],
            maxx: Annotated[float, Path(description="Bounding box max X")],
            maxy: Annotated[float, Path(description="Bounding box max Y")],
            format: Annotated[
                ImageType,
                Path(
                    description="Default will be automatically defined if the output image needs a mask (png) or not (jpeg).",
                ),
            ],
            src_path=Depends(self.path_dependency),
            reader_params=Depends(self.reader_dependency),
            layer_params=Depends(self.layer_dependency),
            dataset_params=Depends(self.dataset_dependency),
            image_params=Depends(self.img_part_dependency),
            dst_crs=Depends(DstCRSParams),
            coord_crs=Depends(CoordCRSParams),
            post_process=Depends(self.process_dependency),
            colormap=Depends(self.colormap_dependency),
            render_params=Depends(self.render_dependency),
            env=Depends(self.environment_dependency),
        ):
            """Create image from a bbox."""
            with rasterio.Env(**env):
                logger.info(f"opening data with reader: {self.reader}")
                src_dst = self.reader(src_path, **reader_params.as_dict())
                image = await src_dst.part(
                    [minx, miny, maxx, maxy],
                    dst_crs=dst_crs,
                    bounds_crs=coord_crs or WGS84_CRS,
                    **layer_params.as_dict(),
                    **image_params.as_dict(),
                    **dataset_params.as_dict(),
                )
                dst_colormap = getattr(src_dst, "colormap", None)

            if post_process:
                image = post_process(image)

            content, media_type = self.render_func(
                image,
                output_format=format,
                colormap=colormap or dst_colormap,
                **render_params.as_dict(),
            )

            headers: dict[str, str] = {}
            if image.bounds is not None:
                headers["Content-Bbox"] = ",".join(map(str, image.bounds))
            if uri := CRS_to_uri(image.crs):
                headers["Content-Crs"] = f"<{uri}>"

            return Response(content, media_type=media_type, headers=headers)

        # POST endpoints
        @self.router.post(
            "/feature",
            operation_id=f"{self.operation_prefix}postDataForGeoJSON",
            **img_endpoint_params,
        )
        @self.router.post(
            "/feature.{format}",
            operation_id=f"{self.operation_prefix}postDataForGeoJSONWithFormat",
            **img_endpoint_params,
        )
        @self.router.post(
            "/feature/{width}x{height}.{format}",
            operation_id=f"{self.operation_prefix}postDataForGeoJSONWithSizesAndFormat",
            **img_endpoint_params,
        )
        async def feature_image(
            geojson: Annotated[Feature, Body(description="GeoJSON Feature.")],
            format: Annotated[
                ImageType | None,
                Field(
                    description="Default will be automatically defined if the output image needs a mask (png) or not (jpeg)."
                ),
            ] = None,
            src_path=Depends(self.path_dependency),
            reader_params=Depends(self.reader_dependency),
            layer_params=Depends(self.layer_dependency),
            dataset_params=Depends(self.dataset_dependency),
            image_params=Depends(self.img_part_dependency),
            coord_crs=Depends(CoordCRSParams),
            dst_crs=Depends(DstCRSParams),
            post_process=Depends(self.process_dependency),
            colormap=Depends(self.colormap_dependency),
            render_params=Depends(self.render_dependency),
            env=Depends(self.environment_dependency),
        ):
            """Create image from a geojson feature."""
            with rasterio.Env(**env):
                logger.info(f"opening data with reader: {self.reader}")
                src_dst = self.reader(src_path, **reader_params.as_dict())
                image = await src_dst.feature(
                    geojson.model_dump(exclude_none=True),
                    shape_crs=coord_crs or WGS84_CRS,
                    dst_crs=dst_crs,
                    **layer_params.as_dict(),
                    **image_params.as_dict(),
                    **dataset_params.as_dict(),
                )
                dst_colormap = getattr(src_dst, "colormap", None)

            if post_process:
                image = post_process(image)

            content, media_type = self.render_func(
                image,
                output_format=format,
                colormap=colormap or dst_colormap,
                **render_params.as_dict(),
            )

            headers: dict[str, str] = {}
            if image.bounds is not None:
                headers["Content-Bbox"] = ",".join(map(str, image.bounds))
            if uri := CRS_to_uri(image.crs):
                headers["Content-Crs"] = f"<{uri}>"

            return Response(content, media_type=media_type, headers=headers)

    ############################################################################
    # OGC Maps (Optional)
    ############################################################################
    def ogc_maps(self):  # noqa: C901
        """Register OGC Maps /map` endpoint."""

        self.conforms_to.update(
            {
                "https://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/core",
                "https://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/crs",
                "https://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/scaling",
                "https://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/scaling/width-definition",
                "https://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/scaling/height-definition",
                "https://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/spatial-subsetting",
                "https://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/spatial-subsetting/bbox-definition",
                "https://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/spatial-subsetting/bbox-crs",
                "https://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/spatial-subsetting/crs-curie",
                "https://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/png",
                "https://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/jpeg",
                "https://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/tiff",
            }
        )

        # GET endpoints
        @self.router.get(
            "/map",
            operation_id=f"{self.operation_prefix}getMap",
            **img_endpoint_params,
        )
        async def get_map(
            src_path=Depends(self.path_dependency),
            ogc_params=Depends(OGCMapsParams),
            reader_params=Depends(self.reader_dependency),
            layer_params=Depends(self.layer_dependency),
            dataset_params=Depends(self.dataset_dependency),
            post_process=Depends(self.process_dependency),
            colormap=Depends(self.colormap_dependency),
            render_params=Depends(self.render_dependency),
            env=Depends(self.environment_dependency),
        ) -> Response:
            """OGC Maps API."""
            with rasterio.Env(**env):
                logger.info(f"opening data with reader: {self.reader}")
                src_dst = self.reader(src_path, **reader_params.as_dict())
                if ogc_params.bbox is not None:
                    image = await src_dst.part(
                        ogc_params.bbox,
                        dst_crs=ogc_params.crs or src_dst.crs,
                        bounds_crs=ogc_params.bbox_crs or WGS84_CRS,
                        width=ogc_params.width,
                        height=ogc_params.height,
                        max_size=ogc_params.max_size,
                        **layer_params.as_dict(),
                        **dataset_params.as_dict(),
                    )

                else:
                    image = await src_dst.preview(
                        width=ogc_params.width,
                        height=ogc_params.height,
                        max_size=ogc_params.max_size,
                        dst_crs=ogc_params.crs or src_dst.crs,
                        **layer_params.as_dict(),
                        **dataset_params.as_dict(),
                    )

                dst_colormap = getattr(src_dst, "colormap", None)

            if post_process:
                image = post_process(image)

            content, media_type = self.render_func(
                image,
                output_format=ogc_params.format,
                colormap=colormap or dst_colormap,
                **render_params.as_dict(),
            )

            headers: dict[str, str] = {}
            if image.bounds is not None:
                headers["Content-Bbox"] = ",".join(map(str, image.bounds))
            if uri := CRS_to_uri(image.crs):
                headers["Content-Crs"] = f"<{uri}>"

            return Response(content, media_type=media_type, headers=headers)
