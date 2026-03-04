"""TiTiler+stacapi FastAPI application."""

from typing import Annotated, Any, Literal

import httpx
import jinja2
import morecantile
import rasterio
from fastapi import FastAPI, Query
from fastapi import __version__ as fastapi_version
from fastapi.responses import ORJSONResponse
from morecantile import TileMatrixSets
from pydantic import __version__ as pydantic_version
from rio_tiler import __version__ as rio_tiler_version
from starlette import __version__ as starlette_version
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.templating import Jinja2Templates

from titiler.core.resources.enums import MediaType
from titiler.core import __version__ as titiler_version
from titiler.core.errors import DEFAULT_STATUS_CODES, add_exception_handlers
from titiler.core.factory import (
    AlgorithmFactory,
    ColorMapFactory,
    TMSFactory,
)
from titiler.core.middleware import CacheControlMiddleware, LoggerMiddleware
from titiler.core.models.OGC import Conformance, Landing
from titiler.core.resources.enums import OptionalHeader
from titiler.core.utils import accept_media_type, create_html_response, update_openapi


from async_titiler.factory import AsyncTilerFactory
from async_titiler.settings import ApiSettings
from async_titiler import __version__ as async_titiler_version

settings = ApiSettings()

# custom template directory
templates_location: list[Any] = (
    [jinja2.FileSystemLoader(settings.template_directory)]
    if settings.template_directory
    else []
)
# default template directory
templates_location.append(jinja2.PackageLoader(__package__, "templates"))
templates_location.append(jinja2.PackageLoader("titiler.core", "templates"))

jinja2_env = jinja2.Environment(
    autoescape=jinja2.select_autoescape(["html", "xml"]),
    loader=jinja2.ChoiceLoader(templates_location),
)
templates = Jinja2Templates(env=jinja2_env)


app = FastAPI(
    title=settings.name,
    openapi_url="/api",
    docs_url="/api.html",
    version=async_titiler_version,
    root_path=settings.root_path,
)
# Fix OpenAPI response header for OGC Common compatibility
update_openapi(app)

add_exception_handlers(app, DEFAULT_STATUS_CODES)

# Set all CORS enabled origins
if settings.cors_origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["*"],
    )

app.add_middleware(CacheControlMiddleware, cachecontrol=settings.cachecontrol)

optional_headers = []
if settings.debug:
    app.add_middleware(LoggerMiddleware)
    optional_headers = [OptionalHeader.server_timing, OptionalHeader.x_assets]


APP_CONFORMS_TO = {
    "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/core",
    "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/landing-page",
    "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/oas30",
    "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/html",
    "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/json",
}

###############################################################################
# Tiling Endpoints
endoints = AsyncTilerFactory(add_viewer=True, templates=templates)
app.include_router(endoints.router)

APP_CONFORMS_TO.update(endoints.conforms_to)

###############################################################################
# Tiling Schemes Endpoints
tms = TMSFactory(templates=templates)
app.include_router(tms.router, tags=["OGC TileMatrix Schemes"])
APP_CONFORMS_TO.update(tms.conforms_to)

###############################################################################
# Algorithms Endpoints
algorithms = AlgorithmFactory(templates=templates)
app.include_router(algorithms.router, tags=["Algorithms"])
APP_CONFORMS_TO.update(algorithms.conforms_to)

###############################################################################
# Colormaps endpoints
cmaps = ColorMapFactory(templates=templates)
app.include_router(cmaps.router, tags=["ColorMaps"])
APP_CONFORMS_TO.update(cmaps.conforms_to)


###############################################################################
# Landing page
@app.get(
    "/",
    response_model=Landing,
    response_model_exclude_none=True,
    response_class=ORJSONResponse,
    responses={
        200: {
            "content": {
                "text/html": {},
                "application/json": {},
            }
        },
    },
    operation_id="getLandingPage",
    summary="landing page",
    tags=["Landing Page"],
)
def landing(
    request: Request,
    f: Annotated[
        Literal["html", "json"] | None,
        Query(
            description="Response MediaType. Defaults to endpoint's default or value defined in `accept` header."
        ),
    ] = None,
):
    """The landing page provides links to the API definition, the conformance statements and to the feature collections in this dataset."""
    data = {
        "title": "Async-TiTiler",
        "description": "TiTiler built on top async-geotiff / obstore",
        "links": [
            {
                "title": "Landing page",
                "href": str(request.url_for("landing")),
                "type": "text/html",
                "rel": "self",
            },
            {
                "title": "The API definition (JSON)",
                "href": str(request.url_for("openapi")),
                "type": "application/vnd.oai.openapi+json;version=3.0",
                "rel": "service-desc",
            },
            {
                "title": "The API documentation",
                "href": str(request.url_for("swagger_ui_html")),
                "type": "text/html",
                "rel": "service-doc",
            },
            {
                "title": "Conformance Declaration",
                "href": str(request.url_for("conformance")),
                "type": "text/html",
                "rel": "http://www.opengis.net/def/rel/ogc/1.0/conformance",
            },
            {
                "title": "List of Available TileMatrixSets",
                "href": str(request.url_for("tilematrixsets")),
                "type": "application/json",
                "rel": "http://www.opengis.net/def/rel/ogc/1.0/tiling-schemes",
            },
            {
                "title": "List of Available Algorithms",
                "href": str(request.url_for("available_algorithms")),
                "type": "application/json",
                "rel": "data",
            },
            {
                "title": "List of Available ColorMaps",
                "href": str(request.url_for("available_colormaps")),
                "type": "application/json",
                "rel": "data",
            },
        ],
    }

    if f:
        output_type = MediaType[f]
    else:
        accepted_media = [MediaType.html, MediaType.json]
        output_type = (
            accept_media_type(request.headers.get("accept", ""), accepted_media)
            or MediaType.json
        )

    if output_type == MediaType.html:
        return create_html_response(
            request,
            data,
            title="Async-TiTiler",
            template_name="landing",
            templates=templates,
        )

    return data


@app.get(
    "/conformance",
    response_model=Conformance,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "text/html": {},
                "application/json": {},
            }
        },
    },
    tags=["OGC Common"],
)
def conformance(
    request: Request,
    f: Annotated[
        Literal["html", "json"] | None,
        Query(
            description="Response MediaType. Defaults to endpoint's default or value defined in `accept` header."
        ),
    ] = None,
):
    """Conformance classes.

    Called with `GET /conformance`.

    Returns:
        Conformance classes which the server conforms to.

    """
    data = {"conformsTo": sorted(APP_CONFORMS_TO)}

    if f:
        output_type = MediaType[f]
    else:
        accepted_media = [MediaType.html, MediaType.json]
        output_type = (
            accept_media_type(request.headers.get("accept", ""), accepted_media)
            or MediaType.json
        )

    if output_type == MediaType.html:
        return create_html_response(
            request,
            data,
            "conformance",
            title="Conformance",
            templates=templates,
        )

    return data


###############################################################################
# Health Check Endpoint
@app.get("/healthz", description="Health Check", tags=["Health Check"])
def ping(request: Request) -> dict:
    """Health check."""
    try:
        resp = httpx.get(app.state.stac_url)
        api_online = True if resp.status_code == 200 else False
    except:  # noqa
        api_online = False

    data = {
        "stac-api_online": api_online,
        "versions": {
            "titiler": titiler_version,
            "async-titiler": async_titiler_version,
            "rasterio": rasterio.__version__,
            "rio-tiler": rio_tiler_version,
            "gdal": rasterio.__gdal_version__,
            "proj": rasterio.__proj_version__,
            "fastapi": fastapi_version,
            "starlette": starlette_version,
            "pydantic": pydantic_version,
        },
    }
    return data
