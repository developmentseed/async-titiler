"""async-titiler IO dependencies."""

import os
import posixpath
import re
from pathlib import Path
from typing import Annotated, Any
from urllib.parse import urlparse

import httpx
from async_geotiff import GeoTIFF
from cache import AsyncTTL
from fastapi import Query
from obstore.store import from_url


@AsyncTTL(time_to_live=300)
async def _find_bucket_region(bucket: str, use_https: bool = True) -> str | None:
    prefix = "https" if use_https else "http"
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{prefix}://{bucket}.s3.amazonaws.com")
    return response.headers.get("x-amz-bucket-region")


@AsyncTTL(time_to_live=300)
async def _get_geotiff(url: str) -> GeoTIFF:
    parsed = urlparse(url)
    if not parsed.scheme:
        url = str(Path(url).resolve())
        parsed = urlparse(f"file://{url}")

    config: dict[str, Any] = {}
    client_options: dict[str, Any] = {}

    s3_hosts = [
        "amazonaws.com",
    ]
    if parsed.scheme == "s3" or any(host in parsed.netloc for host in s3_hosts):
        region_name_env = (
            os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION")) or None
        )

        # s3:// urls
        if parsed.scheme == "s3":
            config["region"] = (
                await _find_bucket_region(parsed.netloc) or region_name_env
            )

            # AWS_S3_ENDPOINT and AWS_HTTPS are GDAL config options of vsis3 driver
            # # https://gdal.org/user/virtual_file_systems.html#vsis3-aws-s3-files
            endpoint_url = os.environ.get("AWS_S3_ENDPOINT", None)
            use_https = os.environ.get("AWS_HTTPS", "YES") in ["YES", "TRUE", "ON"]
            if endpoint_url:
                config["endpoint"] = (
                    "https://" + endpoint_url if use_https else "http://" + endpoint_url
                )

        # https://{bucket}.s3.{region}?.amazonaws.com urls
        else:
            # We assume that https:// url are public object
            config["skip_signature"] = True

            # Get Region from URL or guess if needed
            if expr := re.compile(
                r"(?P<bucket>[a-z0-9\.\-_]+)\.s3"
                r"(\.dualstack)?"
                r"(\.(?P<region>[a-z0-9\-_]+))?"
                r"\.amazonaws\.(com|cn)",
                re.IGNORECASE,
            ).match(parsed.netloc):
                bucket = expr.groupdict()["bucket"]
                if not expr.groupdict().get("region"):
                    config["region"] = (
                        await _find_bucket_region(bucket) or region_name_env
                    )

    directory = posixpath.dirname(parsed.path)
    filename = posixpath.basename(parsed.path)
    store_url = f"{parsed.scheme}://{parsed.netloc}{directory}"

    store = from_url(
        store_url,
        config=config,
        client_options=client_options,
    )
    geotiff = await GeoTIFF.open(filename, store=store)

    return geotiff


# NOTE: Find a way to cache this
async def DatasetPathParams(
    url: Annotated[str, Query(description="Dataset URL")],
) -> GeoTIFF:
    """Create dataset path from args"""
    return await _get_geotiff(url)
