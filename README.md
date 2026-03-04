# async-titiler

<p align="center">
  <p align="center">TiTiler built with Async-GeoTIFF</p>
</p>

<p align="center">
  <a href="https://github.com/developmentseed/async-titiler/actions?query=workflow%3ACI" target="_blank">
      <img src="https://github.com/developmentseed/async-titiler/workflows/CI/badge.svg" alt="Test">
  </a>
  <a href="https://codecov.io/gh/developmentseed/async-titiler" target="_blank">
      <img src="https://codecov.io/gh/developmentseed/async-titiler/branch/main/graph/badge.svg" alt="Coverage">
  </a>
  <a href="https://github.com/developmentseed/async-titiler/blob/main/LICENSE" target="_blank">
      <img src="https://img.shields.io/github/license/developmentseed/async-titiler.svg" alt="License">
  </a>
</p>

---

**Documentation**: 

**Source Code**: <a href="https://github.com/developmentseed/async-titiler" target="_blank">https://github.com/developmentseed/async-titiler</a>

---

## Installation

To install from sources and run for development:

We recommand using [`uv`](https://docs.astral.sh/uv) as project manager for development.

See https://docs.astral.sh/uv/getting-started/installation/ for installation 

```bash
git clone https://github.com/developmentseed/async-titiler.git
cd async-titiler

uv sync
```

## Launch

```
uv run --extra server uvicorn async_titiler.main:app --port 8000
```

### Using Docker

```
$ git clone https://github.com/developmentseed/async-titiler.git
$ cd async-titiler
$ docker-compose up --build api
```

It runs `async_titiler` using Uvicorn web server.

## Contribution & Development

See [CONTRIBUTING.md](https://github.com//developmentseed/async-titiler/blob/main/CONTRIBUTING.md)

## License

See [LICENSE](https://github.com//developmentseed/async-titiler/blob/main/LICENSE)

## Authors

See [contributors](https://github.com/developmentseed/async-titiler/graphs/contributors) for a listing of individual contributors.

## Changes

See [CHANGES.md](https://github.com/developmentseed/async-titiler/blob/main/CHANGES.md).
