ARG PYTHON_VERSION=3.14

FROM python:${PYTHON_VERSION}
RUN apt update && apt upgrade -y \
  && apt install curl -y \
  && rm -rf /var/lib/apt/lists/*

# Ensure root certificates are always updated at evey container build
# and curl is using the latest version of them
RUN mkdir /usr/local/share/ca-certificates/cacert.org
RUN cd /usr/local/share/ca-certificates/cacert.org && curl -k -O https://www.cacert.org/certs/root.crt
RUN cd /usr/local/share/ca-certificates/cacert.org && curl -k -O https://www.cacert.org/certs/class3.crt
RUN update-ca-certificates
ENV CURL_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt

RUN python -m pip install -U pip
RUN python -m pip install uvicorn uvicorn-worker gunicorn

COPY async_titiler/ async_titiler/
COPY pyproject.toml pyproject.toml
COPY README.md README.md
COPY LICENSE LICENSE

RUN python -m pip install --no-cache-dir --upgrade .
RUN rm -rf async_titiler/ pyproject.toml README.md LICENSE

RUN groupadd -g 1000 user && \
    useradd -u 1000 -g user -s /bin/bash -m user

USER user