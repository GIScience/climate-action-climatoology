FROM python:3.11.5-bookworm as builder

RUN pip install --no-cache-dir poetry==1.6.1

COPY pyproject.toml poetry.lock ./

RUN poetry install --no-ansi --no-interaction --all-extras --without dev,test --no-root

COPY climatoology ./climatoology
COPY conf ./conf
COPY README.md ./README.md

RUN poetry install --no-ansi --no-interaction --all-extras --without dev,test

SHELL ["/bin/bash", "-c"]
ENTRYPOINT poetry run python climatoology/app/api.py
EXPOSE 8000
