FROM python:3.11.5-bookworm as builder

RUN pip install --no-cache-dir poetry==1.6.1

COPY pyproject.toml poetry.lock ./

RUN poetry install --no-ansi --no-interaction --no-root --all-extras --without dev,test

COPY climatoology ./climatoology
COPY conf ./conf

SHELL ["/bin/bash", "-c"]
ENTRYPOINT poetry run uvicorn climatoology.app.api:app --host 0.0.0.0 --port 8000 --root-path ${ROOT_PATH:-'/'} --log-config=conf/logging/app/logging.yaml
EXPOSE 8000
