FROM python:3.11-slim-bullseye
LABEL maintainer="mnl.janez@gmail.com"

ENV APP_DIR=/app
ENV TZ=UTC
RUN echo ${TZ} > /etc/timezone
ENV CKAN_URL=http://localhost:5000/
ENV PYCSW_URL=http://localhost:8000/
ENV CKAN_API_KEY=ckan-api-key
ENV DEFAULT_LICENSE=http://creativecommons.org/licenses/by/4.0/
ENV DEFAULT_LICENSE_ID=cc-by
ENV DEV_MODE=False
ENV TIMEOUT=300

RUN apt-get -q -y update && \
    apt-get install -y wget && \
    DEBIAN_FRONTEND=noninteractive apt-get -yq install gettext-base && \
    wget -O /wait-for https://raw.githubusercontent.com/eficode/wait-for/v2.2.3/wait-for && \
    chmod +x /wait-for && \
    python3 -m pip install pdm

WORKDIR ${APP_DIR}
COPY pyproject.toml pdm.lock .

RUN pdm install --no-self --group prod

COPY ckan-ogc/conf/config.yaml.template config.yaml
COPY ckan-ogc/entrypoint.sh entrypoint.sh
COPY ogc2ckan ogc2ckan
COPY data data

ENTRYPOINT ["/bin/bash", "./entrypoint.sh"]