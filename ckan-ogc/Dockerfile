FROM python:3.11-slim
LABEL maintainer="mnl.janez@gmail.com"

ENV APP_DIR=/app
ENV TZ=UTC
RUN echo ${TZ} > /etc/timezone
ENV CKAN_URL=http://localhost:5000/
ENV PYCSW_URL=http://localhost:8000/
ENV CKAN_API_KEY=ckan-api-key
ENV CKAN_DATASET_MULTILANG=False
ENV DEFAULT_LICENSE=http://creativecommons.org/licenses/by/4.0/
ENV DEFAULT_LICENSE_ID=cc-by
ENV DEV_MODE=False
ENV TIMEOUT=300
ENV SSL_UNVERIFIED_MODE=False

RUN apt-get -q -y update && \
    apt-get install -y wget curl && \
    DEBIAN_FRONTEND=noninteractive apt-get -yq install gettext-base && \
    wget -O /wait-for https://raw.githubusercontent.com/eficode/wait-for/v2.2.3/wait-for && \
    chmod +x /wait-for && \
    python3 -m pip install pdm

WORKDIR ${APP_DIR}
COPY pyproject.toml pdm.lock .

RUN pdm install --no-self --group prod

COPY ckan-ogc/conf/config.yaml.template config.yaml
COPY ckan-ogc/docker-entrypoint.d/entrypoint.sh entrypoint.sh
COPY ogc2ckan ogc2ckan

ENTRYPOINT ["/bin/bash", "./entrypoint.sh"]