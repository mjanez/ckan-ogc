<h1 align="center">CKAN Docker harvester</h1>
<p align="center">
<a href="https://github.com/mjanez/ogc-ckan-tools"><img src="https://img.shields.io/badge/%20ckan-ogc-brightgreen" alt="ogc2ckan version"></a><a href="https://opensource.org/licenses/MIT"> <img src="https://img.shields.io/badge/license-Unlicense-brightgreen" alt="License: Unlicense"></a> <a href="https://github.com/mjanez/ckan-pycsw/actions/workflows/docker/badge.svg" alt="License: Unlicense"></a>


<p align="center">
    <a href="#overview">Overview</a> •
    <a href="#quick-start">Quick start</a> •
    <a href="#schema-development">Schema development</a> •
    <a href="#test">Test</a> •
    <a href="#debug">Debug</a> •
    <a href="#containers">Containers</a>
</p>

**Requirements**:
* [Docker](https://docs.docker.com/get-docker/)

## Overview
Docker compose environment for development and testing with CKAN Open Data portals.

>**Note**<br>
> In the integration with: [mjanez/ckan-docker](https://github.com/mjanez/ckan-docker)[^1], it is possible to test it with a CKAN-type open data portal.

## Quick start
### With docker compose
Copy the `.env.example` template and configure by changing the `.env` file. Change `PYCSW_URL` and `CKAN_URL`,  as well as the published port `PYCSW_PORT`, if needed.

    ```shell
    cp .env.example .env
    ```

Configure the [ckan-ogc/conf/config.yaml.template](/ogc-ckan-tools/ckan-ogc/conf/config.yaml.template).


[^1]: A custom installation of Docker Compose with specific extensions for spatial data and [GeoDCAT-AP](https://github.com/SEMICeu/GeoDCAT-AP)/[INSPIRE](https://github.com/INSPIRE-MIF/technical-guidelines) metadata [profiles](https://en.wikipedia.org/wiki/Geospatial_metadata).

## Debug
### VSCode
1. Build and run container.
2. Attach Visual Studio Code to container
3. Start debugging on `ckan2pycsw.py` Python file (`Debug the currently active Python file`).
