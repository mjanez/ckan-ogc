<h1 align="center">CKAN Docker Compose harvesters</h1>
<p align="center">
<a href="https://github.com/mjanez/ckan-ogc"><img src="https://img.shields.io/badge/%20ckan-ogc-brightgreen" alt="ogc2ckan version"></a><a href="https://opensource.org/licenses/MIT"> <img src="https://img.shields.io/badge/license-Unlicense-brightgreen" alt="License: Unlicense"></a> <a href="https://github.com/mjanez/ckan-ogc/actions/workflows/docker/badge.svg" alt="License: Unlicense"></a>


<p align="center">
    <a href="#overview">Overview</a> •
    <a href="#quick-start">Quick start</a> •
    <a href="#debug">Debug</a> •
    <a href="#additional-info">Additional info</a>
    <a href="#containers">Containers</a>
</p>

**Requirements**:
* [Docker](https://docs.docker.com/get-docker/)

## Overview
Docker Compose environment for ingesting metadata from different spatial/semantic/general metadata sources into CKAN.

* OGC harvester (WCS/WFS, WMS & WMTS services)
* CSW harvester (ISO 19115/19139 Metadata Catalogue Services)
* Spreadsheets (XLS/XLSX)
* Metadata files (XML ISO19139)
* CKAN API - WIP
* Semantic metadata files (RDF/TTL) - WIP
* Tabular data (CSV, TSV) - WIP

>**Note**<br>
> It can be tested with an open data portal of the CKAN type such as: : [mjanez/ckan-docker](https://github.com/mjanez/ckan-docker)[^1]

## Quick start
First copy the `.env.example` template and configure by changing the `.env` file. Change `PYCSW_URL` and `CKAN_URL`,  as well as the Harvester info  `OGC2CKAN INFO`, if needed.

```bash
cp .env.example .env
```

Custom ennvars:
- `CKAN_URL`: CKAN site URL to load the harvested datasets.
- `PYCSW_URL`: PyCSW site URL to load the harvested datasets.
- `APP_DIR`: Path to the application folder in Docker.
- `TZ`: Timezone.
- `CKAN_API_KEY`: CKAN authorisation key can be created at `{CKAN_URL}/user/admin`.
- `DEFAULT_LICENSE`: Default license for the harvested datasets. Open Data default: `http://creativecommons.org/licenses/by/4.0/`
- `DEFAULT_LICENSE_ID`: Default license ID for the harvested datasets, ID list: `{ckan_site_url}/api/3/action/license_list`. Open Data default: `cc-by-4.0`
- `PARALLELIZATION`: [WIP] Parallelization of the harvesters. Default: `False`
- `CKAN_DATASET_SCHEMA`: Dataset schema of the CKAN Endpoint. Default: `geodcatap_eu`
- `SSL_UNVERIFIED_MODE`: SSL certificate from host will download if `SSL_UNVERIFIED_MODE=True`. Ennvar to avoid SSL error when certificate was self-signed.
- `METADATA_DISTRIBUTIONS`: If need to create a metadata distributions as CKAN resources (GeoDCAT-AP/ISO19139), set `METADATA_DISTRIBUTIONS=True`. Default: `False`

    >**Warning**<br>
    > `SSL_UNVERIFIED_MODE=True` is not recommended for production environments. Update your certificate or use a valid one. **Check the container log if it fails, and put `True` in the `.env` file.**

Then configure your custom `ckan-ogc/conf/config.yaml.template`. Define the harvest servers and the CKAN DCAT default info.
* Put your XLS/CSV/XML files in: [./data/*](/data/) folder as you need.

>**Note**<br>
>Also if you need create yous custom organization YAML file in `ogc2ckan/mappings/organizations`. Use the template `ogc2ckan/mappings/organizations/organizations.yaml.template` to create your custom file.


### With docker compose
To deploy the environment, `docker compose` will build the latest source in the repo.

To deploy a `5 minutes` image, use the latest ([`ghcr.io/mjanez/ckan-ogc:latest`](https://github.com/mjanez/ckan-ogc/pkgs/container/ckan-ogc)) with [`docker-compose.ghcr.yml`](/docker-compose.ghcr.yml)


```bash
git clone https://github.com/mjanez/ckan-ogc
cd ckan-ogc

docker compose up --build

# Github latest registry image
docker compose -f docker-compose.ghcr.yml --build

# Or detached mode
docker compose up -d --build
```

>**Note**:<br>
> Deploy the dev (local build) `docker-compose.dev.yml` with:
>
>```bash
> docker compose -f docker-compose.dev.yml up --build
>```


>**Note**:<br>
>If needed, to build a specific container simply run:
>
>```bash
>  docker build -t target_name xxxx/
>```

### Without Docker
Dependencies:
```bash
python3 -m pip install --user pip3
pip3 install pdm
pdm install --no-self
```

configure your custom `config.yaml`. Define the harvest servers and the CKAN DCAT default info.

```bash
cp ckan-ogc/conf/config.yaml.template ./config.yaml
```

Remember to configure your `.env`
```yaml
cp .env.example .env
```

Run:
```bash
pdm run python ogc2ckan/ogc2ckan.py
```

## Debug
### VSCode
#### Python debugger with Docker
1. Build and run container.
2. Attach Visual Studio Code to container.
3. Start debugging on `ogc2ckan.py` Python file (`Debug the currently active Python file`) in the container.

#### Python debugger without Docker
1. Update the previously created `.env` file in the root of the `ckan-ogc` repo and move it to: [`/ogc2ckan`](/ogc2ckan)
2. Open [`ogc2ckan.py`](/ogc2ckan/ogc2ckan.py).
3. Start debugging on `ogc2ckan.py` Python file (`Debug the currently active Python file`). 

>**Note**<br
> By default, the Python extension looks for and loads a file named `.env` in the current workspace folder. More info about Python debugger and [Enviromental variables use](https://code.visualstudio.com/docs/python/environments#_environment-variables).

## Additional info
### CKAN Schemas
The CKAN output schemas are located in the [`ogc2ckan/ckan_datasets`](./ogc2ckan/ckan_datasets) folder. The schemas are used to map the metadata fields from the different sources to the CKAN dataset fields. Now are available the following schemas:
   * `geodcatap`: Schema based in [GeoDCAT-AP Schema for CKAN](https://github.com/mjanez/ckanext-schemingdcat).
   * `base`: A DCAT schema with the basic fields. 

You can create your own Schema.

### Harvester
The harvester is located in the [`ogc2ckan/harvesters`](./ogc2ckan/harvesters) folder. The harvester is a Python script that harvests the metadata from the different sources and creates the datasets in CKAN.

There are differente harvesters:
   * `csw`: Harvests the metadata from a CSW server using OWSLib.
   * `table`: Harvests the metadata from a XLS/XLSX file that contains the metadata records in a table format using the CKAN `field_name` of the [custom schemas](./ogc2ckan/mappings/ckan_fields) as the column name.
   * `ogc`: Harvests the metadata from a OGC server (WCS/WFS, WMS & WMTS services) using OWSLib.
   * `xml`: Harvests the metadata from a XML file that contains the metadata records in a ISO19139 format.

You can create your own Harvester.

### Configuration file (`config.yaml`)
The configuration file is located in the [`ckan-ogc/conf/config.yaml.template`](./config.yaml.template) file. It is a YAML file that contains the configuration of the harvesters and the CKAN DCAT default info.

The configuration file contains the elements that are used to configure the harvesters and the CKAN DCAT default info. The required elements inside the Harvesters are specified at Harvester Schema in the [`ogc2ckan/model/harvest_schema.py`](./ogc2ckan/model/harvest_schema.py) file.

### Custom organizations
A custom organization is a YAML file that contains the custom metadata fields that will be used to create the dataset in CKAN. The custom organization is located in the [`ogc2ckan/mappings/organizations`](./ogc2ckan/mappings/organizations) folder.

If you need create yous custom organization YAML file use the [`template-org.yaml`](./ogc2ckan/mappings/organizations/template-org.yaml) to create your custom file. Specified by the `dataset_id` the custom organization will be used in the harvested datasets (if the organization exists in the CKAN instance) to create the dataset with the custom metadata fields.


## Containers
List of *containers*:
### Base images
| Repository | Type | Docker tag | Size | Notes |
| --- | --- | --- | --- | --- |
| python 3.13| base image | `python/python:3.12-slim-bullseye` | 45.57 MB |  - |

### Built images
| Repository | Type | Docker tag | Size | Notes |
| --- | --- | --- | --- | --- |
| mjanez/ckan-ogc| custom image | `mjanez/ckan-ogc:latest-dev` | 582 MB |  Latest stable version from Registry. |
| mjanez/ckan-ogc| custom image | `mjanez/ckan-ogc:main-dev` | 582 MB |  Dev version from Registry.  |
| mjanez/ckan-ogc| custom image | `mjanez/ckan-ogc:latest` | 457 MB |  Latest stable version. |
| mjanez/ckan-ogc| custom image | `mjanez/ckan-ogc:main` | 457 MB |  Development branch version.  |


[^1]: A custom installation of Docker Compose with specific extensions for spatial data and [GeoDCAT-AP](https://github.com/SEMICeu/GeoDCAT-AP)/[INSPIRE](https://github.com/INSPIRE-MIF/technical-guidelines) metadata [profiles](https://en.wikipedia.org/wiki/Geospatial_metadata).
