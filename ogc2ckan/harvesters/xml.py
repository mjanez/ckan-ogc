#TODO: WIP harvester
# inbuilt libraries
from datetime import datetime

# third-party libraries
from owslib.iso import *

# custom classes
from harvesters.base import Harvester
  
from config.ckan_config import CKANInfo

# custom functions
from mappings.default_ogc2ckan_config import OGC2CKAN_HARVESTER_MD_CONFIG


class HarvesterMetadataFile(Harvester):
    def __init__(self, app_dir, url, name, groups, active, organization, type, custom_organization_active, custom_organization_mapping_file, private_datasets, default_keywords, default_inspire_info, constraints, **default_dcat_info):
        super().__init__(app_dir, url, name, groups, active, organization, type, custom_organization_active, custom_organization_mapping_file, private_datasets, default_keywords, default_inspire_info, **default_dcat_info)
        self.constraints = constraints
        self.metadata_file = None