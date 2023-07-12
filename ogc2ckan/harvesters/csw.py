#TODO: WIP harvester
# inbuilt libraries
from datetime import datetime

# third-party libraries
from owslib.wms import WebMapService
from owslib.wfs import WebFeatureService
from owslib.wcs import WebCoverageService
from owslib.wmts import WebMapTileService
from owslib.fes import PropertyIsEqualTo, PropertyIsLike, SortBy, SortProperty

# custom classes
from harvesters.base import Harvester
  
from config.ckan_config import CKANInfo

# custom functions
from mappings.default_ogc2ckan_config import OGC2CKAN_HARVESTER_MD_CONFIG


class HarvesterCSW(Harvester):
    def __init__(self, app_dir, url, name, groups, active, organization, type, custom_organization_active, custom_organization_mapping_file, private_datasets, default_keywords, default_inspire_info, constraints, **default_dcat_info):
        super().__init__(app_dir, url, name, groups, active, organization, type, custom_organization_active, custom_organization_mapping_file, private_datasets, default_keywords, default_inspire_info, **default_dcat_info)
        self.constraints = constraints
        self.wms = None
        self.wfs = None
        self.wcs = None
        self.wmts = None 
        self.wms_url = None
        self.wfs_url = None
        self.wcs_url = None
        self.wmts_url = None
        
    def set_csw_url(self, csw_url):
        self.csw_url = csw_url

    def set_wms_url(self, wms_url):
        self.wms_url = wms_url

    def set_wfs_url(self, wfs_url):
        self.wfs_url = wfs_url

    def set_wcs_url(self, wcs_url):
        self.wcs_url = wcs_url

    def set_wmts_url(self, wmts_url):
        self.wmts_url = wmts_url

    def get_wms_url_value(self):
        return self.wms_url

    def get_wfs_url_value(self):
        return self.wfs_url

    def get_wcs_url_value(self):
        return self.wcs_url

    def get_wmts_url_value(self):
        return self.wmts_url

    def get_wms_url(self):
        return self.wms_url if self.wms_url is not None else (self.url + '?service=wms')

    def get_wfs_url(self):
        return self.wfs_url if self.wfs_url is not None else (self.url + '?service=wfs')

    def get_wcs_url(self):
        return self.wcs_url if self.wcs_url is not None else (self.url + '?service=WCS')

    def get_wmts_url(self):
        return self.wmts_url if self.wmts_url is not None else (self.url.replace('/ows', '/gwc') + '/service/wmts')

    def connect_wms(self):
        return WebMapService(self.get_wms_url())

    def connect_wfs(self):
        return WebFeatureService(self.get_wfs_url())

    def connect_wcs(self):
        return WebCoverageService(self.get_wcs_url())

    def connect_wmts(self):
        return WebMapTileService(self.get_wmts_url())