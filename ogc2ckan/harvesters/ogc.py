# inbuilt libraries
from datetime import datetime
from urllib.parse import urlencode

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


class HarvesterOGC(Harvester):
    def __init__(self, app_dir, url, name, groups, active, organization, type, custom_organization_active, custom_organization_mapping_file, private_datasets, default_keywords, default_inspire_info, ckan_name_not_uuid, workspaces, constraints=None, **default_dcat_info):
        super().__init__(app_dir, url, name, groups, active, organization, type, custom_organization_active, custom_organization_mapping_file, private_datasets, default_keywords, default_inspire_info, ckan_name_not_uuid, **default_dcat_info)
        self.workspaces = workspaces
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

    def get_datasets(self, ckan_info):
        # Connect to OGC services
        self.wms = self.connect_wms()
        self.wfs = self.connect_wfs()
        self.wcs = self.connect_wcs()
        self.wmts = self.connect_wmts()
        
        for record in self.wcs.contents:
            self.datasets.append(self.get_dataset(ckan_info, record, 'wcs'))
        for record in self.wfs.contents:
            self.datasets.append(self.get_dataset(ckan_info, record, 'wfs'))
                
        return self.datasets
        
    def get_dataset(self, ckan_info: CKANInfo, record: str, service_type: str):
        '''
        Gets a dataset from an OGC service. If the layer is also published as a WMS or WMTS layer, the distribution is also included.

        Args:
            ckan_info (CKANInfo): CKANInfo object containing the CKAN URL and API key.
            record (str): Name of the dataset to retrieve.
            service_type (str): Type of OGC service ('wfs' for feature datasets or 'wcs' for coverage datasets).

        Returns:
            Dataset: Dataset object.
        '''
        # Get basic elements for the CKAN dataset
        dataset, distribution, datadictionary, datadictionaryfield, uuid_identifier, ckan_name, ckan_groups, inspire_id = \
            self.get_dataset_common_elements(record, ckan_info.ckan_dataset_schema)
        
        # layer_info for wfs/wcs
        if service_type == 'wfs':
            layer_info = self.wfs.contents.get(record)
            wms_name = record
            wms_layer_info = self.wms.contents.get(wms_name)
            wmts_layer_info = self.wmts.contents.get(wms_name)
        elif service_type == 'wcs':
            layer_info = self.wcs.contents.get(record)
            wms_name = record.replace("__", ":")
            wms_layer_info = self.wms.contents.get(wms_name)
            wmts_layer_info = self.wmts.contents.get(wms_name)

        # Search if custom organization info exists for the dataset
        custom_metadata = None
        if self.custom_organization_active:
            custom_metadata = self.get_custom_default_metadata(wms_name.split(':')[1])

        # OGC Workspace and OGC services info
        ogc_workspace = wms_name.split(':')[0] if wms_name else None

        # Set basic info of MD
        dataset = dataset(uuid_identifier, ckan_name, self.organization, ckan_info.default_license_id)
        dataset.set_ogc_workspace(ogc_workspace)
        
        # Set private dataset
        private = getattr(self, 'private_datasets', False)
        dataset.set_private(private)
        
        # Set alternate identifier (layer name)
        alternate_identifier = wms_name if wms_name else None
        dataset.set_alternate_identifier(alternate_identifier)

        # Title
        title = custom_metadata.get('title') if custom_metadata else layer_info.title
        dataset_title = title or self.localized_strings_dict['title'] if title is not None else f"{self.localized_strings_dict['title']} {inspire_id}"
        dataset.set_title(dataset_title)

        # Description
        description = custom_metadata.get('description') if custom_metadata else layer_info.abstract
        dataset.set_notes(description or self.localized_strings_dict['description'])
  
        # CKAN Groups defined in config.yaml
        dataset.set_groups(ckan_groups)

        # Set inspireId (identifier)
        dataset.set_inspire_id(inspire_id)  

        # Creation/Publication/Revision dates
        issued_date = datetime.now().strftime('%Y-%m-%d')
        dataset.set_issued(issued_date)
        dataset.set_created(issued_date)
        dataset.set_modified(issued_date)

        # DCAT Type (dataset/series)
        dcat_type = OGC2CKAN_HARVESTER_MD_CONFIG['dcat_type']
        is_series = wms_layer_info and wms_layer_info.timepositions
        dataset.set_resource_type(dcat_type['series'] if is_series else dcat_type['dataset'])

        # Set SpatialRepresentationType
        dataset.set_representation_type(OGC2CKAN_HARVESTER_MD_CONFIG['representation_type'].get(service_type))

        # Set valid date
        if hasattr(wms_layer_info, 'valid'):
            valid_date = wms_layer_info.valid
        else:
            valid_date = self.get_default_dcat_info_attribute("valid")

        if valid_date and valid_date is not None:
            dataset.set_valid(self._normalize_date(valid_date))

        # Set access rights (Dataset)
        ## Unnecesary. Default rights in Dataset

        # Set SpatialResolutionInMeters if denominators exist
        denominators = getattr(layer_info, 'denominators', [])
        if denominators:
            dataset.set_spatial_resolution_in_meters(denominators[0])

        # Set language
        language = self.get_default_dcat_info_attribute("language").replace('https:', 'http:')
        dataset.set_language(language)

        # Set spatial coverage
        bb = wms_layer_info.boundingBoxWGS84 if wms_layer_info else layer_info.boundingBox
        self.set_bounding_box(dataset, bb) if bb is not None else None

        # Set spatial URI      
        spatial_uri = custom_metadata.get('spatial_uri') if custom_metadata else self.get_default_dcat_info_attribute("spatial_uri")
        dataset.set_spatial_uri(spatial_uri)       

        # Set temporal coverage
        if is_series and wms_layer_info and wms_layer_info.timepositions:
            time_extent = wms_layer_info.timepositions[0].split(",")
            dataset.set_temporal_start(time_extent[0].split("/")[0])
            dataset.set_temporal_end(time_extent[-1].split("/")[-1] if len(time_extent) > 1 else None)
        else:
            time_extent = {
                'temporal_start': custom_metadata.get('temporal_start') if custom_metadata else self.default_dcat_info.temporal_start if self.default_dcat_info and hasattr(self.default_dcat_info, 'temporal_start') else None,
                'temporal_end': custom_metadata.get('temporal_end') if custom_metadata else self.default_dcat_info.temporal_end if self.default_dcat_info and hasattr(self.default_dcat_info, 'temporal_end') else None
            }
            dataset.set_temporal_start(self._normalize_date(time_extent['temporal_start']))
            dataset.set_temporal_end(self._normalize_date(time_extent['temporal_end']))

        # Set frequency
        if hasattr(wms_layer_info, 'frequency'):
            frequency = wms_layer_info.frequency
        else:
            frequency = OGC2CKAN_HARVESTER_MD_CONFIG['frequency']
        dataset.set_frequency(frequency)

        # Set info if exists custom metadata
        if custom_metadata is not None:
            # Set provenance (INSPIRE Lineage)
            provenance = custom_metadata.get('provenance', self.localized_strings_dict['provenance'])
            dataset.set_provenance(provenance)

            # Set source (INSPIRE quality)
            source = custom_metadata.get('source', None)
            dataset.set_source(source)
            
            # Set lineage_source (INSPIRE Lineage sources)
            lineage_source = custom_metadata.get('lineage_source', self.localized_strings_dict.get('lineage_source', None))
            dataset.set_lineage_source(lineage_source)

            # Set reference
            reference = custom_metadata.get('reference', None)
            dataset.set_reference(reference)

            # Set process steps (INSPIRE quality)
            lineage_process_steps = custom_metadata.get('lineage_process_steps', self.localized_strings_dict.get('lineage_process_steps', None))
            dataset.set_lineage_process_steps(lineage_process_steps)

        # Set conformance (INSPIRE regulation) and set EPSG
        epsg_text = layer_info.crsOptions[0] if layer_info.crsOptions else None
        self._set_conformance(dataset, epsg_text=epsg_text)

        # Set Metadata profile
        metadata_profile = self.get_default_dcat_info_attribute("metadata_profile")
        dataset.set_metadata_profile(metadata_profile)         

        # Set Responsible Parties (Point of contact, Resource publisher, Resource creator/author and Resource contact/maintainer)
        if custom_metadata is not None:
            self.set_custom_responsible_parties(dataset, custom_metadata, ckan_info.ckan_site_url)
        else:
            self.set_default_responsible_parties(dataset, self.default_dcat_info, ckan_info)

        # Overwrite Point of contact (Metadata) and Responsible Party (Resource) from OGC Info
        if self.wms.provider:
            contact_name = self.wms.provider.contact.name if self.wms.provider.contact.name is not None else self.wms.provider.contact.organization
            dataset.set_contact_name(contact_name)
            dataset.set_contact_email(self.wms.provider.contact.email.lower())

        # Set license
        dataset.set_license(ckan_info.default_license)
        
        # Set distributions
        self.get_distribution(ckan_info, dataset, distribution, record, service_type, layer_info, wms_layer_info, wmts_layer_info)
        
        # Metadata distributions (INSPIRE & GeoDCAT-AP)
        self.set_metadata_distributions(ckan_info, dataset, distribution, record)
        
        # Set keywords/themes/topic categories
        self.set_default_keywords_themes_topic(dataset, custom_metadata, ckan_info.ckan_dataset_schema)
        
        # Set translated fields if multilang is True
        if ckan_info.dataset_multilang:
            self.set_translated_fields(dataset, layer_info, language)
        
        return dataset
    
    def get_distribution(self, ckan_info: CKANInfo, dataset, distribution, record: str, service_type: str, layer_info, wms_layer_info, wmts_layer_info):
        # Add distributions (WMS, WFS, WMTS & GeoJSON)
        dataset.set_distributions([])

        def add_distribution(distribution, dist_info):
            dataset.add_distribution(self.get_ckan_distribution(distribution, record, dist_info))

        # WMS
        if wms_layer_info is not None:
            wms_url = f"{self.get_wms_url()}&request=GetCapabilities#{record}"
            dist_info = self._get_distribution_info("WMS", wms_url, self.localized_strings_dict['distributions']['wms'], ckan_info.default_license, ckan_info.default_license_id, dataset.access_rights, dataset.language)
            add_distribution(distribution, dist_info)

        # WMTS
        if wmts_layer_info is not None:
            wmts_url = f"{self.get_wmts_url()}?request=GetCapabilities"
            dist_info = self._get_distribution_info("WMTS", wmts_url, self.localized_strings_dict['distributions']['wmts'], ckan_info.default_license, ckan_info.default_license_id, dataset.access_rights, dataset.language)
            add_distribution(distribution, dist_info)

        # WFS
        if service_type == "wfs":
            wfs_url = f"{self.get_wfs_url()}&request=GetCapabilities#{record}"
            dist_info = self._get_distribution_info("WFS", wfs_url, self.localized_strings_dict['distributions']['wfs'], ckan_info.default_license, ckan_info.default_license_id, dataset.access_rights, dataset.language)
            add_distribution(distribution, dist_info)

            # GeoJSON
            layer_id_parts = layer_info.id.split(':')
            workspace = layer_id_parts[0]
            layername = layer_id_parts[1]
            json_url = f"{self.get_wfs_url().replace('geoserver/ows', f'geoserver/{workspace.lower()}/ows')}&version=1.0.0&request=GetFeature&typeName={layername.lower()}&outputFormat=application/json&maxFeatures=100"
            dist_info_json = self._get_distribution_info("GeoJSON", json_url, self.localized_strings_dict['distributions']['geojson'], ckan_info.default_license, ckan_info.default_license_id, dataset.access_rights, dataset.language)
            add_distribution(distribution, dist_info_json)

        # WCS
        if service_type == "wcs":
            wcs_url = f"{self.get_wcs_url()}&request=GetCapabilities#{record}"
            dist_info = self._get_distribution_info("WCS", wcs_url, self.localized_strings_dict['distributions']['wcs'], ckan_info.default_license, ckan_info.default_license_id, dataset.access_rights, dataset.language)
            add_distribution(distribution, dist_info)
