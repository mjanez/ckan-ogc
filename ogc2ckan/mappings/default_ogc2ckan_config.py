# Default values of ogc2ckan
# File paths
OGC2CKAN_PATHS_CONFIG = {
    'default_localized_strings_file': 'default_localized_strings.yaml',
    'default_languages_yaml': 'languages.yaml',
    'default_mappings_folder': 'ogc2ckan/mappings'
}

# Harvesters develop for this project. ogc2ckan/harvesters/harvesters.py
OGC2CKAN_HARVESTER_CONFIG = {
    'csw_server': {
        'type': 'csw',
        'active': True,
        'keywords': ['csw', 'catalog'],
        'formats': ['csw']
    },
    'ogc_server': {
        'type': 'ogc',
        'active': True,
        'keywords': ['ows', 'geoserver', 'mapserver', 'ogc'],
        'formats': ['wfs', 'wcs', 'wms', 'wmts']
    },
    'table': {
        'type': 'table',
        'active': True,
        'keywords': ['xls', 'csv'],
        'formats': ['csv', 'xls', 'xlsx', 'tsv']
    },
    'xml': {
        'type': 'xml',
        'active': True,
        'keywords': ['xml', 'iso', 'gmd', 'inspire'],
        'formats': ['xml']
    },
}

# CKAN Api routes
OGC2CKAN_CKAN_API_ROUTES = {
    'create_ckan_dataset': '/api/3/action/package_create',
    'update_ckan_dataset': '/api/3/action/package_update',
    'create_ckan_resource': '/api/3/action/resource_create',
    'update_ckan_resource': '/api/3/action/resource_update',
    'create_ckan_resource_view': '/api/3/action/resource_view_create',
    'create_ckan_resource_dictionary': '/api/3/action/resource_dictionary_create',
    'get_ckan_datasets_list': '/api/3/action/package_search?fl={fields}&rows={rows}&include_private={include_private}',
    'get_ckan_datasets_list_paginate': '/api/3/action/package_search?fl={fields}&rows={rows}&start={start}&include_private={include_private}',
    'get_ckan_dataset_info': '/api/3/action/package_search?q={field}:"{field_value}"',
}

# CKANInfo class default configuration
OGC2CKAN_CKANINFO_CONFIG = {
    'ckan_site_url': 'http://localhost:5000',
    'pycsw_site_url': 'http://localhost:8000',
    'authorization_key': None,
    'dataset_multilang': False,
    'default_license': 'http://creativecommons.org/licenses/by/4.0/',
    'default_license_id': 'cc-by',
    'parallelization': False,
    'ssl_unverified_mode': False,
    'dir3_url': 'http://datos.gob.es/es/recurso/sector-publico/org/Organismo',
    'ckan_dataset_schema': 'geodcatap-eu',
    'metadata_distributions': False,
    'ckan_fields_json': 'geodcatap.json'
}

# DBDsn class default configuration
OGC2CKAN_DBDSN_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'database': 'database',
    'user': 'user',
    'password': 'password'
}

# Metadata multilang fields avalaibles, schema: geodcatap_es
OGC2CKAN_MD_MULTILANG_FIELDS = {
    'title': 'title_translated',
    'notes': 'notes_translated',
    'provenance': 'provenance',
    'purpose': 'purpose',
    'version_notes': 'version_notes'
}

# Default DCAT metadata configuration
OGC2CKAN_HARVESTER_MD_CONFIG = {
    'access_rights': 'http://inspire.ec.europa.eu/metadata-codelist/LimitationsOnPublicAccess/noLimitations',
    'conformance': [
        'http://inspire.ec.europa.eu/documents/inspire-metadata-regulation','http://inspire.ec.europa.eu/documents/commission-regulation-eu-no-13122014-10-december-2014-amending-regulation-eu-no-10892010-0'
    ],
    'author': 'ckan-ogc',
    'author_email': 'admin@localhost',
    'author_url': 'http://localhost:5000/organization/test',
    'author_uri': 'http://localhost:5000/organization/test',
    'contact_name': 'ckan-ogc',
    'contact_email': 'admin@localhost',
    'contact_url': 'http://localhost:5000/organization/test',
    'contact_uri': 'http://localhost:5000/organization/test',
    'dcat_type': {
        'series': 'http://inspire.ec.europa.eu/metadata-codelist/ResourceType/series',
        'dataset': 'http://inspire.ec.europa.eu/metadata-codelist/ResourceType/dataset',
        'service': 'http://inspire.ec.europa.eu/metadata-codelist/ResourceType/service',
        'default': 'http://inspire.ec.europa.eu/metadata-codelist/ResourceType/dataset',
    },
    'encoding': 'UTF-8',
    'frequency' : 'http://publications.europa.eu/resource/authority/frequency/UNKNOWN',
    'inspireid_theme': 'HB',
    'language': 'http://publications.europa.eu/resource/authority/language/ENG',
    'license': 'http://creativecommons.org/licenses/by/4.0/',
    'license_id': 'cc-by',
    'lineage_process_steps': 'ckan-ogc lineage process steps.',
    'maintainer': 'ckan-ogc',
    'maintainer_email': 'admin@localhost',
    'maintainer_url': 'http://localhost:5000/organization/test',
    'maintainer_uri': 'http://localhost:5000/organization/test',
    'metadata_profile': [
        "http://semiceu.github.io/GeoDCAT-AP/releases/2.0.0","http://inspire.ec.europa.eu/document-tags/metadata"
    ],
    'provenance': 'ckan-ogc provenance statement.',
    'publisher_name': 'ckan-ogc',
    'publisher_email': 'admin@localhost',
    'publisher_url': 'http://localhost:5000/organization/test',
    'publisher_identifier': 'http://localhost:5000/organization/test',
    'publisher_uri': 'http://localhost:5000/organization/test',
    'publisher_type': 'http://purl.org/adms/publishertype/NonProfitOrganisation',
    'reference_system': 'http://www.opengis.net/def/crs/EPSG/0/4258',
    'representation_type': {
        'wfs': 'http://inspire.ec.europa.eu/metadata-codelist/SpatialRepresentationType/vector',
        'wcs': 'http://inspire.ec.europa.eu/metadata-codelist/SpatialRepresentationType/grid',
        'default': 'http://inspire.ec.europa.eu/metadata-codelist/SpatialRepresentationType/vector',
        'grid': 'http://inspire.ec.europa.eu/metadata-codelist/SpatialRepresentationType/grid',
        'vector': 'http://inspire.ec.europa.eu/metadata-codelist/SpatialRepresentationType/vector',
        'textTable': 'http://inspire.ec.europa.eu/metadata-codelist/SpatialRepresentationType/textTable',
        'tin': 'http://inspire.ec.europa.eu/metadata-codelist/SpatialRepresentationType/tin',
        'stereoModel': 'http://inspire.ec.europa.eu/metadata-codelist/SpatialRepresentationType/stereoModel',
        'video': 'http://inspire.ec.europa.eu/metadata-codelist/SpatialRepresentationType/video',
    },
    'spatial': None,
    'spatial_uri': 'http://datos.gob.es/recurso/sector-publico/territorio/Pais/España',
    'temporal_start': None,
    'temporal_end': None,
    'theme': 'http://inspire.ec.europa.eu/theme/hb',
    'theme_es': 'http://datos.gob.es/kos/sector-publico/sector/medio-ambiente',
    'theme_eu': 'http://publications.europa.eu/resource/authority/data-theme/ENVI',
    'topic': 'http://inspire.ec.europa.eu/metadata-codelist/TopicCategory/biota',
    'valid': None
}

OGC2CKAN_MD_FORMATS = {
    'api': ('API', 'http://www.iana.org/assignments/media-types/application/vnd.api+json', None, 'Application Programming Interface'),
    'api feature': ('OGCFeat', 'http://www.opengis.net/def/interface/ogcapi-features', 'http://www.opengeospatial.org/standards/features', 'OGC API - Features'),
    'wms': ('WMS', 'http://www.opengis.net/def/serviceType/ogc/wms', 'http://www.opengeospatial.org/standards/wms', 'Web Map Service'),
    'zip': ('ZIP', 'http://www.iana.org/assignments/media-types/application/zip', 'http://www.iso.org/standard/60101.html', 'ZIP File'),
    'rar': ('RAR', 'http://www.iana.org/assignments/media-types/application/vnd.rar', 'http://www.rarlab.com/technote.htm', 'RAR File'),
    'wfs': ('WFS', 'http://www.opengis.net/def/serviceType/ogc/wfs', 'http://www.opengeospatial.org/standards/wfs', 'Web Feature Service'),
    'wcs': ('WCS', 'http://www.opengis.net/def/serviceType/ogc/wcs', 'http://www.opengeospatial.org/standards/wcs', 'Web Coverage Service'),
    'tms': ('TMS', 'http://wiki.osgeo.org/wiki/Tile_Map_Service_Specification', 'http://www.opengeospatial.org/standards/tms', 'Tile Map Service'),
    'wmts': ('WMTS', 'http://www.opengis.net/def/serviceType/ogc/wmts', 'http://www.opengeospatial.org/standards/wmts', 'Web Map Tile Service'),
    'kml': ('KML', 'http://www.iana.org/assignments/media-types/application/vnd.google-earth.kml+xml', 'http://www.opengeospatial.org/standards/kml', 'Keyhole Markup Language'),
    'kmz': ('KMZ', 'http://www.iana.org/assignments/media-types/application/vnd.google-earth.kmz+xml', 'http://www.opengeospatial.org/standards/kml', 'Compressed Keyhole Markup Language'),
    'gml': ('GML', 'http://www.iana.org/assignments/media-types/application/gml+xml', 'http://www.opengeospatial.org/standards/gml', 'Geography Markup Language'),
    'geojson': ('GeoJSON', 'http://www.iana.org/assignments/media-types/application/geo+json', 'http://www.rfc-editor.org/rfc/rfc7946', 'GeoJSON'),
    'json': ('JSON', 'http://www.iana.org/assignments/media-types/application/json', 'http://www.ecma-international.org/publications/standards/Ecma-404.htm', 'JavaScript Object Notation'),
    'atom': ('ATOM', 'http://www.iana.org/assignments/media-types/application/atom+xml', 'http://validator.w3.org/feed/docs/atom.html', 'Atom Syndication Format'),
    'xml': ('XML', 'http://www.iana.org/assignments/media-types/application/xml', 'http://www.w3.org/TR/REC-xml/', 'Extensible Markup Language'),
    'arcgis_rest': ('ESRI Rest', None, None, 'ESRI Rest Service'),
    'shp': ('SHP', 'http://www.iana.org/assignments/media-types/application/vnd.shp', 'http://www.esri.com/library/whitepapers/pdfs/shapefile.pdf', 'ESRI Shapefile'),
    'shapefile': ('SHP', 'http://www.iana.org/assignments/media-types/application/vnd.shp', 'http://www.esri.com/library/whitepapers/pdfs/shapefile.pdf', 'ESRI Shapefile'),
    'esri': ('SHP', 'http://www.iana.org/assignments/media-types/application/vnd.shp', 'http://www.esri.com/library/whitepapers/pdfs/shapefile.pdf', 'ESRI Shapefile'),
    'html': ('HTML', 'http://www.iana.org/assignments/media-types/text/html', 'http://www.w3.org/TR/2011/WD-html5-20110405/', 'HyperText Markup Language'),
    'visor': ('HTML', 'http://www.iana.org/assignments/media-types/text/html', 'http://www.w3.org/TR/2011/WD-html5-20110405/', 'Map Viewer'),
    'enlace': ('HTML', 'http://www.iana.org/assignments/media-types/text/html', 'http://www.w3.org/TR/2011/WD-html5-20110405/', 'Map Viewer'),
    'pdf': ('PDF', 'http://www.iana.org/assignments/media-types/application/pdf', 'http://www.iso.org/standard/75839.html', 'Portable Document Format'),
    'csv': ('CSV', 'http://www.iana.org/assignments/media-types/text/csv', 'http://www.rfc-editor.org/rfc/rfc4180', 'Comma-Separated Values'),
    'netcdf': ('NetCDF', 'http://www.iana.org/assignments/media-types/text/csv', 'http://www.opengeospatial.org/standards/netcdf', 'Network Common Data Form'),
    'csw': ('CSW', 'http://www.opengis.net/def/serviceType/ogc/csw', 'http://www.opengeospatial.org/standards/cat', 'Catalog Service for the Web'),
    'geodcatap': ('RDF', 'http://www.iana.org/assignments/media-types/application/rdf+xml', 'http://semiceu.github.io/GeoDCAT-AP/releases/2.0.0/', 'GeoDCAT-AP 2.0 Metadata')
    ,
    'inspire': ('XML', 'http://www.iana.org/assignments/media-types/application/xml', ['http://inspire.ec.europa.eu/documents/inspire-metadata-regulation','http://inspire.ec.europa.eu/documents/commission-regulation-eu-no-13122014-10-december-2014-amending-regulation-eu-no-10892010-0', 'http://www.isotc211.org/2005/gmd/'], 'INSPIRE ISO 19139 Metadata')
}

OGC2CKAN_ISO_MD_ELEMENTS = {
    'lineage_source': 'gmd:dataQualityInfo/gmd:DQ_DataQuality/gmd:lineage/gmd:LI_Lineage/gmd:source/gmd:LI_Source/gmd:description/gco:CharacterString',
    'lineage_process_steps': 'gmd:dataQualityInfo/gmd:DQ_DataQuality/gmd:lineage/gmd:LI_Lineage/gmd:processStep'
    
}

# loose definition of BCP47-like strings
BCP_47_LANGUAGE = u'^[a-z]{2,8}(-[0-9a-zA-Z]{1,8})*$'