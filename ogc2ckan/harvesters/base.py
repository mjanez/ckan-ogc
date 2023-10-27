# inbuilt libraries
import inspect
import uuid
import re
import unicodedata
from typing import Any
import logging
from datetime import datetime
import html
import os
import string

# third-party libraries
from geojson import Polygon, dumps
from pyproj import Transformer
import pandas as pd
from bs4 import BeautifulSoup
from owslib.iso import MD_Keywords
from owslib.namespaces import Namespaces
from owslib import util

# custom classes
from controller import ckan_management
from model.custom_organization import CustomOrganization
from controller.mapping import get_mapping_value
from config.ogc2ckan_config import load_yaml, get_log_module
from mappings.default_ogc2ckan_config import OGC2CKAN_PATHS_CONFIG, OGC2CKAN_HARVESTER_MD_CONFIG, OGC2CKAN_CKANINFO_CONFIG, OGC2CKAN_MD_FORMATS, OGC2CKAN_ISO_MD_ELEMENTS, OGC2CKAN_MD_MULTILANG_FIELDS, BCP_47_LANGUAGE
from harvesters.harvesters import get_harvester_class


log_module = get_log_module(os.path.abspath(__file__))

class DCATInfo:
    '''Represents the information of a dataset in DCAT format.

    This class is used to store the information of a dataset in DCAT format.
    The instance attributes correspond to the properties of a dataset in DCAT format.

    Args:
        default_dcat_info: Keyword arguments representing the properties of a dataset in DCAT format.
    '''
    def __init__(self, default_dcat_info):
        for key, value in default_dcat_info.items():
            setattr(self, key, value)


class Harvester:
    '''
    A base class for harvesting datasets from different types of servers.

    Args:
        app_dir (str): The directory path to the application.
        url (str): The URL of the server.
        name (str): The name of the harvester.
        groups (list): A list of group names to harvest.
        active (bool): Whether the harvester is active or not.
        organization (str): The name of the organization that owns the harvester.
        type (str): The type of harvester.
        custom_organization_active (bool): Custom organization info active.
        custom_organization_mapping_file (str): Name of the YAML.
        private_datasets (bool): Whether the harvester should harvest private datasets or not.
        default_keywords (list): A list of default keywords to add to harvested datasets.
        default_inspire_info (dict): A dictionary of default INSPIRE metadata to add to harvested datasets.
        **default_dcat_info: A dictionary of default DCAT metadata to add to harvested datasets.

    Attributes:
        url (str): The URL of the server.
        name (str): The name of the harvester.
        groups (list): A list of group names to harvest.
        active (bool): Whether the harvester is active or not.
        organization (str): The name of the organization that owns the harvester.
        type (str): The type of harvester.
        custom_organization_active (bool): Custom organization info active.
        custom_organization_mapping_file (str): Name of the YAML.
        private_datasets (bool): Whether the harvester should harvest private datasets or not.
        default_keywords (list): A list of default keywords to add to harvested datasets.
        default_inspire_info (dict): A dictionary of default INSPIRE metadata to add to harvested datasets.
        datasets (list): A list of Dataset objects.

    Methods:
        from_harvest_server(cls, harvest_server): Returns a new Harvester object based on a HarvestServer object.
        _create_harvester_from_server(harvest_server, harvester_class): Creates a new Harvester object from a HarvestServer object and a Harvester class.
        create_datasets(self, ckan_info): Creates new datasets in a CKAN instance based on the harvested datasets.
        get_all_datasets(self, ckan_info): Gets all datasets from the server.

    '''
    def __init__(self, app_dir, url, name, groups, active, organization, type, custom_organization_active, custom_organization_mapping_file, private_datasets, default_keywords, default_inspire_info, ckan_name_not_uuid, default_dcat_info):
        self.app_dir = app_dir
        self.url = url
        self.name = name
        self.groups = groups
        self.active = active
        self.organization = organization
        self.type = type
        self.custom_organization_active = custom_organization_active
        self.custom_organization_mapping_file = custom_organization_mapping_file
        self.private_datasets = private_datasets
        self.default_dcat_info = DCATInfo(default_dcat_info) if default_dcat_info else None
        self.default_keywords = default_keywords
        self.default_inspire_info = default_inspire_info
        self.ckan_name_not_uuid = ckan_name_not_uuid or False
        self.datasets = []
        self.datadictionaries = []
        self.ckan_dataset_count = 0
        self.source_dataset_count = 0
        self.ckan_dataset_errors = []
        self.ckan_dictionaries_count = 0
        self.source_dictionaries_count = 0
        self.ckan_dictionaries_errors = []
        # Additional custom organization info (ckan-harvester/src/ckan/ogc_ckan/custom/mappings)
        self.custom_organization_info = CustomOrganization(self) if custom_organization_active else None
        default_localized_strings_file = f"{self.app_dir}/{OGC2CKAN_PATHS_CONFIG['default_mappings_folder']}/{OGC2CKAN_PATHS_CONFIG['default_localized_strings_file']}"
        # Localized default info
        yaml_dict = load_yaml(default_localized_strings_file)
        self.default_language = self.get_default_dcat_info_attribute("language")
        self.localized_strings_dict = self._get_localized_dict(yaml_dict, self.default_language)
        self.ows_namespaces = self._ows_get_namespaces()


    @classmethod
    def from_harvest_server(cls, harvest_server, app_dir):
        """Create a new Harvester object from a HarvestServer object.

        Args:
            cls (type): The class of the Harvester object to create.
            harvest_server (HarvestServer): The HarvestServer object to use for creating the Harvester object.
            app_dir (str): The directory path to the application.

        Returns:
            Harvester: A new Harvester object of the appropriate class.

        Raises:
            TypeError: If the Harvester class cannot be determined from the harvest server type.
        """
        # Get the Harvester class for the harvest server type
        harvester_class = get_harvester_class(harvest_server.type, harvest_server.url)
        harvester_args = inspect.signature(harvester_class).parameters.keys()
        harvester_kwargs = {k: v for k, v in harvest_server.__dict__.items() if k in harvester_args}

        # Add app_dir to harvester_kwargs
        harvester_kwargs['app_dir'] = app_dir

        # Create a new Harvester object of the appropriate class
        harvester = harvester_class(**harvester_kwargs)

        return harvester

    def create_datasets(self, ckan_info):
        '''
        Create datasets if you are only interested in creating new datasets

        :self harvester: Harvester object
        :param ckan_info: CKAN Parameters from config.yaml

        :return: CSW Records and CKAN New records counters and Datasets object
        '''

        # Get all datasets
        self.get_datasets(ckan_info)

        if hasattr(self, 'workspaces') and self.workspaces:
            logging.info(f"{log_module}:{self.name} ({self.type.upper()}) server OGC workspaces selected: {', '.join([w.upper() for w in self.workspaces])}")

            # Create datasets using ckan_management
            self.ckan_dataset_count, self.source_dataset_count, self.ckan_dataset_errors = ckan_management.create_ckan_datasets(ckan_info.ckan_site_url, ckan_info.authorization_key, self.datasets, ckan_info.dataset_multilang, ckan_info.ssl_unverified_mode, self.workspaces)

        else:
            # Create datasets using ckan_management
            self.ckan_dataset_count, self.source_dataset_count, self.ckan_dataset_errors = ckan_management.create_ckan_datasets(ckan_info.ckan_site_url, ckan_info.authorization_key, self.datasets, ckan_info.dataset_multilang, ckan_info.ssl_unverified_mode)

        # Create data dictionaries using ckan_management
        if self.datadictionaries:
            self.ckan_dictionaries_count, self.source_dictionaries_count, self.ckan_dictionaries_errors = ckan_management.create_ckan_datadictionaries(ckan_info.ckan_site_url, ckan_info.authorization_key, self.datadictionaries, ckan_info.ssl_unverified_mode)

    def get_dataset_common_elements(self, record: str, ckan_dataset_schema: str) -> tuple:
        """
        Generates common elements for harvesting a dataset.

        Args:
            record (str): The record to harvest.
            ckan_dataset_schema (str): The CKAN dataset schema defined in config.yaml to retrieve.

        Returns:
            tuple: A tuple containing the following common elements:
                - dataset (object): The CKAN dataset class based on the schema.
                - distribution (object): The CKAN distribution class based on the schema.
                - uuid_identifier (str): A UUID identifier for the dataset.
                - ckan_name (str): The CKAN name for the dataset, based on the UUID or the identifier and organization.
                - ckan_groups (list): A list of CKAN groups for the dataset.
                - inspire_id (str): The INSPIRE ID for the dataset.
        """
        # Get CKAN classes based on schema
        from ckan_datasets.ckan_datasets import CKAN_DATASET_SCHEMAS

        # Find the schema in the schema dictionary
        schema = CKAN_DATASET_SCHEMAS.get(ckan_dataset_schema, CKAN_DATASET_SCHEMAS["default"])

        # Return the corresponding classes
        dataset = schema["dataset"]
        distribution = schema["distribution"]
        datadictionary = schema ["datadictionary"]
        datadictionaryfield = schema ["datadictionaryfield"]

        uuid_identifier = self._create_uuid_identifier()

        # Use ckan_name instead of uuid_identifier if required
        if self.ckan_name_not_uuid:
            ckan_name = self._get_ckan_name(record, self.organization)
            uuid_identifier = ckan_name
        else:
            ckan_name = uuid_identifier

        ckan_groups = [{'name': g.lower()} for g in self.groups or []]

        # Create inspireId
        inspire_id = ".".join(filter(None,[self.default_inspire_info['inspireid_nutscode'], self.default_inspire_info['inspireid_theme'], record.replace(':', '.').replace(' ', ''), self.default_inspire_info['inspireid_versionid']])).upper()

        return dataset, distribution, datadictionary, datadictionaryfield, uuid_identifier, ckan_name, ckan_groups, inspire_id

    def get_custom_default_metadata(self, dataset_id: str, dict_property: str = 'dataset_id') -> Any:
        """
        Get the custom default metadata for a given dataset ID.

        Args:
            dataset_id (str): The ID of the dataset.
            dict_property (str): The property to retrieve from the mapping dictionary.

        Returns:
            Any: The value of the specified property in the mapping dictionary, or None if the property is not found.
        """
        try:
            mapping = self.custom_organization_info.find_mapping_value(dataset_id, dict_property)
        except KeyError:
            mapping = None

        if mapping is None:
            mapping = self.custom_organization_info.find_similar_mapping_value(dataset_id, dict_property)

        if mapping is None:
            # If 'dataset_id' property is not found, try 'dataset_group_id'
            try:
                mapping = self.custom_organization_info.find_mapping_value(
                    dataset_id,
                    'dataset_group_id'
                    )
            except KeyError:
                logging.error(f"{log_module}:Dataset: '{dataset_id}' does not have info in 'custom_organization_mapping_file'. Add it or deactivate `custom_organization_active`")
                mapping = None

        return mapping

    def get_custom_metadata_value(self, custom_metadata, key, default=None):
        """
        Get the value associated with a key from custom metadata or a default value.

        Args:
            custom_metadata (dict): Custom metadata dictionary.
            key (str): The key to retrieve the value for.
            default: The default value to return if the key is not found.

        Returns:
            The value associated with the key in custom_metadata, or the default value if the key is not present in custom_metadata.

        If the key is not found in custom_metadata, this function checks if the
        'self.default_dcat_info' attribute has a value for the key and returns it.
        If neither source has the key, the default value is returned.

        Example:
            To retrieve the 'lineage_source' from custom_metadata with a default
            value of None:
            >>> value = get_custom_metadata_value(custom_metadata, 'lineage_source')
        """
        if custom_metadata is not None and key in custom_metadata:
            return custom_metadata[key]
        elif hasattr(self, 'default_dcat_info') and hasattr(self.default_dcat_info, key):
            return getattr(self.default_dcat_info, key)
        else:
            return default

    def get_default_dcat_info_attribute(self, field_name):
        """
        Tries to retrieve the value of a field_name from self.default_dcat_info. If the field does not exist, returns the default value from OGC2CKAN_HARVESTER_MD_CONFIG.

        Args:
            field_name (str): The name of the field to retrieve.

        Returns:
            The value of the specified field from self.default_dcat_info, or the default value from OGC2CKAN_HARVESTER_MD_CONFIG if the field does not exist.
        """
        try:
            return getattr(self.default_dcat_info, field_name, OGC2CKAN_HARVESTER_MD_CONFIG[field_name])

        except AttributeError:
            logging.error(f"{log_module}:Field '{field_name}' does not exist in 'default_dcat_info' section at 'config.yaml'")
            return None

        except KeyError as e:
            logging.error(f"{log_module}:Field '{field_name}' does not exist in 'OGC2CKAN_HARVESTER_MD_CONFIG' at: 'ckan-ogc/ogc2ckan/mappings/default_ogc2ckan_config.py': {e}")
            return None

    # OWS functions
    def ows_update_metadata_sections(self, layer_info):
        def get_first_element_from_list(lst):
            return lst[0] if isinstance(lst, list) and lst else None

        layer_info.identification = get_first_element_from_list(layer_info.identification) if layer_info.identification else None
        layer_info.distributor = get_first_element_from_list(layer_info.distribution.distributor) if layer_info.distribution and layer_info.distribution.distributor else None
        layer_info.distribution = layer_info.distribution.online if hasattr(layer_info.distribution, 'online') else None
        layer_info.contact = get_first_element_from_list(layer_info.contact) if layer_info.contact else None
        layer_info.identification.publisher = get_first_element_from_list(layer_info.identification.publisher) if layer_info.identification and layer_info.identification.publisher else None
        layer_info.topiccategory = get_first_element_from_list(layer_info.identification.topiccategory) if layer_info.identification and layer_info.identification.topiccategory else None
        layer_info.uricode = get_first_element_from_list(layer_info.identification.uricode) if layer_info.identification and layer_info.identification.uricode else None

        try:
            if not layer_info.identification:
                raise AttributeError("identification")
            if not layer_info.distribution:
                raise AttributeError("distribution")
            if not layer_info.contact:
                raise AttributeError("contact")
            if not layer_info.topiccategory:
                raise AttributeError("topiccategory")
            if not layer_info.uricode:
                raise AttributeError("uricode")

        except AttributeError as e:
            logging.error(f"{log_module}:An error occurred in ows_update_metadata_sections: {e}")
            setattr(layer_info, e.args[0], None)

    def ows_get_metadata_not_owslib(self, layer_info):
        """Gets metadata values that are not retrieved by OWSLib from an MD_Metadata object.

        Args:
            layer_info (object): Object containing metadata information.

        Returns:
            dict: Dictionary containing metadata values.
        """
        return {
            "lineage_source": self._ows_findall_metadata_elements(layer_info, self.ows_namespaces, OGC2CKAN_ISO_MD_ELEMENTS['lineage_source'])
        }

    def ows_get_keywords(self, dataset, keywords):
        """
        Gets the keywords from a OWS MD_Metadata record.

        Args:
            dataset: The CKAN Dataset to ingest.
            keywords: The OWS MD_Metadata Record object list of keywords to get.

        Returns:
            list: The keywords.
        """
        keywords_list = dataset.keywords
        keywords_thesaurus_list = dataset.keywords_thesaurus
        themes_set = set(dataset.theme)
        keywords_uri_set = set(dataset.keywords_uri)
        topic = dataset.topic
        
        for keyword in keywords:
            for k in keyword.keywords:
                url = getattr(k, "url", None)
                if url:
                    last_part = url.split("/")[-1]
                    keywords_list.append({'name': last_part.lower()})
                    keywords_uri_set.add(url)
                    if '/theme/' in url:
                        # INSPIRE Theme
                        themes_set.add(url)
            
            if keyword.thesaurus:
                url = keyword.thesaurus.get('url')
                title = keyword.thesaurus.get('title')
                date = keyword.thesaurus.get('date')
                datetype = keyword.thesaurus.get('datetype')
                if url:
                    keywords_thesaurus_list.append({'title': title, 'date': date, 'datetype': datetype, 'url': url})
        
        if topic:
            themes_set.add(get_mapping_value(topic, 'theme', 'id', 'topic_category'))

        # Unique values
        keywords_list = [dict(t) for t in {tuple(d.items()) for d in keywords_list}]

        # Set themes/keywords/keywords_uri
        self._set_themes(dataset, list(themes_set))
        dataset.set_keywords(keywords_list)
        self._set_keywords_uri(dataset, list(keywords_uri_set))

    def ows_set_metadata_dates(self, dataset, record_id):
        """
        Sets the metadata dates for a CKAN dataset from an ISO metadata record.

        Args:
            dataset: The CKAN dataset to set the metadata dates for.
            record_id: The ISO metadata record to get the metadata dates from.
        """
        # Default values
        issued_date = datetime.now().strftime('%Y-%m-%d')
        created_date = '1900-01-01'
        modified_date = issued_date

        for date in record_id.date:
            if date.type == "creation":
                created_date = self._normalize_date(date.date)
            elif date.type == "publication":
                issued_date = self._normalize_date(date.date)
            elif date.type == "revision":
                modified_date = self._normalize_date(date.date)

        dataset.set_issued(issued_date)
        dataset.set_created(created_date)
        dataset.set_modified(modified_date)

        return issued_date, modified_date

    @staticmethod
    def _ows_get_namespaces():
        n = Namespaces()
        ns = n.get_namespaces(["gco", "gfc", "gmd", "gmi", "gml", "gml32", "gmx", "gts", "srv", "xlink"])
        ns[None] = n.get_namespace("gmd")
        
        return ns

    @staticmethod
    def _ows_findall_metadata_elements(layer_info, namespaces, tag):
        """
        Finds all elements in an ISO metadata record (md).

        Args:
            layer_info: The MD_Metadata object that contains ISO metadata record to search in.
            namespaces: The namespaces to use for the search.
            tag: The tag of the element to search for.

        Returns:
            The elements if founds, otherwise None.
        """
        results = []
        val = layer_info.md.findall(util.nspath_eval(tag, namespaces))

        for i in val:
            if hasattr(i, 'text'):
                i = util.testXMLValue(i)
            results.append(i)

        return results

    @staticmethod
    def _ows_find_metadata_element(layer_info, namespaces, tag):
        """
        Finds element in an ISO metadata record (md).

        Args:
            layer_info: The MD_Metadata object that contains ISO metadata record to search in.
            namespaces: The namespaces to use for the search.
            tag: The tag of the element to search for.

        Returns:
            The element if found, otherwise None.
        """
        val = layer_info.md.find(util.nspath_eval(tag, namespaces))
        if hasattr(val, 'text'):
            val = util.testXMLValue(val.text)
        return val

    @staticmethod
    def _ows_convert_keyword(keywords, iso2dict=False, theme="theme"):
        """
        Convert keywords to a standardized format.

        Args:
            keywords (list): The list of keywords to convert.
            iso2dict (bool, optional): Whether to convert ISO keywords to a dictionary format. Default is False.
            theme (str, optional): The theme/category for the keywords. Default is "theme".

        Returns:
            list or dict: The converted keywords in a standardized format. If `iso2dict` is True, returns a list of dictionaries.
                        Otherwise, returns a list of lists.
        """
        def convert_iso_keywords(keywords):
            _keywords = []
            for kw in keywords:
                if isinstance(kw, MD_Keywords):
                    _keywords.append([_kw.name for _kw in kw.keywords])
                else:
                    _keywords.append(kw)
            return _keywords

        if not iso2dict and keywords:
            return [
                {
                    "keywords": convert_iso_keywords(keywords),
                    "thesaurus": {"date": None, "datetype": None, "title": None},
                    "type": theme,
                }
            ]
        return convert_iso_keywords(keywords)

    @staticmethod
    def _create_harvester_from_server(harvest_server, harvester_class):
        harvester = harvester_class(
            url=harvest_server.url,
            name=harvest_server.name,
            groups=harvest_server.groups,
            active=harvest_server.active,
            organization=harvest_server.organization,
            type=harvest_server.type,
            custom_organization_active=harvest_server.custom_organization_active,
            custom_organization_mapping_file=harvest_server.custom_organization_mapping_file,
            private_datasets=harvest_server.private_datasets,
            default_keywords=harvest_server.default_keywords,
            default_inspire_info=harvest_server.default_inspire_info,
            ckan_name_not_uuid=harvest_server.ckan_name_not_uuid,
            **harvest_server.default_dcat_info
        )

        return harvester

    @staticmethod
    def _get_localized_dict(yaml_dict, language):
        '''
        Get a dictionary of localized strings for a given language.

        Args:
            yaml_dict (dict): The dictionary of localized strings.
            language (str): The language code or URI.

        Returns:
            dict: A dictionary of localized strings for the specified language, or an empty dictionary if the language is not found.
        '''
        # Use URI or language code
        if isinstance(language, str) and language.startswith('http'):
            language = language.split('/')[-1].upper()
        else:
            language = language.upper()

        # Filter the dictionary for the desired language
        localized_dict = yaml_dict.get(language, {})

        return localized_dict

    @staticmethod
    def _create_uuid_identifier():
        uuid_identifier = str(uuid.uuid4())

        return uuid_identifier

    @staticmethod
    def _get_ckan_name(name, organization):
        # the name of a CKAN dataset, must be between 2 and 100 characters long and contain only lowercase
        # alphanumeric characters, - and _, e.g. 'warandpeace'
        normal = unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode('utf-8')
        ckan_name = re.sub(r'[^a-z0-9_-]', '_', normal.lower())[:100]
        if len(ckan_name) == 0:
            ckan_name = 'unnamed'
        elif ckan_name[0].isdigit():
            ckan_name = 'n' + ckan_name[1:]
        ckan_name = organization.lower() + '_' + ckan_name.lower()

        return ckan_name

    @staticmethod
    def _normalize_date(date):
        if isinstance(date, str):
            date_formats = ['%Y-%m-%d', '%d-%m-%Y']
            for date_format in date_formats:
                try:
                    date = datetime.strptime(date, date_format).strftime('%Y-%m-%d')
                    return date
                except ValueError:
                    pass
            return None
        elif isinstance(date, datetime):
            date = date.strftime('%Y-%m-%d')
        else:
            return None
        return date

    @staticmethod
    def _set_min_max_coordinates(dataset, minx, maxx, miny, maxy):
        '''
        '''
        #print(str(minx) + ' ' + str(maxx) + ' ' + str(miny) + ' ' + str(maxy))
        #transformer = pyproj.Transformer.from_crs('epsg:3857', 'epsg:4326')
        #print('transform')
        #print(transformer.transform(minx, miny))
        dataset_bb = dumps(Polygon([[
            (minx, miny),
            (maxx, miny),
            (maxx, maxy),
            (minx, maxy),
            (minx, miny)
        ]]))

        dataset.set_spatial(dataset_bb)

    @staticmethod
    def _set_conformance(dataset, code=None, epsg_text=None):
        """Sets the conformance information of the dataset.

            Args:
                dataset: The dataset to set the conformance information for.
                code: The conformance code to add to the dataset.
                epsg_text: The EPSG text to use for the reference system.
        """
        # Add INSPIRE conformance. http://inspire.ec.europa.eu/documents/directive-20072ec-european-parliament-and-council-14-march-2007-establishing
        dataset.set_conformance(OGC2CKAN_HARVESTER_MD_CONFIG['conformance'])
        # Add coordinate reference system for layers with coordinates
        if dataset.spatial and code is None:
            # Check reference_system
            epsg_text_str = str(epsg_text) if epsg_text is not None else ''
            epsg_text_str = str(epsg_text) if epsg_text is not None else ''
            epsg = re.findall(r'(?i)(epsg\S[^\s]+)', epsg_text_str)[0].replace('EPSG:', '') if 'epsg' in epsg_text_str.lower() else None

            if epsg is not None:
                epsg = re.sub(r'\D', '', epsg)

            if epsg and len(epsg) <= 5 and epsg.isdigit():
                dataset.set_reference_system(f'http://www.opengis.net/def/crs/EPSG/0/{epsg}')
            elif 'etrs89' in (epsg_text or '').lower():
                dataset.set_reference_system(OGC2CKAN_HARVESTER_MD_CONFIG['reference_system'])
        elif code is not None:
            dataset.conformance.append(code)

    @staticmethod
    def _get_ckan_format(dist_info):
        """Get the CKAN format information for a distribution.

        Args:
            dist_info (dict): A dictionary containing information about the distribution.

        Returns:
            tuple: A tuple containing the CKAN format information for the distribution.
        """
        if 'format' in dist_info:
            informat = dist_info['format'].lower()
        else:
            try:
                informat = ''.join(str(value) for value in dist_info.values()).lower()
                informat = next((key for key in OGC2CKAN_MD_FORMATS if key.lower() in informat), dist_info.get('url', '').lower())
            except:
                informat = dist_info['url'].lower()

        return OGC2CKAN_MD_FORMATS.get(informat, (None, None, None, None))

    @staticmethod
    def _get_distribution_info(format_type, url, description, license, license_id, rights, language):
        """Create a dictionary with distribution information.

        Args:
            format_type (str): The format type of the distribution.
            url (str): The URL of the distribution.
            description (str): The description of the distribution.
            license (str): The license of the distribution.
            license_id (str): The license ID of the distribution.
            rights (str): The rights of the distribution.
            language (str): The language of the distribution.

        Returns:
            dict: A dictionary with the distribution information.
        """
        return {
            "format": format_type,
            "protocol": None,
            "url": url,
            "description": description,
            "license": license,
            "license_id": license_id,
            "rights": rights,
            "language": language
        }

    @staticmethod
    def _set_themes(dataset, themes):
        modified_themes = [
            theme.replace('https:', 'http:') if "http" in theme or "inspire" in theme
            else "http://inspire.ec.europa.eu/theme/" + theme.lower()
            for theme in themes
        ]

        dataset.set_theme(modified_themes)

    @staticmethod
    def _set_themes_es(dataset, themes_es):
        eu_themes = []
        es_themes = [theme.replace('https:', 'http:') for theme in themes_es]
        for theme in es_themes:
            theme_eu = get_mapping_value(theme, 'theme_es', 'theme_eu')
            eu_themes.append(theme_eu)

        dataset.set_theme_es(es_themes)
        dataset.set_theme_eu(eu_themes)

    @staticmethod
    def _set_themes_eu(dataset, theme_eu):
        themes = [theme.replace('https:', 'http:') for theme in theme_eu]

        dataset.set_theme_eu(themes)

    @staticmethod
    def _set_keywords_uri(dataset, keywords_uri):
            keywords_uri= [w.replace(',', ';') for w in keywords_uri]
            dataset.set_keywords_uri(keywords_uri)

    @staticmethod
    def _get_dir3_uri(dir3_soup: BeautifulSoup, uri_default: str, organization: str = None) -> str:
        """
        Gets the organization URI based on DIR3 identifiers.

        Args:
            dir3_soup: BeautifulSoup data of datos.gob.es.
            uri_default: Default URI if nothing is found, used config.yaml.
            organization: Organization string from record contact object.

        Returns:
            The organization URI.
        """
        if organization is not None and '.' in organization:
            organization = organization.split('.')[0]
        try:
            dir3_df = pd.read_html(str(dir3_soup.find('table', class_='table table-bordered table-condensed table-hover')))[0]
            # Use regex to extract the organization name from the domain
            organization_name = re.search(r'[^.]+', organization).group()
            # Use str.extract to get the matching groups from the 'Organismo' column
            dir3_df = dir3_df[dir3_df['Organismo'].str.extract(fr'({organization_name})', flags=re.IGNORECASE, expand=False).notna()]
            uri_value = dir3_df.iloc[0]['URI']

            if not isinstance(uri_value, str):
                uri_value = uri_default
        except:
            uri_value = uri_default

        return uri_value

    @staticmethod
    def _set_ckan_groups(groups):
        # Normalize groups for CKAN
        # if groups is a string, convert it to a list
        if isinstance(groups, str):
            ckan_groups = [{'name': g.lower().replace(" ", "-").strip()} for g in groups.split(',') or []]
        elif isinstance(groups, list):
            ckan_groups = [{'name': g.lower().replace(" ", "-").strip()} for g in groups or []]

        else:
            ckan_groups = []

        return ckan_groups

    @staticmethod
    def _unescape_string(text):
        if text is not None:
            try:
                # Unescape HTML entities and fix encoding issues
                fixed_text = html.unescape(text).encode('utf-8').decode('utf-8')
            
                return fixed_text
            
            except Exception as e:
                logging.error(f"{log_module}:Error fixing encoding: {e}")
                return text

    def get_ckan_distribution(self, distribution, record, dist_info):
        format_type, media_type, conformance, name = self._get_ckan_format(dist_info)
        format_type = format_type or "Unknown"
        name = name or f"{format_type} distribution of {record}"
        media_type = media_type or None
        conformance = conformance or None
        distribution= distribution(
            url=dist_info['url'],
            name=name,
            format=format_type,
            media_type=media_type,
            description=dist_info['description'],
            license=dist_info['license'],
            license_id=dist_info['license_id'],
            rights=dist_info['rights'],
            language=dist_info['language'],
            conformance=conformance
        )
        return distribution

    def set_metadata_distributions(self, ckan_info, dataset, distribution, record):
        if ckan_info.metadata_distributions == True or ckan_info.metadata_distributions == "True":
            # Add GeoDCAT-AP Metadata distribution
            dist_info = self._get_distribution_info("geodcatap", ckan_info.ckan_site_url + "/dataset/" + dataset.name + ".rdf", self.localized_strings_dict['distributions']['geodcatap'], ckan_info.default_license, ckan_info.default_license_id, dataset.access_rights, dataset.language)
            dataset.add_distribution(self.get_ckan_distribution(distribution, record, dist_info))

            # Add INSPIRE ISO19139 Metadata distribution
            # http://localhost:8000/csw?service=CSW&version=2.0.2&request=GetRecordById&id=spamitma_hermes_service_0_rtig_pu&elementsetname=full&outputSchema=http://www.isotc211.org/2005/gmd
            # http://localhost:8000/csw?service=CSW&version=2.0.2&request=GetRecordById&id=mitma_spamitma_hermes&elementsetname=full&outputSchema=http://www.isotc211.org/2005/gmd
            dist_info = self._get_distribution_info("inspire", ckan_info.pycsw_site_url + "/?service=CSW&version=2.0.2&request=GetRecordById&id=" + dataset.name + "&elementsetname=full&outputSchema=http://www.isotc211.org/2005/gmd", self.localized_strings_dict['distributions']['inspire'], ckan_info.default_license, ckan_info.default_license_id, dataset.access_rights, dataset.language)
            dataset.add_distribution(self.get_ckan_distribution(distribution, record, dist_info))

    def set_custom_responsible_parties(self, dataset, custom_metadata, ckan_site_url):
        responsible_parties = [
            ('contact', ['name', 'email', 'url', 'uri']),
            ('publisher', ['name', 'email', 'url', 'identifier', 'type']),
            ('maintainer', ['name', 'email', 'url', 'uri']),
            ('author', ['name', 'email', 'url', 'uri'])
        ]

        for party, attributes in responsible_parties:
            for attribute in attributes:
                custom_value = custom_metadata.get(f'{party}_{attribute}', None)
                default_value = getattr(self.default_dcat_info, f'{party}_{attribute}', None)
                value = custom_value or default_value
                if value is None and party == 'maintainer':
                    publisher_value = getattr(custom_metadata, f'publisher_{attribute}', None) or getattr(self.default_dcat_info, f'publisher_{attribute}', None)
                    value = publisher_value
                if value:
                    attribute = f'set_{party}_{attribute}'
                    method = getattr(dataset, attribute)
                    method(value)

        dataset.set_publisher_uri('{}/organization/{}'.format(ckan_site_url, self.organization).casefold())

    def set_default_responsible_parties(self, dataset, default_dcat_info, ckan_info, source_dataset=None):
        """
        Sets the metadata for a dataset.

        Args:
            dataset: The dataset to set the metadata for.
            source_dataset: The dataset information from the source.
            default_dcat_info: The default DCAT information.
            ckan_info: The CKAN information.
        """
        source_dataset = source_dataset or {}

        # Define a mapping of metadata fields to default DCAT attributes
        metadata_fields = {
            "contact_name": "publisher_name",
            "contact_email": "publisher_email",
            "contact_url": "publisher_url",
            "contact_uri": "contact_uri",
            "publisher_name": "publisher_name",
            "publisher_email": "publisher_email",
            "publisher_url": "publisher_url",
            "publisher_identifier": "contact_uri",
            "maintainer_name": "publisher_name",
            "maintainer_email": "publisher_email",
            "maintainer_url": "publisher_url",
            "publisher_type": "publisher_type",
            "maintainer_uri": "contact_uri",
            "author_name": "author_name",
            "author_email": "author_email",
            "author_url": "author_url",
            "author_uri": "contact_uri"
        }

        # Set metadata fields based on default DCAT attributes or source dataset
        for metadata_field, dcat_attribute in metadata_fields.items():
            value = source_dataset.get(metadata_field) or getattr(default_dcat_info, dcat_attribute, None)
            if metadata_field == "contact_uri" and not value:
                value = self._get_dir3_uri(ckan_info.dir3_soup, self.get_default_dcat_info_attribute("contact_uri"), source_dataset.get("contact_name"))
            elif metadata_field == "publisher_identifier" and not value:
                value = self._get_dir3_uri(ckan_info.dir3_soup, self.get_default_dcat_info_attribute("contact_uri"), source_dataset.get("publisher_name"))
            elif metadata_field == "maintainer_uri" and not value:
                value = self._get_dir3_uri(ckan_info.dir3_soup, self.get_default_dcat_info_attribute("contact_uri"), source_dataset.get("maintainer_name"))
            elif metadata_field == "publisher_type" and value:
                value = value.replace('https:', 'http:')
            getattr(dataset, f"set_{metadata_field}")(value)

        # Set publisher URI
        dataset.set_publisher_uri((ckan_info.ckan_site_url + "/organization/" + self.organization).lower())

    # Geo Operations
    def set_bounding_box(self, dataset, bounding_box):
        """Sets the bounding box for a dataset.

        Extracts bounding box from array with this format: [{'nativeSrs': 'http://www.opengis.net/def/crs/EPSG/0/4326',
        'bbox': (42.85220261509768, -8.578697981248412, 42.90184661509768, -8.511959981248413)}]

        Args:
            dataset: The dataset to set the bounding box for.
            bounding_box: A list containing the bounding box coordinates.

        Returns:
            None.

        Raises:
            None.
        """
        # 0: minx, 1: miny, 2: maxx, 3: maxy
        self._set_min_max_coordinates(dataset, bounding_box[0], bounding_box[2], bounding_box[1], bounding_box[3])

    def set_bounding_box_from_iso(self, dataset, bounding_box):
        """Sets the bounding box for a dataset based on an ISO bounding box.

        Args:
            dataset: The dataset to set the bounding box for.
            bounding_box: An ISO bounding box object.

        Returns:
            None.

        Raises:
            None.
        """
        # Need to convert a string to float
        self._set_min_max_coordinates(dataset, float(bounding_box.minx), float(bounding_box.maxx), float(bounding_box.miny), float(bounding_box.maxy))

    def set_bounding_box_from_bounding_box(self, dataset, bounding_box):
        """Sets the bounding box for a dataset based on another bounding box.

        Args:
            dataset: The dataset to set the bounding box for.
            bounding_box: A list containing the bounding box coordinates.

        Returns:
            None.

        Raises:
            None.
        """
        if bounding_box[4].id:
            transformer = Transformer.from_crs(bounding_box[4].id, 'EPSG:4326')

            minx, miny = transformer.transform(bounding_box[0], bounding_box[2])
            maxx, maxy = transformer.transform(bounding_box[2], bounding_box[3])

            self._set_min_max_coordinates(dataset, minx, maxx, miny, maxy)

    def set_translated_fields(self, dataset, source_data: object, source_language=None):
        if not isinstance(source_data, object):
            raise Exception(f"source data of: '{dataset.title}' not supported translated_fields")

        # Check if source_language and self.default_language are the same, if not and source_language is not None, set default_language_code to source_language
        source_language = source_language if "http" in source_language else None
        default_language = source_language if source_language is not None and source_language != self.default_language else self.default_language

        required_lang = get_mapping_value(default_language, 'language', 'iso_639_1')

        for field, field_translated in OGC2CKAN_MD_MULTILANG_FIELDS.items():
            output = {}
            source_langs = set()

            prefix = field + '-'
            for match_field in filter(lambda attr: attr.startswith(prefix), dir(source_data)):
                lang = match_field.split('-', 1)[1]
                source_langs.add(lang)
                m = re.match(BCP_47_LANGUAGE, lang)
                if not m:
                    logging.error(f"{log_module}:set_translated_fields | Invalid language code: '{lang}'")
                    output = None
                    
                try:
                    if output is not None:
                        output[lang] = getattr(source_data, match_field)
                except Exception as e:
                    logging.error(f"{log_module}:set_translated_fields | Error getting attribute '{match_field}': {e}")
                    output = None
                    
                if output is None:
                    output = {}

            if required_lang in source_langs:
                getattr(dataset, f"set_{field_translated}")(output)  
            else:
                value = getattr(dataset, field) if hasattr(dataset, field) else ''
                output[required_lang] = value if value is not None else ''
                getattr(dataset, f"set_{field_translated}")(output)
                try:
                    getattr(dataset, f"set_{field_translated}")(output)
                except Exception as e:
                    logging.error(f"{log_module}:set_translated_fields | Error getting attribute '{field_translated}': {e}")

    def set_default_keywords_themes_topic(self, dataset, custom_metadata, ckan_schema = 'geodcatap', source_keywords = None, source_keywords_uri = None):
        """Sets the keywords for a dataset. INSPIRE keywords/themes, default/custom keywords, ISO 19115 Topic category and Spanish NTI-RISP Theme.

        Args:
            dataset: The dataset to set the keywords for.
            custom_metadata: A dictionary containing custom metadata for the dataset.
            ckan_schema: The CKAN schema (ckanext-scheming) dataset type.

        Returns:
            None.

        Raises:
            None.
        """
        
        if source_keywords is not None:
            keywords = source_keywords
        else:
            keywords = []
            
        if source_keywords_uri is not None:
            keywords_uri = set(source_keywords_uri)
        else:
            keywords_uri = set()
        
        # Includes default keywords
        themes = []
        themes_es = []
        themes_eu = []
        if self.default_keywords is not None:
            for k in self.default_keywords:
                keyword_name = k["name"].lower()
                if k["uri"]:
                    keywords_uri.add(k["uri"])
                keywords.append({'name': keyword_name})

        # Add custom keywords
        if custom_metadata is not None and 'keywords' in custom_metadata:
            for k in custom_metadata['keywords']:
                new_keyword = {'name': k['name'].lower()}
                if new_keyword not in keywords:
                    keywords.append(new_keyword)
                keywords_uri.add(k['uri'])

        # Set keywords (INSPIRE quality) and INSPIRE Themes
        theme = self.get_default_dcat_info_attribute("theme").lower() or None
        if theme:
            inspireid_theme = theme.split('/')[-1]

        # Insert inspireid_theme (default) as theme/keyword
        keywords.append({'name': inspireid_theme})
        keywords_uri.add(theme)
        themes.append(theme)

        # Set ISO 19115 Topic Category
        ## Insert topic (default) as topic
        default_topic = self.get_default_dcat_info_attribute("topic")
        dataset.set_topic(default_topic)

        # Insert theme_eu (default) 
        theme_eu = self.get_default_dcat_info_attribute("theme_eu")
        themes_eu.append(theme_eu)

        # Insert theme_es if ckan_schema == 'geodcatap_es'
        if ckan_schema == 'geodcatap_es':
            theme_es = self.get_default_dcat_info_attribute("theme_es")
            themes_es.append(theme_es)
            self._set_themes_es(dataset, list(set(themes_es)))

        self._set_themes_eu(dataset, list(set(themes_eu)))
        self._set_themes(dataset, list(set(themes)))
        dataset.set_keywords(self.clean_keywords(keywords))
        self._set_keywords_uri(dataset, list(keywords_uri))
        
    def clean_keywords(self, keywords):
        """
        Cleans the names of keywords in a list of dictionaries.

        Removes empty names and non-alphanumeric characters, allowing only: a-z, ñ, 0-9, _, -, ., and spaces.
        Truncates the cleaned name to a maximum length of 100 characters.

        Args:
            keywords (list): A list of dictionaries containing keyword information.

        Returns:
            list: A new list of dictionaries with cleaned keyword names.
        """
        cleaned_keywords = [{'name': self._clean_name(d['name'])} for d in keywords if d.get('name')]

        return cleaned_keywords

    @staticmethod
    def _clean_name(name):
        """
        Cleans a name by removing accents, special characters, and spaces.

        Args:
            name (str): The name to clean.

        Returns:
            str: The cleaned name.
        """
        # Define a dictionary to map accented characters to their unaccented equivalents except ñ
        accent_map = {
            'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u',
            'ü': 'u', 'ñ': 'ñ'
        }

        # Replace accented and special characters with their unaccented equivalents or _
        name = ''.join(accent_map.get(c, c) for c in name)
        name = re.sub(r'[^a-zñ0-9_.-]', '_', name.lower().strip())

        # Truncate the name to 40 characters
        name = name[:40]

        return name