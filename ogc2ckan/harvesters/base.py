# inbuilt libraries
import inspect
import uuid
import re
import unicodedata
from typing import Any
import logging
from datetime import datetime

# third-party libraries
from geojson import Polygon, dumps
from pyproj import Transformer
import pandas as pd
import re
from bs4 import BeautifulSoup

# custom classes
from controller import ckan_management
from model.custom_organization import CustomOrganization
from config.ogc2ckan_config import load_yaml
from mappings.default_ogc2ckan_config import OGC2CKAN_PATHS_CONFIG, OGC2CKAN_HARVESTER_MD_CONFIG, OGC2CKAN_CKANINFO_CONFIG, OGC2CKAN_MD_FORMATS
from harvesters.harvesters import get_harvester_class


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
    def __init__(self, app_dir, url, name, groups, active, organization, type, custom_organization_active, custom_organization_mapping_file, private_datasets, default_keywords, default_inspire_info, default_dcat_info):
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
        self.datasets = []
        self.ckan_count = 0
        self.server_count = 0
        # Additional custom organization info (ckan-harvester/src/ckan/ogc_ckan/custom/mappings)
        self.custom_organization_info = CustomOrganization(self) if custom_organization_active else None
        default_localized_strings_file = f"{self.app_dir}/{OGC2CKAN_PATHS_CONFIG['default_mappings_folder']}/{OGC2CKAN_PATHS_CONFIG['default_localized_strings_file']}"
        # Localized default info
        yaml_dict = load_yaml(default_localized_strings_file)
        language = self.default_dcat_info.language
        self.localized_strings_dict = self._get_localized_dict(yaml_dict, language)

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

        # Check if the dataset exists in CKAN
        if hasattr(self, 'constraints') and self.constraints:
            emails = set(email.lower().replace(' ','') for email in (self.constraints.get('mails') or []) if email)
        else:
            emails = []
        datasets_title = [x.title for x in self.datasets if x.contact_email.lower().replace(' ','') in emails]
        logging.info(f"{self.name} ({self.type.upper()}) server records found: {', '.join(datasets_title)}")

        if hasattr(self, 'workspaces') and self.workspaces:
            logging.info(f"{self.name} ({self.type.upper()}) server OGC workspaces selected: {', '.join([w.upper() for w in self.workspaces])}")
            
            # Create datasets using ckan_management
            self.ckan_count, self.server_count = ckan_management.create_ckan_datasets(ckan_info.ckan_site_url, ckan_info.authorization_key, self.datasets, ckan_info.ssl_unverified_mode, self.workspaces)

        else:
            # Create datasets using ckan_management
            self.ckan_count, self.server_count = ckan_management.create_ckan_datasets(ckan_info.ckan_site_url, ckan_info.authorization_key, self.datasets,  ckan_info.ssl_unverified_mode)
        
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
                - ckan_name (str): The CKAN name for the dataset, based on the UUID identifier and organization.
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
        
        uuid_identifier = self._create_uuid_identifier()
        ckan_name = uuid_identifier
        
        ckan_groups = [{'name': g.lower()} for g in self.groups or []]

        # Create inspireId
        inspire_id = ".".join(filter(None,[self.default_inspire_info['inspireid_nutscode'], self.default_inspire_info['inspireid_theme'], record.replace(':', '.'), self.default_inspire_info['inspireid_versionid']])).upper()

        return dataset, distribution, uuid_identifier, ckan_name, ckan_groups, inspire_id
 
    def get_custom_default_metadata(self, dataset_id: str, dict_property: str = 'dataset_id') -> Any:
        """
        Get the custom default metadata for a given dataset ID.

        Args:
            dataset_id (str): The ID of the dataset.
            dict_property (str): The property to retrieve from the mapping dictionary.

        Returns:
            Any: The value of the specified property in the mapping dictionary, or None if the property is not found.
        """
        mapping = self.custom_organization_info.find_mapping_value(dataset_id, dict_property)
        
        if mapping is None:
            mapping = self.custom_organization_info.find_similar_mapping_value(dataset_id, dict_property)
        
        return mapping
 
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
        modified_themes = [theme.replace('https:', 'http:') for theme in themes_es]
        dataset.set_theme_es(modified_themes)

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
                value = self._get_dir3_uri(ckan_info.dir3_soup, getattr(default_dcat_info, "contact_uri", None), source_dataset.get("contact_name"))
            elif metadata_field == "publisher_identifier" and not value:
                value = self._get_dir3_uri(ckan_info.dir3_soup, getattr(default_dcat_info, "contact_uri", None), source_dataset.get("publisher_name"))
            elif metadata_field == "maintainer_uri" and not value:
                value = self._get_dir3_uri(ckan_info.dir3_soup, getattr(default_dcat_info, "contact_uri", None), source_dataset.get("maintainer_name"))
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

    def set_keywords_themes_topic(self, dataset, custom_metadata):
        """Sets the keywords for a dataset. INSPIRE keywords/themes, default/custom keywords, ISO 19115 Topic category and Spanish NTI-RISP Theme.

        Args:
            dataset: The dataset to set the keywords for.
            custom_metadata: A dictionary containing custom metadata for the dataset.

        Returns:
            None.

        Raises:
            None.
        """
        # Includes default keywords
        keywords = []
        themes = []
        themes_es = []
        keywords_uri = set()
        if self.default_keywords is not None:
            for k in self.default_keywords:
                keyword_name = k["name"].lower()
                if '/theme/' in k["uri"]:
                    # INSPIRE Theme
                    keywords.append({'name': keyword_name})
                else: 
                    # Keyword
                    keywords.append({'name': keyword_name})
                keywords_uri.add(k["uri"])

        # Add custom keywords
        if custom_metadata is not None and 'keywords' in custom_metadata:
            for k in custom_metadata['keywords']:
                new_keyword = {'name': k['name'].lower()}
                if new_keyword not in keywords:
                    keywords.append(new_keyword)
                keywords_uri.add(k['uri'])

        # Set keywords (INSPIRE quality) and INSPIRE Themes
        inspireid_theme = self.default_inspire_info['inspireid_theme'].lower()
        theme_inspire = "http://inspire.ec.europa.eu/theme/" + inspireid_theme
            
        # Insert inspireid_theme (default) as theme/keyword
        keywords.append({'name': inspireid_theme})
        keywords_uri.add(theme_inspire)                
        themes.append(inspireid_theme)

        # Set ISO 19115 Topic Category
        ## Insert topic (default) as topic
        default_topic = self.default_dcat_info.topic
        dataset.set_topic(default_topic)
            
        # Insert theme_es (default) as theme            
        themes_es.append(self.default_dcat_info.theme_es)
            
        self._set_themes_es(dataset, list(set(themes_es)))
        self._set_themes(dataset, list(set(themes)))
        dataset.set_keywords(keywords)
        self._set_keywords_uri(dataset, list(keywords_uri))