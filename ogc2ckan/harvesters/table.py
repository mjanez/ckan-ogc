# inbuilt libraries
from datetime import datetime
import os
from pathlib import Path
import logging
import re
import uuid

# third-party libraries
import pandas as pd
import numpy as np
import openpyxl
import xlrd

# custom classes
from harvesters.base import Harvester
from config.ckan_config import CKANInfo

# custom functions
from config.ogc2ckan_config import get_log_module
from mappings.default_ogc2ckan_config import OGC2CKAN_HARVESTER_CONFIG, OGC2CKAN_HARVESTER_MD_CONFIG, OGC2CKAN_CKANINFO_CONFIG
from controller.mapping import get_df_mapping_json

log_module = get_log_module(os.path.abspath(__file__))

class ObjectFromListDicts:
    """
    A class to represent an object from a list of dictionaries.
    """
    def __init__(self, **entries):
        for key, value in entries.items():
            if key == 'distributions':
                setattr(self, key, [ObjectFromListDicts(**d) for d in value])
            else:
                setattr(self, key, value)

    def get(self, key, default=None):
        """
        Returns the value of the specified key if it exists, otherwise returns the default value.

        Args:
            key: The key to get the value for.
            default: The default value to return if the key does not exist.

        Returns:
            The value of the specified key if it exists, otherwise the default value.
        """
        return getattr(self, key, default)

class HarvesterTable(Harvester):
    def __init__(self, app_dir, url, name, groups, active, organization, type, custom_organization_active, custom_organization_mapping_file, private_datasets, default_keywords, default_inspire_info, ckan_name_not_uuid, **default_dcat_info):
        super().__init__(app_dir, url, name, groups, active, organization, type, custom_organization_active, custom_organization_mapping_file, private_datasets, default_keywords, default_inspire_info, ckan_name_not_uuid, **default_dcat_info)
        self.file_extension = Path(self.url).suffix[1:]
        self.table_data = []
        self.datadictionaries = []
        
    def get_file_by_extension(self, harvester_formats):
        filename = os.path.basename(self.url)
        try:
            if self.file_extension in harvester_formats:
                if self.file_extension in ['csv', 'tsv']:
                    #TODO add support for other file formats
                    raise Exception(f"Table file format: '{self.file_extension}' not supported yet")
                    if self.file_extension == 'csv':
                        table_data = pd.read_csv(self.url, sep=',', encoding='utf-8', dtype=str)
                        table_distributions = table_data[table_data['table_type'] == 'distribution']
                    elif self.file_extension == 'tsv':
                        table_data = pd.read_csv(self.url, sep='\t', encoding='utf-8', dtype=str)
                        table_distributions = table_data[table_data['table_type'] == 'distribution']
                elif self.file_extension in ['xls', 'xlsx']:
                    engine = 'openpyxl' if self.file_extension == 'xlsx' else None
                    table_data = pd.read_excel(self.url, sheet_name='Dataset', dtype=str, engine=engine).fillna('')
                    table_distributions = pd.read_excel(self.url, sheet_name='Distribution', dtype=str, engine=engine).fillna('')
                    table_datadictionaries = pd.read_excel(self.url, sheet_name='DataDictionary', dtype=str, engine=engine).fillna('')
                                
                logging.info(f"{log_module}:Load '{self.file_extension.upper()}' file: '{filename}' with {len(table_data)} records") 

                # Clean column names by removing leading/trailing whitespaces, newlines, and tabs
                table_data.columns = table_data.columns.str.strip().str.replace('\n', '').str.replace('\t', '')

                # Remove all fields that are a nan float and trim all spaces of the values
                table_data = table_data.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
                table_data = table_data.fillna(value='')

                # Convert table to list of dicts
                table_data = table_data.to_dict('records')

                # Select only the columns of type 'object' and apply the strip() method to them
                table_distributions.loc[:, table_distributions.dtypes == object] = table_distributions.select_dtypes(include=['object']).apply(lambda x: x.str.strip())
                table_datadictionaries.loc[:, table_datadictionaries.dtypes == object] = table_datadictionaries.select_dtypes(include=['object']).apply(lambda x: x.str.strip())
                
                # Remove prefixes from column names in the distributions/datadictionaries dataframe
                table_distributions = table_distributions.rename(columns=lambda x: x.replace('resource_', ''))
                table_datadictionaries = table_datadictionaries.rename(columns=lambda x: re.sub(re.compile(r'datadictionary(_info)?_'), '', x).replace('info.', ''))

                # Remove rows where 'dataset_id' is None or an empty string
                table_distributions = table_distributions[table_distributions['dataset_id'].notna() & (table_distributions['dataset_id'] != '')]

                if not table_distributions.empty:
                    # Group distributions by dataset_id and convert to list of dicts
                    table_distributions_grouped = table_distributions.groupby('dataset_id' ).apply(lambda x: x.to_dict('records')).to_dict()
                else:
                    logging.info(f"{log_module}:No distributions loaded. Check 'distribution.dataset_id' fields")
                    table_distributions_grouped = None

                # Filter datadictionaries where resource_id is not empty or None
                if 'resource_id' in table_datadictionaries.columns:
                    table_datadictionaries = table_datadictionaries[table_datadictionaries['resource_id'].notna() & (table_datadictionaries['resource_id'] != '')]

                    # Group datadictionaries by resource_id and convert to list of dicts
                    table_datadictionaries_grouped = table_datadictionaries.groupby('resource_id').apply(lambda x: x.to_dict('records')).to_dict()
                else:
                    logging.info(f"{log_module}:No datadictionaries loaded. Check 'datadictionary.resource_id' fields.")
                    table_datadictionaries_grouped = None
                     
                # Add distributions and datadictionaries to each dataset object
                table_data = [
                    {
                        **d,
                        'distributions': [
                            {**dr, 'datadictionaries': table_datadictionaries_grouped.get(dr['id'], []) if table_datadictionaries_grouped else []}
                            for dr in table_distributions_grouped.get(
                                d.get('identifier') or d.get('alternate_identifier') or d.get('inspire_id'), []
                            )
                        ]
                    }
                    for d in table_data
                ]

                return table_data

            else:
                raise Exception(f"Table file format: '{self.file_extension}' not supported")
    
        except Exception as e:
            raise Exception(f"{log_module}:Failed to load the file:'{self.url}'", str(e))

    def get_datasets(self, ckan_info):
        harvester_formats = ckan_info.ckan_harvester['table']['formats']
        # Get table data
        self.table_data = self.get_file_by_extension(harvester_formats)
        
        # Update values with commas to lists of objects
        self.table_data = self._update_object_lists(self.table_data)

        for table_dataset in self.table_data:
            self.datasets.append(self.get_dataset(ckan_info, table_dataset.title, table_dataset))
               
        return self.datasets
    
    def get_dataset(self, ckan_info: CKANInfo, record: str, table_dataset: object = None):
        '''
        Gets a dataset from tabular data.

        Args:
            ckan_info (CKANInfo): CKANInfo object containing the CKAN URL and API key.
            record (str): Name of the dataset to retrieve.
            table_dataset (object): Table dataset object.

        Returns:
            Dataset: Dataset object.
        '''
        # Get basic elements for the CKAN dataset
        dataset, distribution, datadictionary, datadictionaryfield, uuid_identifier, ckan_name, ckan_groups, inspire_id = \
            self.get_dataset_common_elements(record, ckan_info.ckan_dataset_schema)
                
        # Set basic info of MD
        dataset = dataset(uuid_identifier, ckan_name, self.organization, ckan_info.default_license_id)
        
        # Set inspireId (identifier)
        inspire_id = getattr(table_dataset, 'inspire_id', inspire_id)
        dataset.set_inspire_id(inspire_id)  
        
        # Search if exists custom organization info for the dataset
        custom_metadata = None
        if self.custom_organization_active:
            try:
                custom_metadata = self.get_custom_default_metadata(inspire_id)
            except:
                custom_metadata = self.get_custom_default_metadata(inspire_id, 'dataset_group_id')
        
        # Set private dataset. Check first if exists in table_dataset, otherwise check in ckan config
        private = bool(table_dataset.private) if hasattr(table_dataset, 'private') and table_dataset.private in [True, 'True', 'true'] else getattr(self, 'private_datasets', False)
        dataset.set_private(private)
        
        # Set alternate identifier (layer name)
        alternate_identifier = getattr(table_dataset, 'alternate_identifier', None)
        dataset.set_alternate_identifier(alternate_identifier)

        # Title
        title = custom_metadata.get('title') if custom_metadata else table_dataset.title
        dataset_title = title or self.localized_strings_dict['title'] if title is not None else f"{self.localized_strings_dict['title']} {inspire_id}"
        dataset.set_title(dataset_title)

        # Description
        description = custom_metadata.get('description') if custom_metadata else table_dataset.notes
        dataset.set_notes(description or self.localized_strings_dict['description'])
  
        # CKAN Groups
        # ckan_groups is table_dataset.groups if exists, otherwise ckan_groups is ckan_groups
        dataset_groups = getattr(table_dataset, 'groups', ckan_groups)
        dataset.set_groups(self._set_ckan_groups(dataset_groups))

        # Creation/Publication/Revision dates
        issued_date = datetime.now().strftime('%Y-%m-%d')
        created_date = self._normalize_date(table_dataset.created) or issued_date
        modified_date = self._normalize_date(table_dataset.modified) or issued_date
        
        dataset.set_issued(issued_date)
        dataset.set_created(created_date)
        dataset.set_modified(modified_date)

        # DCAT Type (dataset/series)
        dcat_type = getattr(table_dataset, 'dcat_type', OGC2CKAN_HARVESTER_MD_CONFIG['representation_type']['default'])
        dcat_type = dcat_type.replace('https:', 'http:') if dcat_type else None
        dataset.set_resource_type(dcat_type)

        # Set SpatialRepresentationType
        representation_type = getattr(table_dataset, 'representation_type', OGC2CKAN_HARVESTER_MD_CONFIG['representation_type']['default']).replace('https:', 'http:')
        dataset.set_representation_type(representation_type)

        # Set valid date
        if hasattr(table_dataset, 'valid'):
            valid_date = table_dataset.valid
        else:
            valid_date = self.get_default_dcat_info_attribute("valid")

        if valid_date:
            dataset.set_valid(self._normalize_date(valid_date))

        # Set access rights (Dataset)       
        access_rights = getattr(table_dataset, 'access_rights', OGC2CKAN_HARVESTER_MD_CONFIG['access_rights']).replace('https:', 'http:')
        dataset.set_access_rights(access_rights)

        # Set SpatialResolutionInMeters
        spatial_resolution_in_meters = getattr(table_dataset, 'set_spatial_resolution_in_meters', None)
        dataset.set_spatial_resolution_in_meters(spatial_resolution_in_meters)

        # Set language
        language = getattr(table_dataset, 'language', OGC2CKAN_HARVESTER_MD_CONFIG['language']).replace('https:', 'http:')
        dataset.set_language(language)

        # Set spatial coverage
        spatial = getattr(table_dataset, 'spatial', self.get_default_dcat_info_attribute("spatial"))
        dataset.set_spatial(spatial)

        # Set spatial URI
        spatial_uri = getattr(table_dataset, 'spatial_uri', self.get_default_dcat_info_attribute("spatial_uri"))
        dataset.set_spatial_uri(spatial_uri)        

        # Set temporal coverage
        temporal_start = getattr(table_dataset, 'temporal_start', self.get_default_dcat_info_attribute("temporal_start"))
        temporal_end = getattr(table_dataset, 'temporal_end', self.get_default_dcat_info_attribute("temporal_end"))
        dataset.set_temporal_start(self._normalize_date(temporal_start))
        dataset.set_temporal_end(self._normalize_date(temporal_end))

        # Set Frequency
        frequency = getattr(table_dataset, 'frequency', self.get_default_dcat_info_attribute("frequency"))
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
            
        else:
            # Set provenance (INSPIRE Lineage)
            provenance = getattr(table_dataset, 'provenance', self.localized_strings_dict['provenance'])
            dataset.set_provenance(provenance)

            # Set source (INSPIRE quality)
            source = getattr(table_dataset, 'source', None)
            dataset.set_source(source)
            
            # Set lineage_source (INSPIRE Lineage sources)
            lineage_source = getattr(table_dataset, 'lineage_source', self.localized_strings_dict.get('lineage_source', None))
            dataset.set_lineage_source(lineage_source)
            
            # Set reference
            reference = getattr(table_dataset, 'reference', None)
            dataset.set_reference(reference)

            # Set process steps (INSPIRE quality)
            lineage_process_steps = getattr(table_dataset, 'lineage_process_steps', self.localized_strings_dict.get('lineage_process_steps', None))
            dataset.set_lineage_process_steps(lineage_process_steps)            

        # Set conformance (INSPIRE regulation)
        conformance = getattr(table_dataset, 'conformance', OGC2CKAN_HARVESTER_MD_CONFIG['conformance'])
        dataset.set_conformance(conformance)

        # Set EPSG
        reference_system = getattr(table_dataset, 'reference_system', OGC2CKAN_HARVESTER_MD_CONFIG['reference_system'])
        dataset.set_reference_system(reference_system)

        # Set Metadata profile
        metadata_profile = getattr(table_dataset, 'metadata_profile', OGC2CKAN_HARVESTER_MD_CONFIG['metadata_profile'])
        dataset.set_metadata_profile(metadata_profile)
        
        # Set graphic overview
        graphic_overview = getattr(table_dataset, 'graphic_overview', None)
        dataset.set_graphic_overview(graphic_overview)
        
        # Set purpose
        purpose = getattr(table_dataset, 'purpose', None)
        dataset.set_purpose(purpose)
        
        # Set Responsible Parties (Point of contact, Resource publisher and Resource contact/maintainer)
        self.set_default_responsible_parties(dataset, self.default_dcat_info, ckan_info, table_dataset)

        # Responsible Party (Resource creator)
        if hasattr(table_dataset, 'author_name'):
            dataset.set_author_name(table_dataset.author_name)
        if hasattr(table_dataset, 'author_email'):
            dataset.set_author_email(table_dataset.author_email)
        if hasattr(table_dataset, 'author_url'):
            dataset.set_author_url(table_dataset.author_url)
        if hasattr(table_dataset, 'author_uri'):
            dataset.set_author_uri(table_dataset.author_uri)

        # Set license
        dataset.set_license(ckan_info.default_license)
        
        # Set distributions
        if table_dataset.distributions:
            self.get_distribution(ckan_info, dataset, distribution, datadictionary, datadictionaryfield, record, table_dataset)
        
        # Metadata distributions (INSPIRE & GeoDCAT-AP)
        self.set_metadata_distributions(ckan_info, dataset, distribution, record)
        
        # Set keywords from table
        keywords = []
        keywords_uri = []
        if hasattr(table_dataset, 'tag_string'):
            tag_string = table_dataset.tag_string
            tag_string = [tag_string] if isinstance(tag_string, str) else tag_string
            for k in tag_string:
                keyword_name = k.lower()
                if 'http' in keyword_name or '/' in keyword_name:
                    keyword_name = keyword_name.split('/')[-1]
                    keywords_uri.add(keyword_name)
                keywords.append({'name': keyword_name})
                
        if hasattr(table_dataset, 'tag_uri'):
            if isinstance(table_dataset.tag_uri, str):
                keywords_uri.append(table_dataset.tag_uri)
            else:
                for k in table_dataset.tag_uri:
                    keywords_uri.append(k)
        
        # Set keywords/themes/topic categories
        self.set_default_keywords_themes_topic(dataset, custom_metadata, ckan_info.ckan_dataset_schema, keywords, keywords_uri)
        
        # Set translated fields if multilang is True
        if ckan_info.dataset_multilang:
            self.set_translated_fields(dataset, table_dataset, language)

        return dataset
    
    def get_distribution(self, ckan_info: CKANInfo, dataset, distribution, datadictionary, datadictionaryfield, record: str, table_dataset: object = None):
        # Add distributions
        dataset.set_distributions([])
        datadictionaries = []
        
        for i, r in enumerate(table_dataset.distributions):
            distribution_id = self._normalize_id(r.get('id', str(uuid.uuid4())))
            # Get data dictionaries
            if r.datadictionaries:
                self.get_datadictionary(datadictionary, datadictionaryfield, r.datadictionaries, distribution_id)
            
            try:
                # Set distribution format from MD
                dist_info = self._get_distribution_info(
                    r.get('format', None),
                    r.url,
                    r.get('description', None),
                    r.get('license', ckan_info.default_license),
                    r.get('license_id', ckan_info.default_license_id),
                    r.get('rights', dataset.access_rights),
                    r.get('language', dataset.language)
                )

                format_type, media_type, conformance, default_name = self._get_ckan_format(dist_info)
                dist_name = r.get('name', default_name)

            except Exception as e:
                logging.error(f"Error processing distribution {i} of {record}. Default values.")
                format_type, media_type, conformance, dist_name = None, None, [], r.name or f"{self.localized_strings_dict['distributions']['default']} {table_dataset.title}."

            try:
                                
                distribution_data = {
                    'id': distribution_id,
                    'url': r.get('url', ''),
                    'name': dist_name,
                    'format': self._update_custom_formats(format_type, r.get('url', '')),
                    'media_type': media_type,
                    'description': r.get('description', ''),
                    'license': r.get('license', ckan_info.default_license),
                    'license_id': r.get('license_id', ckan_info.default_license_id),
                    'rights': r.get('rights', dataset.access_rights).replace('https:', 'http:'),
                    'language': r.get('language', dataset.language).replace('https:', 'http:'),
                    'conformance': conformance
                }
                
                dataset.add_distribution(distribution(**distribution_data))
                
            except Exception as e:
                logging.error(f"Error adding distribution {dist_name} to dataset: {e}")

    def get_datadictionary(self, datadictionary, datadictionaryfield, table_datadictionary: object = None, distribution_id: str = None):
        # Set Data dictionaries of distributions
        datadictionary = datadictionary(distribution_id)
        try:
            for field in table_datadictionary:
                datadictionary_fields = {
                    'id': field.get('id', 'Unknown field'),
                    'type': field.get('type', 'text').lower(),
                    'label': field.get('label', ''),
                    'notes': field.get('notes', ''),
                    'type_override': field.get('type_override', ''),
                }
                
                datadictionary.add_datadictionary_field(datadictionaryfield(**datadictionary_fields))
            
            # Append datadictionaries to object list in harvester object
            self.datadictionaries.append(datadictionary)
            
        except Exception as e:
            logging.error(f"Error adding data dictionary {distribution_id}: {e}")


    @staticmethod
    def _update_custom_formats(format, url=None, **args):
        """Update the custom format.
        
        If the format contains 'esri' or 'arcgis' (case-insensitive) or the URL contains 'viewer.html?url=',
        the format is updated to 'HTML'.
        
        Args:
            format (str): The custom format to update.
            url (str, optional): The URL to check. Defaults to None.
            **args: Additional arguments that are ignored.
            
        Returns:
            str: The updated custom format.
        """
        if isinstance(format, str) and (any(string in format.lower() for string in ['esri', 'arcgis']) or 'viewer.html?url=' in url):
            format = 'HTML'
            
        return format

    @staticmethod
    def _update_object_lists(data):
        """
        Updates the object lists in the given data by splitting list-like string values and converting the data to a list of objects.

        Args:
            data (list): The list of dictionaries to update.

        Returns:
            list: The updated list of objects.
        """
        # Read the CKAN fields mapping file and filter only the fields that contain 'List' in the 'stored' field
        if OGC2CKAN_CKANINFO_CONFIG['ckan_fields_json']:
            ckan_fields = get_df_mapping_json(OGC2CKAN_CKANINFO_CONFIG['ckan_fields_json'])
        else:
            ckan_fields = get_df_mapping_json()
        ckan_fields_filtered = ckan_fields.loc[ckan_fields['stored'].str.contains('List'), 'new_metadata_field'].tolist()

        # Iterate over each element in the data list
        for element in data:
            # Iterate over each key-value pair in the element dictionary
            for key, value in element.items():
                # Check if the key is in the filtered list
                if key in ckan_fields_filtered:                  
                    # if value is a string list between "" or "'" then remove the quotes
                    if isinstance(value, str) and value.startswith('"') and value.endswith('"') and ',' in value:
                        # Split all values inside ""
                        value_str = re.findall(r'"[^"]+"', value)
                        # Split the string and remove whitespace and starts - from each item
                        element[key] = [x.strip('"').strip().lstrip('-').strip() for x in value_str]

                    # Check if the value is a list-like string
                    elif isinstance(value, str) and any([char in value for char in ',]']):
                        # Split the string and remove whitespace from each item
                        element[key] = [x.strip().lstrip('-').strip() for x in value.split(',')]

                elif key == 'distributions':
                    # Iterate over each distribution dictionary in the 'distributions' list
                    for distribution in value:
                        # Iterate over each key-value pair in the distribution dictionary
                        for key_dist, value_dist in distribution.items():
                            # Check if the key is in the filtered list
                            if key_dist in ckan_fields_filtered:
                                # Check if the value is a list-like string
                                if isinstance(value_dist, str) and any([char in value_dist for char in ',]']):
                                    # Split the string and remove whitespace from each item
                                    distribution[key_dist] = [x.strip() for x in value_dist.split(',')]

        # Convert list of dictionaries to list of objects
        table_datasets = [ObjectFromListDicts(**d) for d in data]

        # Return the updated data
        return table_datasets
