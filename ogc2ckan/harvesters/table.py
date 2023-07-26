# inbuilt libraries
from datetime import datetime
import os
from pathlib import Path
import logging

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
from mappings.default_ogc2ckan_config import OGC2CKAN_HARVESTER_CONFIG, OGC2CKAN_HARVESTER_MD_CONFIG
from controller.mapping import get_df_mapping_json

log_module = get_log_module()

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
    def __init__(self, app_dir, url, name, groups, active, organization, type, custom_organization_active, custom_organization_mapping_file, private_datasets, default_keywords, default_inspire_info, **default_dcat_info):
        super().__init__(app_dir, url, name, groups, active, organization, type, custom_organization_active, custom_organization_mapping_file, private_datasets, default_keywords, default_inspire_info, **default_dcat_info)
        self.file_extension = Path(self.url).suffix[1:]
        self.table_data = []
        
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
                                
                logging.info(f"{log_module}:Load '{self.file_extension.upper()}' file: '{filename}' with {len(table_data)} records") 

                # Remove all fields that are a nan float and trim all spaces of the values
                table_data = table_data.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
                table_data = table_data.fillna(value='')

                # Convert table to list of dicts
                table_data = table_data.to_dict('records')

                # Extract the distribution records from the same excel sheet as datasets using a filter
                table_distributions = table_distributions.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)

                # Remove the 'resource_' prefix from column names in the distributions dataframe
                table_distributions = table_distributions.rename(columns=lambda x: x.replace('resource_', ''))

                # Group distributions by dataset_id and convert to list of dicts
                table_distributions_grouped = table_distributions.groupby('dataset_id').apply(lambda x: x.to_dict('records')).to_dict()

                # Add distributions to each dataset object
                table_data = [{**d, 'distributions': table_distributions_grouped.get(d.get('identifier') or d.get('alternate_identifier'), [])} for d in table_data]
                
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
        dataset, distribution, uuid_identifier, ckan_name, ckan_groups, inspire_id = \
            self.get_dataset_common_elements(record, ckan_info.ckan_dataset_schema)
        
        # Search if exists custom organization info for the dataset
        custom_metadata = None
        if self.custom_organization_active:
            try:
                custom_metadata = self.get_custom_default_metadata(layer_info.id.split(':')[1])
            except:
                custom_metadata = self.get_custom_default_metadata(layer_info.id.split(':')[1], 'dataset_group_id')
        
        # Set basic info of MD
        dataset = dataset(uuid_identifier, ckan_name, self.organization, ckan_info.default_license_id)
        
        # Set private dataset
        private = getattr(self, 'private_datasets', False)
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
        dataset.set_description(description or self.localized_strings_dict['description'])
  
        # CKAN Groups
        dataset.set_groups(ckan_groups)

        # Set inspireId (identifier)
        dataset.set_inspire_id(inspire_id)  

        # Creation/Publication/Revision dates
        issued_date = datetime.now().strftime('%Y-%m-%d')
        created_date = self._normalize_date(table_dataset.created) or issued_date
        modified_date = self._normalize_date(table_dataset.modified) or issued_date
        
        dataset.set_issued(issued_date)
        dataset.set_created(created_date)
        dataset.set_modified(modified_date)

        # DCAT Type (dataset/series)
        dcat_type = OGC2CKAN_HARVESTER_MD_CONFIG['dcat_type']
        is_series = False
        if table_dataset.dcat_type and 'http' in table_dataset.dcat_type and 'series' in table_dataset.dcat_type:
            is_series = True
        dataset.set_resource_type(dcat_type['series' if is_series else 'default'])

        # Set SpatialRepresentationType
        representation_type = getattr(table_dataset, 'representation_type', OGC2CKAN_HARVESTER_MD_CONFIG['spatial_representation_type']['default']).replace('https:', 'http:')
        dataset.set_representation_type(representation_type)

        # Set valid date
        if hasattr(table_dataset, 'valid'):
            valid_date = table_dataset.valid
        elif hasattr(self.default_dcat_info, 'valid'):
            valid_date = self.default_dcat_info.valid
        else:
            valid_date = None

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
        spatial = getattr(table_dataset, 'spatial', getattr(self.default_dcat_info, 'spatial', None))
        dataset.set_spatial(spatial)

        # Set spatial URI
        spatial_uri = getattr(table_dataset, 'spatial_uri', getattr(self.default_dcat_info, 'spatial_uri', None))
        dataset.set_spatial_uri(spatial_uri)        

        # Set temporal coverage (only series)
        if is_series:
            temporal_start = getattr(table_dataset, 'temporal_start', getattr(self.default_dcat_info, 'temporal_start', None))
            temporal_end = getattr(table_dataset, 'temporal_end', getattr(self.default_dcat_info, 'temporal_end', None))
        else:
            dataset.set_temporal_end(getattr(table_dataset, 'temporal_end', None))
            dataset.set_temporal_start(getattr(table_dataset, 'temporal_start', None))

        # Set Frequency
        frequency = getattr(table_dataset, 'frequency', getattr(self.default_dcat_info, 'frequency', None))
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
            dataset.set_source(reference)

            # Set process steps (INSPIRE quality)
            lineage_process_steps = getattr(table_dataset, 'lineage_process_steps', self.localized_strings_dict.get('lineage_process_steps', None))            

        # Set conformance (INSPIRE regulation)
        conformance = getattr(table_dataset, 'conformance', OGC2CKAN_HARVESTER_MD_CONFIG['conformance'])
        dataset.set_conformance(conformance)

        # Set EPSG
        reference_system = getattr(table_dataset, 'reference_system', OGC2CKAN_HARVESTER_MD_CONFIG['reference_system'])
        dataset.set_reference_system(reference_system)

        # Set Metadata profile
        metadata_profile = getattr(table_dataset, 'metadata_profile', OGC2CKAN_HARVESTER_MD_CONFIG['metadata_profile'])
        dataset.set_metadata_profile(metadata_profile)
        
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
        self.get_distribution(ckan_info, dataset, distribution, record, table_dataset)
        
        # Metadata distributions (INSPIRE & GeoDCAT-AP)
        self.set_metadata_distributions(ckan_info, dataset, distribution, record)
        
        # Set keywords/themes/topic categories
        self.set_keywords_themes_topic(dataset, custom_metadata)
        
        return dataset
    
    def get_distribution(self, ckan_info: CKANInfo, dataset, distribution, record: str, table_dataset: object = None):
        # Add distributions
        dataset.set_distributions([])
        
        for i, r in enumerate(table_dataset.distributions):
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
                    'url': r.get('url', ''),
                    'name': dist_name,
                    'format': format_type,
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
        ckan_fields = get_df_mapping_json('ckan_fields.json')
        ckan_fields_filtered = ckan_fields.loc[ckan_fields['stored'].str.contains('List'), 'new_metadata_fields'].tolist()

        # Iterate over each element in the data list
        for element in data:
            # Iterate over each key-value pair in the element dictionary
            for key, value in element.items():
                # Check if the key is in the filtered list
                if key in ckan_fields_filtered:
                    # Check if the value is a list-like string
                    if isinstance(value, str) and any([char in value for char in ',]']):
                        # Split the string and remove whitespace from each item
                        element[key] = [x.strip() for x in value.split(',')]
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

