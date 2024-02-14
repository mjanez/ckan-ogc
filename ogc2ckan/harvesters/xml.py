# inbuilt libraries
import logging
import uuid
import os

# third-party libraries
from owslib.fes import PropertyIsLike
from owslib.iso import MD_Metadata
from owslib.etree import etree

# custom classes
from harvesters.base import Harvester

from config.ckan_config import CKANInfo

# custom functions
from config.ogc2ckan_config import get_log_module
from mappings.default_ogc2ckan_config import OGC2CKAN_HARVESTER_MD_CONFIG, OGC2CKAN_HARVESTER_CONFIG

log_module = get_log_module(os.path.abspath(__file__))


# Custom exceptions.
class XmlError(Exception):
    pass

class HarvesterXML(Harvester):
    def __init__(self, app_dir, url, name, groups, active, organization, type, custom_organization_active, custom_organization_mapping_file, private_datasets, default_keywords, default_inspire_info, ckan_name_not_uuid, constraints, **default_dcat_info):
        super().__init__(app_dir, url, name, groups, active, organization, type, custom_organization_active, custom_organization_mapping_file, private_datasets, default_keywords, default_inspire_info, ckan_name_not_uuid, **default_dcat_info)
        self.md_records = None
        self.folder_path = None
        self.formats = OGC2CKAN_HARVESTER_CONFIG['xml']['formats']
        self.constraint_keywords = self.set_constraint_keywords(constraints)
        self.constraint_mails = self.set_constraint_mails(constraints)

    def set_folder_path(self, folder_path):
        self.folder_path = folder_path
        
    def set_constraint_keywords(self, constraints):
        return [PropertyIsLike("csw:anyText", keyword) for keyword in constraints["keywords"]]

    def set_constraint_mails(self, constraints):
        return [mail.lower().replace(' ','') for mail in constraints["mails"]]
    
    def get_datasets(self, ckan_info):
        self.md_records = self.get_metadata_records()

        for record in self.md_records:
            self.datasets.append(self.get_dataset(ckan_info, record, 'xml'))

        return self.datasets
    
    def get_metadata_records(self):
        """Get metadata records and return them in a dictionary with the identifier as the key.

        Returns:
            dict: A dictionary of MD_Metadata objects with the identifier as the key.
        """
        md_records = {}
        md_file_paths = []
        
        if os.path.isfile(self.url):
            md_file_paths.extend([self.url])
        else:
            for md_format in self.formats:
                try:
                    md_file_paths.extend([os.path.join(root, file) for root, dirs, files in os.walk(self.url) for file in files if file.endswith(md_format)])
                except XmlError as e:
                    logging.error(f"{log_module}:Error retrieving metadata records from folder: '{self.url}': {e}")
        
        for md_record in md_file_paths:
            try:
                metadata = MD_Metadata(etree.parse(md_record))
                identifier = metadata.identifier
                #TODO: Multilang also for CSW and OGC harvesters
                #metadata.locales = ['es', 'en']
                if identifier:
                    md_records[identifier] = metadata
            except XmlError as e:
                logging.error(f"{log_module}:Error adding loading MD_Metadata record: '{md_record}': {e}")
                
        return md_records

    def get_dataset(self, ckan_info: CKANInfo, record: str, service_type: str):
        '''
        Gets a dataset from an XML metadata file (MD_Metadata OWSLib class).

        Args:
            ckan_info (CKANInfo): CKANInfo object containing the CKAN URL and API key.
            record (str): identifier of the dataset to retrieve.
            service_type (str): Type of OGC service ('csw' for Catalog endpoints).

        Returns:
            Dataset: Dataset object.
        '''
        # Get basic elements for the CKAN dataset
        dataset, distribution, datadictionary, datadictionaryfield, uuid_identifier, ckan_name, ckan_groups, inspire_id = \
            self.get_dataset_common_elements(record, ckan_info.ckan_dataset_schema)

        # Get metadata record info
        if service_type == 'xml':
            layer_info = self.md_records[record]
            
        self.ows_update_metadata_sections(layer_info)
        layer_info.md_not_owslib = self.ows_get_metadata_not_owslib(layer_info)

        # Search if custom organization info exists for the dataset
        custom_metadata = None
        if self.custom_organization_active:
            custom_metadata = self.get_custom_default_metadata(layer_info.identifier)

        # Set basic info of MD
        dataset = dataset(uuid_identifier, ckan_name, self.organization, ckan_info.default_license_id)

        # Set inspireId (identifier)
        inspire_id = layer_info.uricode
        dataset.set_inspire_id(inspire_id)

        # Set private dataset
        private = getattr(self, 'private_datasets', False)
        dataset.set_private(private)

        # Set alternate identifier (layer name)
        alternate_identifier = layer_info.identifier if layer_info.identifier else None
        dataset.set_alternate_identifier(alternate_identifier)
        
        # Title
        title = custom_metadata.get('title') if custom_metadata else layer_info.identification.title
        dataset_title = title or self.localized_strings_dict['title'] if title is not None else f"{self.localized_strings_dict['title']} {inspire_id}"
        dataset.set_title(dataset_title)

        # Description
        description = custom_metadata.get('description') if custom_metadata else layer_info.identification.abstract
        dataset.set_notes(description or self.localized_strings_dict['description'])

        # CKAN Groups defined in config.yaml
        dataset.set_groups(ckan_groups)

        # Creation/Publication/Revision dates
        issued_date, modified_date = self.ows_set_metadata_dates(
                                                            dataset,
                                                            layer_info.identification
                                                            )

        # DCAT Type (dataset/series)
        dcat_type = OGC2CKAN_HARVESTER_MD_CONFIG['dcat_type']
        dataset.set_resource_type(
            dcat_type['series'] if layer_info.hierarchy == "series"
            else dcat_type['spatial_data_service'] if layer_info.hierarchy == "service"
            else dcat_type['dataset'])

        # Set SpatialRepresentationType
        representation_type = (
            layer_info.identification.spatialrepresentationtype[0]
            if layer_info.identification.spatialrepresentationtype
            else 'default'
        )
        dataset.set_representation_type(
            OGC2CKAN_HARVESTER_MD_CONFIG['representation_type'].get(
                representation_type)
            )

        # Set valid date
        if hasattr(layer_info, 'valid'):
            valid_date = layer_info.valid
        else:
            valid_date = self.get_default_dcat_info_attribute("valid")

        if valid_date and valid_date is not None:
            dataset.set_valid(self._normalize_date(valid_date))

        # Set access rights (Dataset)
        ## Unnecesary. Default rights in Dataset

        # Set SpatialResolutionInMeters if denominators exist
        denominators = getattr(layer_info.identification, 'denominators', [])
        if denominators:
            dataset.set_spatial_resolution_in_meters(denominators[0])

        # Set language
        language = getattr(layer_info, 'languagecode', OGC2CKAN_HARVESTER_MD_CONFIG['language']).replace('https:', 'http:')
        language = 'http://publications.europa.eu/resource/authority/language/' + language.upper() if 'http' not in language else language
        dataset.set_language(language)

        # Set spatial coverage
        bb = getattr(layer_info.identification, 'bbox', None)
        if bb is not None:
            self.set_bounding_box_from_iso(dataset, bb)
        else:
            dataset.set_spatial(self.get_default_dcat_info_attribute("spatial"))

       # Set spatial URI
        dataset.set_spatial_uri(self.get_custom_metadata_value(custom_metadata, 'spatial_uri'))

        # Set temporal coverage
        try:
            dataset.set_temporal_start(layer_info.identification.temporalextent_start)
            dataset.set_temporal_end(layer_info.identification.temporalextent_end)
        except AttributeError:
            dataset.set_temporal_start(self.get_custom_metadata_value(custom_metadata, 'temporal_start'))
            dataset.set_temporal_end(self.get_custom_metadata_value(custom_metadata, 'temporal_end'))

        # Set frequency
        if hasattr(layer_info, 'frequency'):
            frequency = layer_info.frequency
        else:
            frequency = OGC2CKAN_HARVESTER_MD_CONFIG['frequency']
        dataset.set_frequency(frequency)

        # Set provenance (INSPIRE Lineage)
        provenance = getattr(layer_info.dataquality, 'lineage', None)
        dataset.set_provenance(provenance)

        # Set purpose (ISO19115 MD Identification)
        purpose = getattr(layer_info.identification, 'purpose', None)
        dataset.set_purpose(self._unescape_string(purpose))

        # Set lineage_source (INSPIRE Lineage sources)
        lineage_source = layer_info.md_not_owslib.get('lineage_source', None)
        dataset.set_lineage_source(lineage_source)

        # Set process steps (INSPIRE quality)
        lineage_process_steps = layer_info.md_not_owslib.get('lineage_process_steps', None)
        dataset.set_lineage_process_steps(lineage_process_steps)

        # Set reference
        reference = getattr(layer_info, 'parentidentifier', None)
        dataset.set_reference(reference)

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
        epsg_text = layer_info.referencesystem.code if layer_info.referencesystem else None
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
        if hasattr(layer_info, "contact"):
            contact = layer_info.contact
            contact_name = getattr(contact, "name", None) or getattr(contact, "organization", None) or 'Unknown'
            dataset.set_contact_name(contact_name)
            dataset.set_contact_email(getattr(contact, "email", None))
            dataset.set_contact_url(getattr(contact, "onlineresource", None) and getattr(contact.onlineresource, "url", None))
            dataset.set_contact_uri(self._get_dir3_uri(ckan_info.dir3_soup, None, getattr(contact, "organization", None)))

        # Overwrite publisher/ISO19115 distributor
        publisher = getattr(layer_info.identification, "publisher", None) or getattr(layer_info, "distributor", None)
        if publisher is not None:
            publisher_name = getattr(publisher, "organization", None) or getattr(publisher, "name", None) or 'Unknown'
            dataset.set_publisher_name(publisher_name)
            dataset.set_publisher_email(getattr(publisher, "email", None))
            dataset.set_publisher_url(getattr(publisher, "onlineresource", None) and getattr(publisher.onlineresource, "url", None))
            dataset.set_publisher_identifier(self._get_dir3_uri(ckan_info.dir3_soup, None, getattr(publisher, "organization", None)))

        # Set license
        dataset.set_license(ckan_info.default_license)

        # Set distributions
        self.get_distribution(ckan_info, dataset, distribution, record, service_type, layer_info)

        # Metadata distributions (INSPIRE & GeoDCAT-AP)
        self.set_metadata_distributions(ckan_info, dataset, distribution, record)

        # Set default keywords/themes/topic categories
        self.set_default_keywords_themes_topic(dataset, custom_metadata, ckan_info.ckan_dataset_schema)

        # Set topic category ISO19115
        topiccategory = getattr(layer_info, "topiccategory", None)
        if topiccategory:
            dataset.keywords.append({'name': topiccategory})
            topiccategory = "/".join(["http://inspire.ec.europa.eu/metadata-codelist/TopicCategory", topiccategory])
            dataset.keywords_uri.append(topiccategory)

        # Set keywords from CSW
        self.ows_get_keywords(dataset, layer_info.identification.keywords)

        # Set translated fields if multilang is True
        if ckan_info.dataset_multilang:
            self.set_translated_fields(dataset, layer_info, language)

        return dataset

    def get_distribution(self, ckan_info: CKANInfo, dataset, distribution, record: str, service_type: str, layer_info):
        """Adds distribution information to a CKAN dataset.

        Args:
            ckan_info (CKANInfo): Object containing CKAN-related information.
            dataset (CKANPackage): CKAN package object to add distribution information to.
            distribution (object): Object containing distribution information.
            record (str): Identifier for the metadata record.
            service_type (str): Type of service.
            layer_info (object): Object containing metadata information.

        Returns:
            None
        """
        # Add distributions
        dataset.set_distributions([])

        # Add distributions from CSW
        for i, r in enumerate(layer_info.distribution):
            distribution_id = str(uuid.uuid4())
            try:
                # Set distribution format from MD
                dist_info = {
                    "function": r.function,
                    "url": r.url,
                    "description": r.description,
                    "name": r.name,
                    "protocol": r.protocol,
                }

                format_type, media_type, conformance, default_name = self._get_ckan_format(dist_info)
                dist_name = r.name or default_name

            except Exception:
                logging.error(f"{log_module}:Error processing distribution '{i}' of '{record}'. Asign default values.")
                format_type, media_type, conformance, dist_name = None, None, [], r.name or f"{self.localized_strings_dict['distributions']['default']} {dataset.title}."

            try:
                distribution_data = {
                    'id': distribution_id,
                    'url': r.url or '',
                    'name': dist_name,
                    'format': format_type,
                    'media_type': media_type,
                    'description': r.description or None,
                    'license': dataset.license,
                    'license_id': dataset.license_id,
                    'rights': dataset.access_rights,
                    'language': dataset.language,
                    'conformance': conformance
                }

                dataset.add_distribution(distribution(**distribution_data))

            except Exception as e:
                logging.error(f"{log_module}:Error adding distribution '{dist_name}' to dataset '{dataset.title}': {e}")
