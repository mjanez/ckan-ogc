# Metadata of a Dataset and Distributions. CKAN Fields https://project-open-data.cio.gov/v1.1/metadata-resources/ // https://github.com/project-open-data/project-open-data.github.io/blob/f136070aa9fea595277f6ebd1cd66f57ff504dfd/v1.1/metadata-resources.md

## Import libraries   
import urllib.parse
import json


class Distribution:
    # Dataset Distribution Fields: https://github.com/project-open-data/project-open-data.github.io/blob/master/v1.1/metadata-resources.md#dataset-distribution-fields
    def __init__(self, url, name, format, id=None, created=None, issued=None, modified=None, media_type=None, license=None, license_id=None, rights=None, description=None, language=None, conformance=None, reference_system=None, encoding='UTF-8'):
        self.url = url
        self.id = id
        self.name = name
        self.format = format
        self.media_type = media_type
        self.license = license
        self.license_id = license_id
        self.rights = rights
        self.description = description
        self.language = language
        self.created = created
        self.issued = issued
        self.modified = modified
        self.conformance = conformance
        self.encoding = encoding
        self.reference_system = reference_system

    def set_url(self, url):
        self.url = url

    def set_id(self, id):
        self.id = id

    def set_name(self, name):
        self.name = name

    def set_format(self, format):
        self.format = format

    def set_media_type(self, media_type):
        self.media_type = media_type

    def set_license(self, license):
        self.license = license

    def set_license_id(self, license_id):
        self.license_id = license_id

    def set_rights(self, rights):
        self.rights = rights

    def set_notes(self, description):
        self.description = description

    def set_language(self, language):
        self.language = language

    def set_created(self, created):
        self.created = created

    def set_issued(self, issued):
        self.issued = issued

    def set_modified(self, modified):
        self.modified = modified

    def set_conformance(self, conformance):
        self.conformance = conformance

    def set_reference_system(self, reference_system):
        self.reference_system = reference_system

    def set_encoding(self, encoding):
        self.encoding = encoding

    def to_dict(self):
        return {'url': self.url,
                'name': self.name,
                'id': self.id,
                'format': self.format,
                'mimetype': self.media_type,
                'license': self.license,
                'license_id': self.license_id,
                'rights': self.rights,
                'description': self.description,
                'language': self.language,
                'created': self.created,
                'issued': self.issued,
                'modified': self.modified,  
                'conforms_to': self.conformance,
                'encoding': self.encoding,
                'reference_system': self.reference_system,           
                }

class Dataset:
    # Dataset fields: https://github.com/project-open-data/project-open-data.github.io/blob/master/v1.1/metadata-resources.md#dataset-fields
    def __init__(self, ckan_id, name, owner_org, license_id):
        # initialization of default values
        self.name = name
        self.ckan_id = ckan_id
        self.ogc_workspace = None # Only for OGC datasets
        self.publisher_uri = None
        self.publisher_name = None
        self.publisher_url = None
        self.publisher_email = None
        self.publisher_identifier = None
        self.publisher_type = None
        self.owner_org = owner_org
        self.private = False
        self.groups = []
        self.graphic_overview = None
        # use http://<ckan_url>/api/action/organization_list to see the organization ids in your CKAN site
        self.license_id = license_id
        self.identifier = ckan_id
        self.alternate_identifier = None
        self.title = None
        self.title_translated = None
        self.notes = None
        self.notes_translated = None
        self.dcat_type = None
        self.inspire_id = None
        self.access_rights = "http://inspire.ec.europa.eu/metadata-codelist/LimitationsOnPublicAccess/noLimitations"
        self.representation_type = None
        self.version_notes = None
        self.spatial_resolution_in_meters = None
        self.language = None
        self.theme = None
        self.theme_es = None
        self.theme_eu = None
        self.topic = "http://inspire.ec.europa.eu/metadata-codelist/TopicCategory/biota"
        self.keywords = []
        self.keywords_uri = []
        self.keywords_thesaurus = []
        self.reference_system = None
        self.spatial = None
        self.spatial_uri = None
        self.temporal_start = None
        self.temporal_end = None
        self.frequency = None
        self.created = None
        self.issued = None
        self.modified = None
        self.valid = None
        self.provenance = None
        self.purpose = None
        self.lineage_source = []
        self.lineage_process_steps = []
        self.source = None
        self.reference = []
        self.conformance = []
        self.metadata_profile = [
            "http://semiceu.github.io/GeoDCAT-AP/releases/2.0.0","http://inspire.ec.europa.eu/document-tags/metadata"
        ]
        self.encoding = 'UTF-8'
        self.contact_uri = None
        self.contact_url = None
        self.contact_name = None
        self.contact_email = None
        self.maintainer_name = None
        self.maintainer_uri = None
        self.maintainer_email = None
        self.maintainer_url = None
        self.author_name = None
        self.author_uri = None
        self.author_email = None
        self.author_url = None
        self.distributions = []
        self.license = None

    def set_name(self, name):
        self.name = name

    def set_ckan_id(self, ckan_id):
        self.ckan_id = ckan_id

    def set_ogc_workspace(self, ogc_workspace):
        self.ogc_workspace = ogc_workspace
    
    def set_private(self, private):
        self.private = private

    def set_groups(self, groups):
        self.groups = groups

    def set_graphic_overview(self, graphic_overview):
        self.graphic_overview = graphic_overview

    def set_publisher_uri(self, publisher_uri):
        self.publisher_uri = publisher_uri

    def set_publisher_name(self, publisher_name):
        self.publisher_name = publisher_name
    
    def set_publisher_url(self, publisher_url):
        self.publisher_url = publisher_url

    def set_publisher_email(self, publisher_email):
        self.publisher_email = publisher_email

    def set_publisher_identifier(self, publisher_identifier):
        self.publisher_identifier = publisher_identifier

    def set_publisher_type(self, publisher_type):
        self.publisher_type = publisher_type

    def set_identifier(self, identifier):
        self.identifier = identifier

    def set_alternate_identifier(self, alternate_identifier):
        self.alternate_identifier = alternate_identifier

    def set_title(self, title):
        self.title = title

    def set_title_translated(self, title_translated):
        self.title_translated = title_translated

    def set_notes(self, notes):
        self.notes = notes
        
    def set_notes_translated(self, notes_translated):
        self.notes_translated = notes_translated

    def set_resource_type(self, resource_type):
        self.dcat_type = resource_type

    def set_inspire_id(self, inspire_id):
        self.inspire_id = inspire_id

    def set_access_rights(self, access_rights):
        self.access_rights = access_rights

    def set_representation_type(self, representation_type):
        self.representation_type = representation_type

    def set_version_notes(self, version_notes):
        self.version_notes = version_notes

    def set_spatial_resolution_in_meters(self, spatial_resolution_in_meters):
        self.spatial_resolution_in_meters = spatial_resolution_in_meters

    def set_language(self, language):
        self.language = language

    def set_theme(self, theme):
        self.theme = theme

    def set_theme_es(self, theme_es):
        self.theme_es = theme_es
        
    def set_theme_eu(self, theme_eu):
        self.theme_eu = theme_eu

    def set_topic(self, topic):
        self.topic = topic

    def set_keywords(self, keywords):
        self.keywords = keywords

    def set_keywords_uri(self, keywords_uri):
        self.keywords_uri = keywords_uri
        
    def set_keywords_thesaurus(self, keywords_thesaurus):
        self.keywords_thesaurus = keywords_thesaurus

    def set_reference_system(self, reference_system):
        self.reference_system = reference_system

    def set_spatial(self, spatial):
        self.spatial = spatial

    def set_spatial_uri(self, spatial_uri):
        self.spatial_uri = spatial_uri

    def set_temporal_start(self, temporal_start):
        self.temporal_start = temporal_start

    def set_temporal_end(self, temporal_end):
        self.temporal_end = temporal_end

    def set_frequency(self, frequency):
        self.frequency = frequency

    def set_created(self, created):
        self.created = created

    def set_issued(self, issued):
        self.issued = issued

    def set_valid(self, valid):
        self.valid = valid

    def set_modified(self, modified):
        self.modified = modified

    def set_provenance(self, provenance):
        self.provenance = provenance

    def set_purpose(self, purpose):
        self.purpose = purpose

    def set_lineage_source(self, lineage_source):
        self.lineage_source = lineage_source

    def set_lineage_process_steps(self, lineage_process_steps):
        self.lineage_process_steps = lineage_process_steps

    def set_source(self, source):
        self.source = source

    def set_reference(self, reference):
        self.reference = reference

    def set_conformance(self, conformance):
        self.conformance = conformance

    def set_metadata_profile(self, metadata_profile):
        self.metadata_profile = metadata_profile

    def set_encoding(self, encoding):
        self.encoding = encoding

    def set_contact_uri(self, contact_uri):
        self.contact_uri = contact_uri      

    def set_contact_url(self, contact_url):
        self.contact_url = contact_url   

    def set_contact_name(self, contact_name):
        self.contact_name = contact_name

    def set_contact_email(self, contact_email):
        self.contact_email = contact_email

    def set_maintainer_name(self, maintainer_name):
        self.maintainer_name = maintainer_name

    def set_maintainer_uri(self, maintainer_uri):
        self.maintainer_uri = maintainer_uri  

    def set_maintainer_email(self, maintainer_email):
        self.maintainer_email = maintainer_email

    def set_maintainer_url(self, maintainer_url):
        self.maintainer_url = maintainer_url  

    def set_author_name(self, author_name):
        self.author_name = author_name

    def set_author_uri(self, author_uri):
        self.author_uri = author_uri    

    def set_author_email(self, author_email):
        self.author_email = author_email

    def set_author_url(self, author_url):
        self.author_url = author_url    

    def set_distributions(self, distributions):
        self.distributions = distributions

    def add_distribution(self, distribution):
        self.distributions.append(distribution)

    def set_license(self, license):
        self.license = license

    def set_license_id(self, license_id):
        self.license_id = license_id

    def dataset_dict(self):
        '''    
        CKAN API 'package_create': https://docs.ckan.org/en/2.9/api/index.html#ckan.logic.action.create.package_create
            package_create(
                name = NULL,
                title = NULL,
                private = FALSE,
                author = NULL,
                author_email = NULL,
                maintainer = NULL,
                maintainer_email = NULL,
                license_id = NULL,
                notes = NULL,
                package_url = NULL,
                version = NULL,
                state = "active",
                type = NULL,
                resources = NULL,
                tags = NULL,
                extras = NULL,
                relationships_as_object = NULL,
                relationships_as_subject = NULL,
                groups = NULL,
                owner_org = NULL,
                url = get_default_url(),
                key = get_default_key(),
                as = "list",
                ...
                )
        '''

        # Put the details of the dataset we're going to create into a dict.
        dataset_dict = {
            'id': self.ckan_id,
            'name': self.name,
            'owner_org': self.owner_org,
            'private': self.private,
            'groups': self.groups,
            'graphic_overview': self.graphic_overview,
            'title': self.title,
            'notes': self.notes,
            'license_id': self.license_id,
            'topic': self.topic,
            'tags': self.keywords,
            'tag_uri': self.keywords_uri,
            #TODO: Add tag_thesaurus to CKAN Schema
            #'tag_thesaurus': self.keywords_thesaurus,
            'dcat_type': self.dcat_type,
            'alternate_identifier': self.alternate_identifier,
            'representation_type': self.representation_type,
            'access_rights': self.access_rights,
            'inspire_id': self.inspire_id,
            'version_notes': self.version_notes,
            'spatial_resolution_in_meters': self.spatial_resolution_in_meters,
            'language': self.language,
            'theme_es': self.theme_es,
            'theme_eu': self.theme_eu,
            'theme': self.theme,
            'identifier': self.identifier,
            'provenance': self.provenance,
            'purpose': self.purpose,
            'lineage_source': self.lineage_source,
            'lineage_process_steps': self.lineage_process_steps,
            'source': self.source,
            'frequency': self.frequency,
            'reference': self.reference,
            'conforms_to': self.conformance,
            'metadata_profile': self.metadata_profile,
            'encoding': self.encoding,
            'reference_system': self.reference_system,
            'spatial': self.spatial,
            'spatial_uri': self.spatial_uri,
            'publisher_uri': self.publisher_uri,
            'publisher_name': self.publisher_name,
            'publisher_url': self.publisher_url,
            'publisher_email': self.publisher_email,
            'publisher_identifier': self.publisher_identifier,
            'publisher_type': self.publisher_type,
            'contact_uri': self.contact_uri,
            'contact_url': self.contact_url,
            'contact_name': self.contact_name,
            'contact_email': self.contact_email,
            'maintainer': self.maintainer_name,
            'maintainer_uri': self.maintainer_uri,
            'maintainer_url': self.maintainer_url,
            'maintainer_email': self.maintainer_email,
            'author': self.author_name,
            'author_uri': self.author_uri,
            'author_email': self.author_email,
            'author_url': self.author_url,
            'created': self.created,
            'modified': self.modified,
            'temporal_start': self.temporal_start,
            'temporal_end': self.temporal_end,
            'valid': self.valid,
            'extras': [
            {'key': 'issued', 'value': self.issued},
        ],
            'resources': [i.to_dict() for i in self.distributions]
        }

        return dataset_dict


    def dataset_dict_multilang(self):
        '''    
        CKAN API 'package_create': https://docs.ckan.org/en/2.9/api/index.html#ckan.logic.action.create.package_create
            package_create(
                name = NULL,
                title_translated = NULL,
                private = FALSE,
                author = NULL,
                author_email = NULL,
                maintainer = NULL,
                maintainer_email = NULL,
                license_id = NULL,
                notes_translated = NULL,
                package_url = NULL,
                version = NULL,
                state = "active",
                type = NULL,
                resources = NULL,
                tags = NULL,
                extras = NULL,
                relationships_as_object = NULL,
                relationships_as_subject = NULL,
                groups = NULL,
                owner_org = NULL,
                url = get_default_url(),
                key = get_default_key(),
                as = "list",
                ...
                )
        '''

        # Put the details of the dataset we're going to create into a dict.
        dataset_dict = {
            'id': self.ckan_id,
            'name': self.name,
            'owner_org': self.owner_org,
            'private': self.private,
            'groups': self.groups,
            'graphic_overview': self.graphic_overview,
            'title_translated': self.title_translated,
            'notes_translated': self.notes_translated,
            'license_id': self.license_id,
            'topic': self.topic,
            'tags': self.keywords,
            'tag_uri': self.keywords_uri,
            #TODO: Add tag_thesaurus to CKAN Schema
            #'tag_thesaurus': self.keywords_thesaurus,
            'dcat_type': self.dcat_type,
            'alternate_identifier': self.alternate_identifier,
            'representation_type': self.representation_type,
            'access_rights': self.access_rights,
            'inspire_id': self.inspire_id,
            'version_notes': self.version_notes,
            'spatial_resolution_in_meters': self.spatial_resolution_in_meters,
            'language': self.language,
            'theme_es': self.theme_es,
            'theme_eu': self.theme_eu,
            'theme': self.theme,
            'identifier': self.identifier,
            'provenance': self.provenance,
            'purpose': self.purpose,
            'lineage_source': self.lineage_source,
            'lineage_process_steps': self.lineage_process_steps,
            'source': self.source,
            'frequency': self.frequency,
            'reference': self.reference,
            'conforms_to': self.conformance,
            'metadata_profile': self.metadata_profile,
            'encoding': self.encoding,
            'reference_system': self.reference_system,
            'spatial': self.spatial,
            'spatial_uri': self.spatial_uri,
            'publisher_uri': self.publisher_uri,
            'publisher_name': self.publisher_name,
            'publisher_url': self.publisher_url,
            'publisher_email': self.publisher_email,
            'publisher_identifier': self.publisher_identifier,
            'publisher_type': self.publisher_type,
            'contact_uri': self.contact_uri,
            'contact_url': self.contact_url,
            'contact_name': self.contact_name,
            'contact_email': self.contact_email,
            'maintainer': self.maintainer_name,
            'maintainer_uri': self.maintainer_uri,
            'maintainer_url': self.maintainer_url,
            'maintainer_email': self.maintainer_email,
            'author': self.author_name,
            'author_uri': self.author_uri,
            'author_email': self.author_email,
            'author_url': self.author_url,
            'created': self.created,
            'modified': self.modified,
            'temporal_start': self.temporal_start,
            'temporal_end': self.temporal_end,
            'valid': self.valid,
            'extras': [
            {'key': 'issued', 'value': self.issued},
        ],
            'resources': [i.to_dict() for i in self.distributions]
        }

        return dataset_dict

    def generate_data(self, multilang: bool = False):
        if multilang is True:
            dataset_dict = self.dataset_dict_multilang()
        else:
            dataset_dict = self.dataset_dict()
        # Use the json module to dump the dictionary to a string for posting.
        quoted_data = urllib.parse.quote(json.dumps(dataset_dict))
        byte_data = quoted_data.encode('utf-8')
        return byte_data