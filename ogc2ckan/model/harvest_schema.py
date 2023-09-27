
# inbuilt libraries
import logging
import yaml
import os

# third-party libraries
from jsonschema import validate, ValidationError

# custom functions
from config.ogc2ckan_config import get_log_module
from mappings.default_ogc2ckan_config import OGC2CKAN_HARVESTER_CONFIG


log_module = get_log_module(os.path.abspath(__file__))

# Harvester Schema
class HarvesterSchema:
    schema = {
        "type": "object",
        "properties": {
            "url": {"type": "string"},
            "name": {"type": "string"},
            "groups": {
                "type": "array",
                "items": {"type": "string"}
                },
            "active": {"type": "boolean"},
            "type": {
                "type": "string", 
                "enum": [value['type'] for key, value in OGC2CKAN_HARVESTER_CONFIG.items()]
                },
            "ckan_name_not_uuid": {"type": "boolean"},
            "organization": {"type": "string"},
            "custom_organization_active": {"type": "boolean"},
            "custom_organization_mapping_file": {"type": "string"},
            "private_datasets": {"type": "boolean"},
            "default_dcat_info": {
                "type": "object",
                "properties": {
                    "publisher_name": {"type": "string"},
                    "publisher_email": {"type": "string"},
                    "publisher_identifier": {"type": "string"},
                    "publisher_url": {"type": "string"},
                    "publisher_type": {"type": "string"},
                    "contact_name": {"type": "string"},
                    "contact_email": {"type": "string"},
                    "contact_uri": {"type": "string"},
                    "contact_url": {"type": "string"},
                    "topic": {"type": "string"},
                    "theme_es": {"type": "string"},
                    "theme_eu": {"type": "string"},
                    "spatial": {"type": "string"},
                    "spatial_uri": {"type": "string"},
                    "language": {"type": "string"},
                },
                "required": ["publisher_name", "publisher_email", "publisher_identifier", "publisher_url", "publisher_type", "contact_name", "contact_email", "contact_uri", "contact_url", "topic", "theme_eu", "spatial", "spatial_uri", "language"]
            },
            "default_keywords": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "uri": {"type": "string"}
                    },
                    "required": ["name", "uri"]
                }
            },
            "default_inspire_info": {
                "type": "object",
                "properties": {
                    "inspireid_theme": {"type": "string"},
                    "inspireid_nutscode": {"type": "string"},
                    "inspireid_versionid": {"type": "string"}
                },
                "required": ["inspireid_theme", "inspireid_nutscode", "inspireid_versionid"]
            },
        },
        "required": ["url", "name", "groups", "active", "type", "ckan_name_not_uuid", "organization", "custom_organization_active", "custom_organization_mapping_file", "private_datasets", "default_dcat_info","default_keywords", "default_inspire_info"]
    }

class HarvesterSchemaCSW(HarvesterSchema):
    schema = {
        **HarvesterSchema.schema,
        "properties": {
            **HarvesterSchema.schema["properties"],
            "constraints": {
                "type": "object",
                "properties": {
                    "keywords": {"type": "array"},
                    "mails": {"type": "array"},
                    "inspireid_versionid": {"type": "string"}
                },
                "required": ["keywords", "mails"]
            }
        },
        "required": HarvesterSchema.schema["required"] + ["constraints"]
    }

class HarvesterSchemaOGC(HarvesterSchema):
    schema = {
        **HarvesterSchema.schema,
        "properties": {
            **HarvesterSchema.schema["properties"],
            "workspaces": {
                "type": "array",
                "items": {"type": "string"}
            }
        },
        "required": HarvesterSchema.schema["required"] + ["workspaces"]
    }

def validate_config_file(file_name):
    # Load the YAML
    with open(file_name, "r") as f:
        config = yaml.safe_load(f)

    # Define the schema dictionary
    schema_dict = {
        "csw": HarvesterSchemaCSW.schema,
        "ogc": HarvesterSchemaOGC.schema,
        "xml": HarvesterSchema.schema,
        "table": HarvesterSchema.schema
    }

    # Validate all the objects in the YAML
    for obj in config['harvest_servers']:
        try:
            schema = schema_dict[obj["type"]]
            validate(obj, schema)
        except (KeyError, ValidationError) as e:
            logging.error(f"Schema validation error in the configuration file: {file_name} Error: {e}")
            return False

    return True