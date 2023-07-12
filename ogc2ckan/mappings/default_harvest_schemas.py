

SCHEMA_HARVESTER = {
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
            "enum": ["csw", "ogc", "table"]
            },
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
                "publisher_type": {"type": "string"}
            },
            "required": ["publisher_name", "publisher_email", "publisher_identifier", "publisher_url", "publisher_type"]
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
    "required": ["url", "name", "groups", "active", "type", "organization", "custom_organization_active", "custom_organization_mapping_file", "private_datasets", "default_dcat_info","default_keywords", "default_inspire_info"]
}
