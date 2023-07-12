# custom functions
from mappings.default_ogc2ckan_config import OGC2CKAN_HARVESTER_CONFIG


def get_harvester_class(harvest_type, url):
    """Get the harvester class corresponding to the specified harvest type.

    Args:
        harvest_type (str): The harvest type for which to get the harvester class.

    Returns:
        class: The harvester class corresponding to the specified harvest type. If no class is found corresponding to the
        specified harvest type, the default class is returned.

    Raises:
        None
    """
    from harvesters.base import Harvester
    from harvesters.csw import HarvesterCSW
    from harvesters.ogc import HarvesterOGC
    from harvesters.table import HarvesterTable  
    
    HARVESTER_DICT = {
        'csw': HarvesterCSW,
        'ogc': HarvesterOGC,
        'table': HarvesterTable,
        'default': Harvester,
    }
    
    # Last verification
    if harvest_type not in HARVESTER_DICT:
        if any(ext in url for ext in OGC2CKAN_HARVESTER_CONFIG['csw_server']['keywords']):
            harvest_type = "csw"
            
        elif any(ext in url for ext in OGC2CKAN_HARVESTER_CONFIG['ogc_server']['keywords']):
            harvest_type = "ogc"

        elif any(ext in url for ext in OGC2CKAN_HARVESTER_CONFIG['table']['keywords']):
            harvest_type = "table"
                
    return HARVESTER_DICT.get(harvest_type, HARVESTER_DICT['default'])