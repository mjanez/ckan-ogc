# inbuilt libraries
import yaml
import os
import pandas as pd
import os

# custom functions
from mappings.default_ogc2ckan_config import OGC2CKAN_PATHS_CONFIG
from ogc2ckan import APP_DIR

default_mappings_folder = f"{APP_DIR}/{OGC2CKAN_PATHS_CONFIG['default_mappings_folder']}"

class MappingValueNotFoundError(Exception):
    def __init__(self, value, codelist):
        self.value = value
        self.codelist = codelist
        super().__init__(
            f"Mapping value {self.value}  not found in {self.codelist}.yaml"
            )

# TODO: Retornar valor de un YAML, sabiendo valor y campo que se ha de retornar.
def get_mapping_value(
    value: str,
    codelist: str,
    field_map: str,
    mappings_folder: str = default_mappings_folder):
    """
    Returns the mapping value in YAML for a given codelist value. 

    This function loads a YAML file from the specified mappings folder and returns the value
    for the specified codelist value. If the value is not found in the YAML file, the category itself is returned.
    
    Parameters
    ----------
    value: str. The source value that needs to be mapped to a codelist value.
    codelist : str. The name of the YAML file (without the extension) in which the codelist is defined.
    field_map: str. Name of the field to return.
    mappings_folder: str (default="ckan2pycsw/mappings"). The folder path containing the YAML files for the mappings.

    Return
    ----------
    The value of the codelist value if found in the YAML file, else the value itself.

    Raises
    ------
    MappingValueNotFoundError: ValueError. If the given value is not found in the mapping.
    """
    try:
        yaml_path = os.path.join(mappings_folder, codelist + ".yaml")
        with open(yaml_path, 'r', encoding="utf-8") as file:
            map_yaml = yaml.safe_load(file)
            if not isinstance(map_yaml, list):
                raise ValueError("The YAML file does not contain a valid list.")
    except ValueError:
        raise MappingValueNotFoundError(value, codelist) from None

    for mapping in map_yaml:
        if isinstance(mapping, dict) and 'id' in mapping and mapping['id'] == value:
            return mapping.get(field_map, value)

    return value


def get_df_mapping_json(mapping_file: str, mappings_folder: str = default_mappings_folder) -> pd.DataFrame:
    """
    Imports the JSON mapping file from the specified file path.

    Parameters:
        mapping_file (str): The name of the JSON mapping file to import.
        mappings_folder (str): The path to the folder containing the JSON mapping file to import.

    Returns:
        pandas.DataFrame: The JSON mapping data as a pandas DataFrame.
    """
    if not mapping_file.endswith('.json'):
        mapping_file += '.json'
    file_path = os.path.join(mappings_folder, mapping_file)
    json_dataframe = pd.read_json(file_path, encoding='utf-8', dtype=str)
    
    return json_dataframe