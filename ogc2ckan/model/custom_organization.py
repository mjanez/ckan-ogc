# inbuilt libraries
import yaml

# custom functions
from mappings.default_ogc2ckan_config import OGC2CKAN_PATHS_CONFIG


class CustomOrganization:
    def __init__(self, harvest_server):
        """
        Initializes a new instance of the CustomOrganization class.

        Args:
            harvest_server (HarvestServer): The HarvestServer object containing the custom organization mapping file.

        Attributes:
            custom_organization_mapping_file (str): The path of the custom organization mapping file with the .yaml extension added if necessary.
            mapping_file_path (str): The full path of the custom organization mapping file.
            mapping_values (dict): The values from the "mapping_values" section of the custom organization mapping file, or an empty dictionary if the section is not found.
        """
        self.custom_organization_mapping_file = self._add_yaml_extension(harvest_server.custom_organization_mapping_file)
        self.mapping_file_path = f"{harvest_server.app_dir}/{OGC2CKAN_PATHS_CONFIG['default_mappings_folder']}/organizations/{harvest_server.custom_organization_mapping_file}"
        self.mapping_values = self.config_get_parameters(self.mapping_file_path)

    def config_get_parameters(self, mapping_file_path):
        """
        Reads the specified YAML file and returns the required parameters.

        Args:
            mapping_file_path (str): The path of the YAML file to read.

        Returns:
            dict: The values from the "mapping_values" section of the YAML file, or an empty dictionary if the section is not found.
        """
        with open(mapping_file_path, encoding="utf-8") as stream:
            config = yaml.safe_load(stream)
            mapping_values = config.get("mapping_values", {})

        return mapping_values

    def find_mapping_value(self, search_value: str, search_property: str = "id"):
        """
        Finds a value in the mappings and returns the corresponding property.

        Args:
            search_value (str): The value to search for in the mappings.
            search_property (str, optional): The name of the property to search for. Defaults to "id".

        Returns:
            dict or None: The dictionary object that matches the search value, or None if not found.
        """
        mapping = next((item for item in self.mapping_values if item[search_property] == search_value), None)
        
        return mapping
    
    def find_similar_mapping_value(self, search_value: str, search_property: str = "id"):
        """
        Finds a value in the mappings and returns the corresponding property.

        Args:
            search_value (str): The value to search for in the mappings.
            search_property (str, optional): The name of the property to search for. Defaults to "id".

        Returns:
            dict or None: The dictionary object that matches the search value, or None if not found.
        """
        mapping = next((item for item in self.mapping_values if item[search_property] in search_value), None)
        
        return mapping

    @staticmethod
    def _find_keyword_in_default_keywords(keyword, mapping):
        """
        Finds a keyword in the "default_keywords" section of the given mapping and returns the corresponding dataset ID.

        Args:
            keyword (str): The keyword to search for in the "default_keywords" section.
            mapping (dict): The mapping to search for the keyword.

        Returns:
            str or None: The dataset ID that corresponds to the keyword, or None if the keyword is not found.
        """
        for dataset_id, dataset_info in mapping.items():
            if keyword in dataset_info.get("default_keywords", []):
                return dataset_id
        
        return None

    @staticmethod
    def _add_yaml_extension(file_path):
        """
        Adds the .yaml extension to the file path if it does not already end with .yaml or .yml.

        Args:
            file_path (str): The path of the file.

        Returns:
            str: The file path with the .yaml extension added if necessary.
        """
        if not file_path.endswith(('.yaml', '.yml')):
            file_path += '.yaml'
        
        return file_path