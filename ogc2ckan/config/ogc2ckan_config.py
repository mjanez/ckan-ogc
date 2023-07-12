# inbuilt libraries
import os
import yaml
import logging

def get_log_module():
    # Get the directory and file name of the current file
    dir_path, file_name_ext = os.path.split(os.path.abspath(__file__))
    
    # Split the file name and extension
    file_name, file_ext = os.path.splitext(file_name_ext)

    # Create the log module string
    log_module = f"[{os.path.basename(dir_path)}.{file_name}]"
    
    return log_module

log_module = get_log_module()

def load_yaml(yaml_file_path):
    try:
        with open(yaml_file_path, "r") as file:
            yaml_data = yaml.safe_load(file)
        return yaml_data
    
    except FileNotFoundError:
        logging.error(f"{log_module}:YAML not found: {yaml_file_path}'")
        return {}
    
    except yaml.YAMLError as e:
        logging.error(f"{log_module}:Error loading the YAML: {str(e)}")
        return {}