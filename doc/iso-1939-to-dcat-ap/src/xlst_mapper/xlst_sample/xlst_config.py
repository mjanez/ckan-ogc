#!/usr/bin/env python3
## File: xlst_config.py
## Coding: UTF-8
## Author: Manuel Ángel Jáñez García (mjanez@tragsa.es)
## Institution: Tragsatec
## Project: EIKOS
## Goal: The goal of this script is is to provide config details .
## Parent: iacsdb_config.py 
""" Changelog:
    v1.0 - 8 Jul 2022: Create the first version
"""
# Update the version when apply changes
version = "1.0"

##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##
##            xlst_config.py            ##
##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##

## Import libraries   
import psycopg2
import yaml
from functools import reduce


# Configuration data
def config_getParameters(config_file):
    """Read the --config_file parameter entered and return the required parameters from the YAML 
    
    Required Parameters:
        --config_file: config.yml 
    """
    # Import config_shp.yml parameters
    def get_config_Valor(key, cfg):
        """Read the YAML 
    
        Optional Parameters:
            --key: Key
            --cfg: Config element
        """
        return reduce(lambda c, k: c[k], key.split('.'), cfg)

    with open(config_file) as stream:
        config = yaml.safe_load(stream)
        log_folder = get_config_Valor('default.log_folder', config)
        output_folder = get_config_Valor('default.output_folder', config)
        inspire_info = get_config_Valor('inspire_info', config) 

    return log_folder, output_folder, inspire_info
