# inbuilt libraries
import logging
from datetime import datetime   
import os
from joblib import Parallel, delayed
import ssl
import json

# custom classes
from controller import ckan_management

# custom functions
from model.harvest_schema import validate_config_file
from config.ckan_config import config_getParameters, config_getConnection
from config.log import log_file
from mappings.default_ogc2ckan_config import OGC2CKAN_HARVESTER_CONFIG

# debug
import ptvsd


# Ennvars
TZ = os.environ.get("TZ", "TZ")
DEV_MODE = None
VERSION = os.environ.get("VERSION", "0.1")
CKAN_OGC_DEV_PORT = os.environ.get("CKAN_OGC_DEV_PORT", 5678)
APP_DIR = os.environ.get("APP_DIR", "/app")
config_file = os.path.abspath(APP_DIR + "/config.yaml")
log_module = "[ogc2ckan]"

def launch_harvest(harvest_server=None, ckan_info=None):
    """
    Launch harvesting process depending on the type of harvester

    :param harvest_server: Harvest server parameters
    :param ckan_info: CKAN Parameters from config.yaml

    :return: CSW Records and CKAN New records counters
    """
    from harvesters.base import Harvester
    start = datetime.now()

    logging.info(f"{log_module}:Harvester: '{harvest_server.name}' ('{harvest_server.type.upper()}') with url/path: '{harvest_server.url}' and CKAN organization: {ckan_info.ckan_site_url}/organization/{harvest_server.organization}") 


    harvester = Harvester.from_harvest_server(harvest_server, APP_DIR)

    try:
        harvester.create_datasets(ckan_info)

        # Output info
        end = datetime.now()
        diff = end - start

        # Log CKAN Datasets with conflicts
        logging.info(log_module + ":" + harvest_server["name"] + " (" + harvester.type.upper() + ") dataset records retrieved (" + str(harvester.source_dataset_count) + ") with conflicts: (" + str(len(harvester.ckan_dataset_errors)) + ") from ('" + harvester.type.upper() + "')")
        
        if harvester.ckan_dataset_errors:
            logging.info(log_module + ":" + "Check Datasets with conflicts by 'title': " + json.dumps(harvester.ckan_dataset_errors, ensure_ascii=False))
        
        # Log CKAN Data Dictionaries with conflicts
        if harvester.source_dictionaries_count > 0 or harvester.ckan_dictionaries_count > 0:
            logging.info(log_module + ":" + harvest_server["name"] + " (" + harvester.type.upper() + ") data dictionaries retrieved (" + str(harvester.source_dictionaries_count) + ") with conflicts: (" + str(len(harvester.ckan_dictionaries_errors)) + ") from ('" + harvester.type.upper() + "')")
            
            if harvester.ckan_dictionaries_errors:
                logging.info(log_module + ":" + "Check Data dictionaries with conflicts by 'resource_id': " + json.dumps(harvester.ckan_dictionaries_errors, ensure_ascii=False))
        
        logging.info(log_module + ":" + harvest_server["name"] + " (" + harvester.type.upper() + ") server time elapsed: " + str(diff).split(".")[0])
    
    except Exception as e:
        logging.exception("An exception occurred!")

        # Output info
        end = datetime.now()
        diff = end - start

        logging.error(log_module + ":" + harvest_server["name"] + " (" + harvester.type.upper() + ") server: " + harvest_server['url'] + ' failed connection.')
        print("ERROR::" + harvest_server["name"] + " (" + harvester.type.upper() + ") server: " + harvest_server['url'] + ' failed connection.')

    return harvester

def setup_logging(log_module, VERSION):
    ssl._create_default_https_context = ssl._create_unverified_context
    log_file(APP_DIR + "/log")
    logging.info(f"{log_module}:Version: {VERSION}")
    return datetime.now()

def validate_configuration(config_file):
    if not validate_config_file(config_file):
        raise Exception(f"{log_module}:'{config_file}' does not comply with the schemas  of: 'ogc2ckan/model/harvest_schema.py'. Check it!")
    else:
        logging.info(f"{log_module}:The 'config_file': '{config_file}' comply with the schemas of: 'ogc2ckan/model/harvest_schema.py'")

def start_harvesting(config_file):
    ckan_info, harvest_servers, db_dsn = config_getParameters(config_file)
    processes = os.cpu_count() - 1
    new_records = []

    if ckan_info.ckan_harvester is not None:
        active_harvesters = [h["type"] for h in ckan_info.ckan_harvester.values() if h['active'] is True]
        harvest_servers = [e for e in harvest_servers if e['type'] in active_harvesters and e['active'] is True]
        
        if not harvest_servers:
            error_message = f"{log_module}:No active harvest servers found for types: [{', '.join([OGC2CKAN_HARVESTER_CONFIG[key]['type'] for key in OGC2CKAN_HARVESTER_CONFIG])}]."
            raise ValueError(error_message)

        logging.info(f"{log_module}:Number of processes: {processes}")
        logging.info(f"{log_module}:Multicore parallel processing: '{ckan_info.parallelization}'")
        
        if ckan_info.ssl_unverified_mode == True or ckan_info.ssl_unverified_mode == "True":
            logging.warning(f"{log_module}:[INSECURE] SSL_UNVERIFIED_MODE:'{ckan_info.ssl_unverified_mode}'. Only if you trust the host.")    
        
        if ckan_info.metadata_distributions == True or ckan_info.metadata_distributions == "True":
            logging.warning(f"{log_module}:METADATA_DISTRIBUTIONS:'{ckan_info.metadata_distributions}'. It is not necessary if you do not intend to generate distributions for geographic metadata (INSPIRE ISO19139) or Linked Open Data (GeoDCAT-AP). ckanext-scheming_dcat already links the most important metadata profiles (https://github.com/mjanez/ckanext-scheming_dcat).")
        
        logging.info(f"{log_module}:Type of activated harvesters: {', '.join([f'{h.upper()}' for h in active_harvesters])}")               
        logging.info(f"{log_module}:CKAN_URL: {ckan_info.ckan_site_url}")

        try:
            if harvest_servers is not None and ckan_info.parallelization is True:
                #TODO: Fix multicore parallel processing
                logging.warning(f'{log_module}:Parallel processing is not implemented yet.')
                '''
                parallel_count = Parallel(n_jobs=processes)(delayed(launch_harvest)(harvest_server=endpoint, ckan_info=ckan_info) for endpoint in harvest_servers)
                new_records.append(sum(i[0] for i in parallel_count))
                '''
            elif harvest_servers and ckan_info.parallelization is False:
                for endpoint in harvest_servers:
                    harvester = launch_harvest(harvest_server=endpoint, ckan_info=ckan_info)
                    new_records.append(harvester.ckan_dataset_count)
        except Exception as e:
            logging.error(f"{log_module}:Check invalid 'type' and 'active: True' in 'harvest_servers/{{my-harvest-server}}'at {config_file} Error: {e}")
            new_records = 0

    return new_records, harvest_servers

def main():
    harvester_start = setup_logging(log_module, VERSION)
    
    try:
        validate_configuration(config_file)
        new_records, harvest_servers = start_harvesting(config_file)

        harvester_end = datetime.now()
        hrvst_diff = harvester_end - harvester_start

        if isinstance(new_records, list):
            new_records = sum(new_records)
        else:
            new_records = 0

        logging.info(f"{log_module}:'config.yaml' sources: {str(len(harvest_servers))} and new CKAN Datasets: {str(new_records)} | Total time elapsed: {str(hrvst_diff).split('.')[0]}" )

    except Exception as e:
        logging.error(f"{log_module}: Error reading configuration file: {e}")
             
if __name__ == "__main__":
    if DEV_MODE == True or DEV_MODE.lower() == "true":
        # Allow other computers to attach to ptvsd at this IP address and port.
        ptvsd.enable_attach(address=("0.0.0.0", CKAN_OGC_DEV_PORT), redirect_output=True)

        # Pause the program until a remote debugger is attached
        ptvsd.wait_for_attach()
    else:
        main()