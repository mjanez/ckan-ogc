# inbuilt libraries
import logging
from datetime import datetime   
import os
from joblib import Parallel, delayed
import ssl

# custom classes
from controller import ckan_management
from harvesters.base import Harvester

# custom functions
from config.ckan_config import config_getParameters, config_getConnection
from config.log import log_file
from mappings.default_ogc2ckan_config import OGC2CKAN_HARVESTER_CONFIG

# debug
import ptvsd


# Ennvars
TZ = os.environ.get("TZ", "TZ")
DEV_MODE = None
VERSION = os.environ.get("VERSION", "0.1")
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
    start = datetime.now()

    logging.info(f"{log_module}:Server: {harvest_server.name} ({harvest_server.type.upper()}) with url: '{harvest_server.url}' and CKAN organization: {ckan_info.ckan_site_url}/organization/{harvest_server.organization}") 

    harvester = Harvester.from_harvest_server(harvest_server, APP_DIR)

    try:
        harvester.create_datasets(ckan_info)

        # Output info
        end = datetime.now()
        diff = end - start

        logging.info(harvest_server["name"] + " (" + harvester.type.upper() + ") server records retrieved (" + str(harvester.server_count) + ") with conflicts: (" + str(harvester.server_count - harvester.ckan_count) + ") from (" + harvester.type.upper() + "): [" + ', '.join([d.title for d in harvester.datasets]) + "]")
        logging.info(harvest_server["name"] + " (" + harvester.type.upper() + ") server time elapsed: " + str(diff).split(".")[0])
    
    except Exception as e:
        logging.exception("An exception occurred!")
        self.ckan_count = 0
        self.server_count = 0
        self.datasets = None

        # Output info
        end = datetime.now()
        diff = end - start

        logging.error(harvest_server["name"] + " (" + harvester.type.upper() + ") server: " + harvest_server['url'] + ' failed connection.')
        print("ERROR::" + harvest_server["name"] + " (" + harvester.type.upper() + ") server: " + harvest_server['url'] + ' failed connection.')

    return harvester

def main():
    ssl._create_default_https_context = ssl._create_unverified_context
    
    log_file(APP_DIR + "/log")
    logging.info(f"{log_module}:Version: {VERSION}")
    
    
    # Retrieve parameters and init log
    harvester_start = datetime.now()
    ckan_info, harvest_servers, db_dsn = config_getParameters(config_file)
    
    # Processes
    processes = os.cpu_count() - 1
    new_records = []

    # Type of server to harvest
    if ckan_info.ckan_harvester is not None:
        try:
            active_harvesters = [h["type"] for h in ckan_info.ckan_harvester.values() if h['active'] is True]
            
            # Filter harvest_servers by active_harvesters
            harvest_servers = [e for e in harvest_servers if e['type'] in active_harvesters and e['active'] is True]
            if not harvest_servers:
                error_message = f"{log_module}:No active harvest servers found for types: [{', '.join([OGC2CKAN_HARVESTER_CONFIG[key]['type'] for key in OGC2CKAN_HARVESTER_CONFIG])}]."
                raise ValueError(error_message)

            # Starts software
            logging.info(f"{log_module}:Number of processes: {processes}")
            logging.info(f"{log_module}:Multicore parallel processing: {ckan_info.parallelization}")
            if ckan_info.ssl_unverified_mode == True or ckan_info.ssl_unverified_mode == "True":
                logging.warning(f"{log_module}:[INSECURE] SSL_UNVERIFIED_MODE:'{ckan_info.ssl_unverified_mode}'. Only if you trust the host.")    
            if ckan_info.metadata_distributions == True or ckan_info.metadata_distributions == "True":
                logging.warning(f"{log_module}:METADATA_DISTRIBUTIONS:'{ckan_info.metadata_distributions}'. It is not necessary if you do not intend to generate distributions for geographic metadata (INSPIRE ISO19139) or Linked Open Data (GeoDCAT-AP). ckanext-scheming_dcat already links the most important metadata profiles (https://github.com/mjanez/ckanext-scheming_dcat).")
            logging.info(f"{log_module}:Type of activated harvesters: {', '.join([h.upper() for h in active_harvesters])}")               
            logging.info(f"{log_module}:CKAN_URL: {ckan_info.ckan_site_url}")           

            # Check invalid 'type' parameter in config.yaml         
            if harvest_servers is not None:
                try:
                    #TODO: DB use
                    #conn = config_getConnection(db_dsn[host], db_dsn[port], db_dsn[username], db_dsn[password], db_dsn[dbname])

                    if ckan_info.parallelization is True:
                        #TODO: Fix multicore parallel processing
                        parallel_count = Parallel(n_jobs=processes)(delayed(launch_harvest)( harvest_server=endpoint, ckan_info=ckan_info) for endpoint in harvest_servers)
                        new_records.append(sum(i[0] for i in parallel_count))

                    else:
                        # Single core processing
                        for endpoint in harvest_servers:
                            harvester = launch_harvest(harvest_server=endpoint, ckan_info=ckan_info)
                            new_records.append(harvester.ckan_count)
                            
                except Exception as e:
                        logging.error(f"{log_module}:Check invalid 'type' and 'active: True' in 'harvest_servers/{{my-harvest-server}}'at {config_file} Error: {e}")
                        new_records = 0
        
        
        
        except Exception as e:
            logging.error(f"{log_module}:Activate at least one of the options of 'ckan_info/ckan_harvester' in the configuration file: {config_file} Error: {e}")
            new_records = 0

    # ogc_ckan outputinfo
    harvester_end = datetime.now()
    hrvst_diff =  harvester_end - harvester_start

    try:
        new_records = sum(new_records)
    except:
        new_records = 0

    logging.info(f"{log_module}:config.yaml endpoints: {str(len(harvest_servers))} and new CKAN Datasets: {str(new_records)} | Total time elapsed: {str(hrvst_diff).split('.')[0]})" )
                 
                     
if __name__ == "__main__":
    if DEV_MODE == True or DEV_MODE == "True":
        # Allow other computers to attach to ptvsd at this IP address and port.
        ptvsd.enable_attach(address=("0.0.0.0", 5678), redirect_output=True)

        # Pause the program until a remote debugger is attached
        ptvsd.wait_for_attach()
    else:
        main()