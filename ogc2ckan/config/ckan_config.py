# inbuilt libraries
import yaml
import os
import logging
import requests

# third-party libraries
import psycopg2
from bs4 import BeautifulSoup

# custom functions
from config.ogc2ckan_config import get_log_module, load_yaml
from mappings.default_ogc2ckan_config import OGC2CKAN_CKANINFO_CONFIG, OGC2CKAN_DBDSN_CONFIG, OGC2CKAN_HARVESTER_CONFIG

log_module = get_log_module()


class CKANInfo:
    def __init__(self):
        self.ckan_site_url = os.environ.get('CKAN_URL', OGC2CKAN_CKANINFO_CONFIG['ckan_site_url'])
        self.pycsw_site_url = os.environ.get('PYCSW_URL', OGC2CKAN_CKANINFO_CONFIG['pycsw_site_url'])
        self.authorization_key = os.environ.get('CKAN_API_KEY', OGC2CKAN_CKANINFO_CONFIG['authorization_key'])
        self.default_license = os.environ.get('DEFAULT_LICENSE', OGC2CKAN_CKANINFO_CONFIG['default_license'])
        self.default_license_id = os.environ.get('DEFAULT_LICENSE_ID', OGC2CKAN_CKANINFO_CONFIG['default_license_id'])
        self.ckan_harvester = OGC2CKAN_HARVESTER_CONFIG
        self.ssl_unverified_mode = os.environ.get('SSL_UNVERIFIED_MODE', OGC2CKAN_CKANINFO_CONFIG['ssl_unverified_mode'])
        self.metadata_distributions = os.environ.get('METADATA_DISTRIBUTIONS', OGC2CKAN_CKANINFO_CONFIG['metadata_distributions'])
        self.parallelization = os.environ.get('PARALLELIZATION', OGC2CKAN_CKANINFO_CONFIG['parallelization'])
        self.dir3_soup = self.get_dir3_soup()
        self.ckan_dataset_schema = os.environ.get('CKAN_DATASET_SCHEMA', OGC2CKAN_CKANINFO_CONFIG['ckan_dataset_schema'])

    def get_dir3_soup(self):
        """
        Get the BeautifulSoup object for the dir3_info page.

        Returns:
            BeautifulSoup or None: The BeautifulSoup object for the dir3_info page, or None if an error occurs.
        """
        try:
            dir3_url = OGC2CKAN_CKANINFO_CONFIG['dir3_url']
            response = requests.get(dir3_url)
            response.raise_for_status()  # Check HTTP status code
            self.dir3_soup = BeautifulSoup(response.text, 'html.parser')
        except requests.exceptions.HTTPError as errh:
            print("HTTP Error:", errh)
            self.dir3_soup = None
        except requests.exceptions.ConnectionError as errc:
            print("Error Connecting:", errc)
            self.dir3_soup = None
        except requests.exceptions.Timeout as errt:
            print("Timeout Error:", errt)
            self.dir3_soup = None
        except requests.exceptions.RequestException as err:
            print("Something went wrong:", err)
            self.dir3_soup = None
        
        return self.dir3_soup

class DBDsn:
    def __init__(self):
            self.host = os.environ.get('DB_HOST', OGC2CKAN_DBDSN_CONFIG['host'])
            self.port = os.environ.get('DB_PORT', OGC2CKAN_DBDSN_CONFIG['port'])
            self.database = os.environ.get('DB_NAME', OGC2CKAN_DBDSN_CONFIG['database'])
            self.user = os.environ.get('DB_USER', OGC2CKAN_DBDSN_CONFIG['user']),
            self.password = os.environ.get('DB_PASSWORD', OGC2CKAN_DBDSN_CONFIG['password'])

class ObjectFromListDicts:
    def __init__(self, **entries):
        for key, value in entries.items():
            if key == 'distributions':
                setattr(self, key, [ObjectFromListDicts(**d) for d in value])
            else:
                setattr(self, key, value)

    def __getitem__(self, key):
        return getattr(self, key)

# Configuration data
def config_getParameters(config_file):
    '''
    Read the --config_file parameter entered and return the required parameters from the YAML 
    
    Parameters
    ----------
    config_file: config.yaml
    app_dir: Application directory

    Return
    ----------
    Tuple containing the parameters:
            - db_dsn: Database DSN (Data Source Name)
            - ckan_info: Default CKAN configuration dictionary
            - harvest_servers: Harvest servers information
    '''    
    with open(config_file, encoding='utf-8') as stream:
        config = yaml.safe_load(stream)
        
        ckan_info = CKANInfo()
        db_dsn = DBDsn()
        harvest_servers = [ObjectFromListDicts(**d) for d in config.get('harvest_servers')]
        
        return (
            ckan_info,
            harvest_servers,
            db_dsn,
        )

def config_getConnection(host, port, username, password, dbname):
    '''
    Establish a connection to the database using the provided connection parameters.

    Parameters
    ----------
    host: Hostname of the database server
    port: Port number of the database server
    username: Username for authentication
    password: Password for authentication
    dbname: Name of the database

    Return
    ----------
    Psycopg2 connection object
    '''
    conn = psycopg2.connect(host=host, port=port, user=username, password=password, dbname=dbname)
    return conn