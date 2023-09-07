# inbuilt libraries
import json
import sys
import logging
import ssl
import socket
import os
from typing import Any, Dict, Optional, Tuple

# third-party libraries  
import urllib.request
from pprint import pprint

# custom functions
from config.ogc2ckan_config import get_log_module
from mappings.default_ogc2ckan_config import OGC2CKAN_CKAN_API_ROUTES
SSL_UNVERIFIED_MODE = os.environ.get("SSL_UNVERIFIED_MODE", False)

log_module = get_log_module()


# CKAN Requests
def make_request(url: str, ssl_unverified_mode: bool, data: bytes = None, authorization_key: Optional[str] = None) -> Dict[str, Any]:
    """
    Sends an HTTPS request to the specified URL with the given data and SSL verification mode.

    Args:
        url (str): The URL to send the request to.
        ssl_unverified_mode (bool): Whether to use SSL verification or not.
        data (bytes): The data to send with the request.
        authorization_key (str, optional): The authorization key to use for the request. Defaults to None.

    Returns:
        Dict[str, Any]: The response from the CKAN server as a dictionary.
    """
    try:
        request = urllib.request.Request(url)
        # Creating a dataset requires an authorization header.
        # Replace *** with your API key, from your user account on the CKAN site
        # that you're creating the dataset on.
        if authorization_key is not None:
            request.add_header('Authorization', authorization_key)

        # Try making the HTTPS request using the default SSL context with verification.
        response = urllib.request.urlopen(request, data)
        
    except ssl.CertificateError:
        if ssl_unverified_mode == True or ssl_unverified_mode == "True":
            # If the default SSL verification fails, try downloading and loading the certificate.
            hostname = urllib.parse.urlparse(url).hostname
            port = 443  # Assuming HTTPS (default port)

            # Download the server's certificate in PEM format.
            pem_cert = ssl.get_server_certificate((hostname, port))

            # Create an SSL context and load the downloaded certificate without verification.
            ssl_context = ssl.create_default_context(cadata=pem_cert)
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

            # Make the HTTPS request using the custom SSL context.
            response = urllib.request.urlopen(request, data, context=ssl_context)
        else:
            raise ssl.CertificateError(f"{log_module}:[INSECURE] Put SSL_UNVERIFIED_MODE=True if the host certificate is self-signed or invalid.")    
        
    assert response.code == 200
    # Use the json module to load CKAN's response into a dictionary.
    response_dict = json.loads(response.read())
    assert response_dict['success'] is True
    # package_create / package_update returns the created package as its result.
    package = response_dict['result']
    #pprint(package)

# CKAN harvester functions.
def create_ckan_datasets(ckan_site_url: str, authorization_key: str, datasets: object, ssl_unverified_mode: bool = False, workspaces: Optional[str] = None) -> Tuple[int, int]:
    """
    Create new datasets on a CKAN server.

    Args:
        ckan_site_url (str): The URL of the CKAN server.
        authorization_key (str): The API key for the CKAN server.
        datasets (object): The datasets to create.
        ssl_unverified_mode (bool, optional): Whether to use SSL verification or not. Defaults to False.
        workspaces (str, optional): Only those identifiers starting with identifier_filter (e.g. 'open_data:...') are created. Defaults to None.

    Returns:
        Tuple[int, int]: A tuple containing the number of Harvester server records and CKAN new records counters.
    """
    source_dataset_count = len(datasets)
    ckan_dataset_count = source_dataset_count
    ckan_dataset_errors = []

    for dataset in datasets:
        data = None
        try:
            if workspaces is not None:
                if any(x.lower() in dataset.ogc_workspace.lower() for x in workspaces) is True:
                    data = dataset.generate_data()
                else:
                    ckan_dataset_count = ckan_dataset_count - 1
            else:
                data = dataset.generate_data()
            if data is not None:
                create_ckan_dataset(ckan_site_url, ssl_unverified_mode, data, authorization_key)
        except Exception as e:
            print("\nckan_site_url: " + ckan_site_url)
            print("ERROR", file=sys.stderr)
            print(e, file=sys.stderr)
            print("While trying to create: " + dataset.name + " | " + dataset.title, file=sys.stderr)
            pprint(dataset.dataset_dict(), stream=sys.stderr)
            print("\n", file=sys.stderr)
            ckan_dataset_count = ckan_dataset_count - 1
            
            # Info about the error.
            error_dict = {
                'title': dataset.title,
                'error': str(e)                
            }
            if hasattr(dataset, 'inspire_id') and dataset.inspire_id:
                error_dict['inspire_id'] = dataset.inspire_id
            ckan_dataset_errors.append(error_dict)
            
    return ckan_dataset_count, source_dataset_count, ckan_dataset_errors

def ingest_ckan_datasets(ckan_site_url, authorization_key, datasets, inspireid_theme, theme_es, inspireid_nutscode, ssl_unverified_mode = False, workspace = None):
    #TODO: Fix function.
    """
    '''
    Ingest datasets if you are interested in creating or updating

    :param ckan_site_url: CKAN Server url
    :param authorization_key: API Key (http://localhost:5000/user/admin)
    :param datasets: Datasets object
    :param inspireid_theme: INSPIRE Theme code
    :param theme_es: NTI-RISP sector
    :param inspireid_nutscode: NUTS0 Code
    :param ssl_unverified_mode: [INSECURE] Put SSL_UNVERIFIED_MODE=True if the host certificate is self-signed or invalid.
    :param workspace: Only those identifiers starting with identifier_fiter (e.g. 'open_data:...') are created.
    
    :return: Harvester server records and CKAN New records counters
    '''
    source_dataset_count = len(datasets)
    ckan_dataset_count = source_dataset_count

    for dataset in datasets:
        data = dataset.generate_data()
        try:
            if workspace is not None:
                if dataset.identifier.startswith(".".join([inspireid_nutscode, inspireid_theme, workspace.replace(':', '.')]).upper()):
                    data = dataset.generate_data()
            else:
                data = dataset.generate_data()
            if data is not None:
                ingest_ckan_dataset(ckan_site_url, ssl_unverified_mode, data, authorization_key)
        except Exception as e:
            print("\nckan_site_url: " + ckan_site_url)
            print("ERROR", file=sys.stderr)
            print(e, file=sys.stderr)
            print("While trying to create: " + dataset.name + " | " + dataset.title, file=sys.stderr)
            logging.error("Check the CKAN Log: '" + str(e) + "' |  While trying to create: " + dataset.name + " | " + dataset.title)
            pprint(dataset.dataset_dict(), stream=sys.stderr)
            print("\n", file=sys.stderr)
            ckan_dataset_count = ckan_dataset_count - 1

    return ckan_dataset_count, source_dataset_count
    """

def create_ckan_datadictionaries(ckan_site_url: str, authorization_key: str, datadictionaries: object, ssl_unverified_mode: bool = False) -> Tuple[int, int]:
    """
    Ingest data dictionaries if you are interested in creating or updating.

    Args:
        ckan_site_url (str): The URL of the CKAN server.
        authorization_key (str): The API key for the CKAN server.
        datadictionaries (object): The data dictionaries to ingest.
        ssl_unverified_mode (bool, optional): Whether to use SSL verification or not. Defaults to False.

    Returns:
        Tuple[int, int]: A tuple containing the number of Harvester server records and CKAN new records counters.
    """
    source_dictionaries_count = len(datadictionaries)
    ckan_dictionaries_count = source_dictionaries_count
    ckan_dictionaries_errors = []

    for datadictionary in datadictionaries:
        data = None
        try:
            data = datadictionary.generate_data()
            if data is not None:
                create_ckan_resource_dictionary(ckan_site_url, ssl_unverified_mode, data, authorization_key)
        except Exception as e:
            print("\nckan_site_url: " + ckan_site_url)
            print("ERROR", file=sys.stderr)
            print(e, file=sys.stderr)
            print("While trying to create data dictionary for: " + datadictionary.resource_id, file=sys.stderr)
            pprint(datadictionary.dataset_dict(), stream=sys.stderr)
            print("\n", file=sys.stderr)
            ckan_dictionaries_count = ckan_dictionaries_count - 1
            # Info about the error.
            error_dict = {
                'resource_id': datadictionary.resource_id,
                'error': str(e)                
            }
            ckan_dictionaries_errors.append(error_dict)

    return ckan_dictionaries_count, source_dictionaries_count, ckan_dictionaries_errors

# CKAN API functions.
def create_ckan_dataset(ckan_site_url: str, ssl_unverified_mode: bool, data: dict, authorization_key: str) -> None:
    """
    Create a dataset using CKAN API.

    Args:
        ckan_site_url (str): The URL of the CKAN server.
        ssl_unverified_mode (bool): Whether to use SSL verification or not.
        data (dict): The data to be sent with the request.
        authorization_key (str): The API authorization key.

    Returns:
        None
        
    Additional Information:
        CKAN API Reference:
        https://docs.ckan.org/en/2.9/api/index.html#ckan.logic.action.create.package_create
    """
    # We'll use the package_create function to create a new dataset.
    url = ckan_site_url + OGC2CKAN_CKAN_API_ROUTES['create_ckan_dataset']
    make_request(url, ssl_unverified_mode, data, authorization_key)

def update_ckan_dataset(ckan_site_url, ssl_unverified_mode, data, authorization_key):
    #TODO: Fix function.
    """
    '''
    Update a dataset using CKAN API

    Args:
        ckan_site_url (str): The CKAN server URL.
        ssl_unverified_mode (bool): Indicates whether SSL mode should be verified.
        data (dict): The data to be sent with the request.
        authorization_key (str): The API authorization key.
    '''
    # We'll use the package_update function to update a dataset.
    url = ckan_site_url + OGC2CKAN_CKAN_API_ROUTES['update_ckan_dataset']
    make_request(url, ssl_unverified_mode, data, authorization_key)
    """

def ingest_ckan_dataset(ckan_site_url, ssl_unverified_mode, data, authorization_key):
    #TODO: Fix function.
    """
    '''
    Create a dataset using the CKAN API if it does not exist, otherwise update it

    Args:
        ckan_site_url (str): The CKAN server URL.
        ssl_unverified_mode (bool): Indicates whether SSL mode should be verified.
        data (dict): The data to be sent with the request.
        authorization_key (str): The API authorization key.
    '''
    try:
        create_ckan_dataset(ckan_site_url, ssl_unverified_mode, data, authorization_key)
        print('Dataset created')
    except urllib.error.HTTPError as e:  # urllib.error.HTTPError
        update_ckan_dataset(ckan_site_url, ssl_unverified_mode, data, authorization_key, ssl_unverified_mode)
        print('Dataset updated')
    """

def create_ckan_resource_view(ckan_site_url, ssl_unverified_mode, data, authorization_key):
    #TODO: Fix function.
    """
    '''
    Create a CKAN distribution view in a dataset using CKAN API

    Args:
        ckan_site_url (str): The CKAN server URL.
        ssl_unverified_mode (bool): Indicates whether SSL mode should be verified.
        data (dict): The data to be sent with the request.
        authorization_key (str): The API authorization key.
    '''
    # We'll use the package_create function to create a new dataset.
    url = ckan_site_url + OGC2CKAN_CKAN_API_ROUTES['create_ckan_resource_view']
    make_request(url, ssl_unverified_mode, data, authorization_key)
    """

def create_ckan_resource_dictionary(ckan_site_url: str, ssl_unverified_mode: bool, data: dict, authorization_key: str) -> None:
    """
    Create a Data Dictionary using CKAN API.

    Args:
        ckan_site_url (str): The URL of the CKAN server.
        ssl_unverified_mode (bool): Whether to use SSL verification or not.
        data (dict): The data to be sent with the request.
        authorization_key (str): The API authorization key.

    Returns:
        None

    Additional Information:
        For more information on the CKAN API route used by this function, please see:
        https://github.com/mjanez/ckanext-resourcedictionary/blob/main/ckanext/resourcedictionary/logic/action/resource_dictionary_create.md
    """
    url = ckan_site_url + OGC2CKAN_CKAN_API_ROUTES['create_ckan_resource_dictionary']
    make_request(url, ssl_unverified_mode, data, authorization_key)

def get_ckan_datasets_list(ckan_site_url: str, ssl_unverified_mode: bool, authorization_key: Optional[str] = None, fields: str = 'id,title,extras_inspire_id,extras_alternate_identifier', rows: int = 100) -> None:
    """
    Get a list of datasets from CKAN.

    Args:
        ckan_site_url (str): The URL of the CKAN server.
        ssl_unverified_mode (bool): Whether to use SSL verification or not.
        authorization_key (str, optional): The API authorization key. Defaults to None.
        fields (str, optional): The fields to be returned in the dataset list. Defaults to 'id,title,extra_inspire_id'.
        rows (int, optional): The number of rows to be returned in the dataset list. Defaults to 100.

    Returns:
        None
    """
    # We'll use the package_search function to list all datasets with fields as need.
    url = ckan_site_url + OGC2CKAN_CKAN_API_ROUTES['get_ckan_datasets_list'].format(fields=fields, rows=rows)
    make_request(url, ssl_unverified_mode, authorization_key)
    
def get_ckan_dataset_info(ckan_site_url: str, ssl_unverified_mode: bool, authorization_key: Optional[str] = None, field: str = 'id', field_value: Optional[str] = None) -> None:
    """
    Get information about a dataset from CKAN based on a field and its value.

    Args:
        ckan_site_url (str): The URL of the CKAN server.
        ssl_unverified_mode (bool): Whether to use SSL verification or not.
        authorization_key (str, optional): The API authorization key. Defaults to None.
        field (str, optional): The field to filter the dataset on. Defaults to 'id'.
        field_value (str, optional): The value to filter the dataset on. Defaults to None.

    Returns:
        None
    """
    # We'll use the package_search function to list all datasets with fields as need.
    url = ckan_site_url + OGC2CKAN_CKAN_API_ROUTES['get_ckan_dataset_info'].format(field=field, field_value=field_value)
    make_request(url, ssl_unverified_mode, authorization_key)
