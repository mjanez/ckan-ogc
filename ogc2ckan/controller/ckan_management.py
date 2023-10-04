# inbuilt libraries
import json
import sys
import logging
import ssl
import socket
import os
from typing import Any, Dict, Optional, Tuple, Union, List

# third-party libraries  
import urllib.request
from pprint import pprint, pformat

# custom functions
from config.ogc2ckan_config import get_log_module
from mappings.default_ogc2ckan_config import OGC2CKAN_CKAN_API_ROUTES
SSL_UNVERIFIED_MODE = os.environ.get("SSL_UNVERIFIED_MODE", False)

log_module = get_log_module(os.path.abspath(__file__))


# CKAN Requests
def make_request(url: str, ssl_unverified_mode: bool, data: bytes = None, authorization_key: Optional[str] = None, return_result: bool = False) -> Union[Dict[str, Any], Any]:
    """ Sends an HTTPS request to the specified URL with the given data and SSL verification mode.


    Args:
        url (str): The URL to send the request to.
        ssl_unverified_mode (bool): Whether to use SSL verification or not.
        data (bytes): The data to send with the request.
        authorization_key (str, optional): The authorization key to use for the request. Defaults to None.
        return_result (bool): Whether to return the 'result' object from the CKAN response. Defaults to False.

    Returns:
        Dict[str, Any]: The response from the CKAN server as a dictionary.
        Any: The 'result' object from the CKAN response if return_result is True.
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

    if return_result == True:
        return response_dict
    else:
        return None

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
    ckan_dataset_errors = []
    source_dataset_count = len(datasets)

    # Check if the datasets already exists in CKAN.
    datasets, ckan_dataset_errors, ckan_dataset_count = check_ckan_datasets_exists(ckan_site_url, authorization_key, datasets, ssl_unverified_mode, ckan_dataset_errors)

    for dataset in datasets:
        try:
            if workspaces is not None and not any(x.lower() in dataset.ogc_workspace.lower() for x in workspaces):
                break
            data = dataset.generate_data()
            if data is not None:
                create_ckan_dataset(ckan_site_url, ssl_unverified_mode, data, authorization_key)
                ckan_dataset_count += 1

        except Exception as e:
            print(f"\nckan_site_url: {ckan_site_url}\nERROR: {e}\nWhile trying to create: {dataset.name} | {dataset.title}\n{pformat(dataset.dataset_dict())}\n", file=sys.stderr)
            error_dict = {'title': dataset.title, 'error': str(e)}
            if hasattr(dataset, 'inspire_id') and dataset.inspire_id:
                error_dict['inspire_id'] = dataset.inspire_id
            ckan_dataset_errors.append(error_dict)

    return ckan_dataset_count, source_dataset_count, ckan_dataset_errors

def ingest_ckan_datasets(ckan_site_url, authorization_key, datasets, ssl_unverified_mode = False, workspace = None):
    #TODO: Fix function.
    """
    '''
    Ingest datasets if you are interested in creating or updating

    :param ckan_site_url: CKAN Server url
    :param authorization_key: API Key (http://localhost:5000/user/admin)
    :param datasets: Datasets object
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

def get_ckan_datasets_list(ckan_site_url: str, ssl_unverified_mode: bool, authorization_key: Optional[str] = None, fields: str = 'id,title,extras_inspire_id,extras_alternate_identifier', rows: int = 100, include_private: bool = True) -> List[Dict[str, Any]]:
    """
    Get a list of datasets from CKAN.

    Args:
        ckan_site_url (str): The URL of the CKAN server.
        ssl_unverified_mode (bool): Whether to use SSL verification or not.
        authorization_key (str, optional): The API authorization key. Defaults to None.
        fields (str, optional): The fields to be returned in the dataset list. Defaults to 'id,title,extra_inspire_id'.
        rows (int, optional): The number of rows to be returned in the dataset list. Defaults to 100.
        include_private (bool, optional): Whether to include private datasets in the dataset list. Defaults to True.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries representing the datasets.
    """
    # We'll use the package_search function to list all datasets with fields as need.
    url = ckan_site_url + OGC2CKAN_CKAN_API_ROUTES['get_ckan_datasets_list'].format(fields=fields, rows=rows, include_private=include_private)
    response = make_request(url=url, ssl_unverified_mode=ssl_unverified_mode, authorization_key=authorization_key, return_result=True)
    if response is not None:
        results = response['result']['results']
        count = response['result']['count']
        # if response['result']['count'] > rows then we need to paginate the results.
        if count > rows:
            # Calculate the number of pages we need to paginate through.
            pages = count // rows + 1
            # Paginate through the results.
            for page in range(2, pages + 1):
                url = ckan_site_url + OGC2CKAN_CKAN_API_ROUTES['get_ckan_datasets_list_paginate'].format(fields=fields, rows=rows, include_private=include_private, start=rows * (page - 1))
                response = make_request(url=url, ssl_unverified_mode=ssl_unverified_mode, authorization_key=authorization_key, return_result=True)
                results += response['result']['results']
    else: 
        results = []

    return results
    
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

def check_ckan_datasets_exists(ckan_site_url: str, authorization_key: str, datasets: object, ssl_unverified_mode: bool = False, ckan_dataset_errors: list = []):
    """Check if datasets already exist in CKAN.

    Args:
        ckan_site_url (str): The URL of the CKAN site.
        authorization_key (str): The authorization key for the CKAN site.
        datasets (object): The datasets to check.
        ssl_unverified_mode (bool, optional): Whether to use SSL unverified mode. Defaults to False.
        ckan_dataset_errors (list, optional): A list to store any errors that occur. Defaults to [].

    Returns:
        tuple: A tuple containing the datasets that need to be loaded, a list of errors, and the number of datasets to load.
    """
    ckan_dataset_list = get_ckan_datasets_list(ckan_site_url, ssl_unverified_mode, authorization_key)
    ckan_datasets_to_load = []
    
    # Create a dict indexed by 'id' and 'inspire_id' for efficient searching
    ckan_dataset_dict = {dataset.get('id'): dataset for dataset in ckan_dataset_list}
    ckan_dataset_dict.update({dataset.get('inspire_id'): dataset for dataset in ckan_dataset_list if dataset.get('inspire_id')})
    
    for dataset in datasets:
        dataset_id = dataset.ckan_id
        dataset_title = dataset.title
        inspire_id = dataset.inspire_id
        
        # Check if the dataset already exists in CKAN. Use the indexed dictionary.
        if dataset_id in ckan_dataset_dict or (inspire_id and inspire_id in ckan_dataset_dict):
            matching_field = 'id' if dataset_id in ckan_dataset_dict else 'inspire_id'
            error_message = f"Dataset exists in CKAN with the same '{matching_field}': {dataset_id if matching_field == 'id' else inspire_id}"
            error_dict = {'title': dataset_title, 'id': dataset_id, 'inspire_id': inspire_id, 'error': error_message}
            ckan_dataset_errors.append(error_dict)
        else:
            ckan_datasets_to_load.append(dataset)
        
    return ckan_datasets_to_load, ckan_dataset_errors, len(ckan_datasets_to_load)
