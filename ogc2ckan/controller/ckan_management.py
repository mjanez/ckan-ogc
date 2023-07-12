# inbuilt libraries
import json
import sys
import logging

# third-party libraries  
import urllib.request
from pprint import pprint

# custom functions
from config.ogc2ckan_config import get_log_module
from mappings.default_ogc2ckan_config import OGC2CKAN_CKAN_API_ROUTES

log_module = get_log_module()

# CKAN harvester functions.
def make_request(url, authorization_key, data):
    request = urllib.request.Request(url)
    # Creating a dataset requires an authorization header.
    # Replace *** with your API key, from your user account on the CKAN site
    # that you're creating the dataset on.
    request.add_header('Authorization', authorization_key)
    # Make the HTTP request.
    response = urllib.request.urlopen(request, data)
    assert response.code == 200
    # Use the json module to load CKAN's response into a dictionary.
    response_dict = json.loads(response.read())
    assert response_dict['success'] is True
    # package_create / package_update returns the created package as its result.
    package = response_dict['result']
    #pprint(package)


def create_ckan_dataset(ckan_site_url, authorization_key, data):
    '''
    Create a dataset using CKAN API.
    package_create: https://docs.ckan.org/en/2.9/api/index.html#ckan.logic.action.create.package_create
        package_create(
            name = NULL,
            title = NULL,
            private = FALSE,
            author = NULL,
            author_email = NULL,
            maintainer = NULL,
            maintainer_email = NULL,
            license_id = NULL,
            notes = NULL,
            package_url = NULL,
            version = NULL,
            state = "active",
            type = NULL,
            resources = NULL,
            tags = NULL,
            extras = NULL,
            relationships_as_object = NULL,
            relationships_as_subject = NULL,
            groups = NULL,
            owner_org = NULL,
            url = get_default_url(),
            key = get_default_key(),
            as = "list",
            ...
            )

    :param ckan_site_url: CKAN Server url
    :param authorization_key: API Key (http://localhost:5000/user/admin)
    :param data: Package data from Dataset
    '''
    # We'll use the package_create function to create a new dataset.
    url = ckan_site_url + OGC2CKAN_CKAN_API_ROUTES['create_ckan_dataset']
    make_request(url, authorization_key, data)

def create_ckan_resource_view(ckan_site_url, authorization_key, data):
    # We'll use the package_create function to create a new dataset.
    url = ckan_site_url + OGC2CKAN_CKAN_API_ROUTES['create_ckan_resource_view']
    make_request(url, authorization_key, data)

def create_ckan_datasets(ckan_site_url, authorization_key, datasets, workspaces = None):
    '''
    Create datasets if you are only interested in creating new datasets

    :param ckan_site_url: CKAN Server url
    :param authorization_key: API Key (http://localhost:5000/user/admin)
    :param datasets: Datasets object
    :param workspaces: Only those identifiers starting with identifier_fiter (e.g. 'open_data:...') are created.
    
    :return: Harvester server records and CKAN New records counters
    '''

    server_count = len(datasets)
    ckan_count = server_count

    for dataset in datasets:
        data = None
        try:
            if workspaces is not None:
                if any(x.lower() in dataset.ogc_workspace.lower() for x in workspaces) is True:
                    data = dataset.generate_data()
                else:
                    ckan_count = ckan_count - 1
            else:
                data = dataset.generate_data()
            if data is not None:
                create_ckan_dataset(ckan_site_url, authorization_key, data)
        except Exception as e:
            print("\nckan_site_url: " + ckan_site_url)
            print("ERROR", file=sys.stderr)
            print(e, file=sys.stderr)
            print("While trying to create: " + dataset.name + " | " + dataset.title, file=sys.stderr)
            logging.error("Check the CKAN Log: '" + str(e) + "' |  While trying to create: " + dataset.name + " | " + dataset.title)
            pprint(dataset.dataset_dict(), stream=sys.stderr)
            print("\n", file=sys.stderr)
            ckan_count = ckan_count - 1

    return ckan_count, server_count

def update_ckan_dataset(ckan_site_url, authorization_key, data):
    '''
    Update a dataset using CKAN API

    :param ckan_site_url: CKAN Server url
    :param authorization_key: API Key (http://localhost:5000/user/admin)
    :param data: Package data from Dataset
    '''


    # We'll use the package_update function to update a dataset.
    url = ckan_site_url + OGC2CKAN_CKAN_API_ROUTES['update_ckan_dataset']
    make_request(url, authorization_key, data)

def ingest_ckan_dataset(ckan_site_url, authorization_key, data):
    '''
    Create a dataset using the CKAN API if it does not exist, otherwise update it

    :param ckan_site_url: CKAN Server url
    :param authorization_key: API Key (http://localhost:5000/user/admin)
    :param data: Package data from Dataset
    '''
    try:
        create_ckan_dataset(ckan_site_url, authorization_key, data)
        print('Dataset created')
    except urllib.error.HTTPError as e:  # urllib.error.HTTPError
        update_ckan_dataset(ckan_site_url, authorization_key, data)
        print('Dataset updated')

def ingest_ckan_datasets(ckan_site_url, authorization_key, datasets, inspireid_theme, theme_es, inspireid_nutscode, workspace = None):
    '''
    Ingest datasets if you are interested in creating or updating

    :param ckan_site_url: CKAN Server url
    :param authorization_key: API Key (http://localhost:5000/user/admin)
    :param datasets: Datasets object
    :param inspireid_theme: INSPIRE Theme code
    :param theme_es: NTI-RISP sector
    :param inspireid_nutscode: NUTS0 Code
    :param workspace: Only those identifiers starting with identifier_fiter (e.g. 'open_data:...') are created.
    
    :return: Harvester server records and CKAN New records counters
    '''    
    server_count = len(datasets)
    ckan_count = server_count

    for dataset in datasets:
        data = dataset.generate_data()
        try:
            if workspace is not None:
                if dataset.identifier.startswith(".".join([inspireid_nutscode, inspireid_theme, workspace.replace(':', '.')]).upper()):
                    data = dataset.generate_data()
            else:
                data = dataset.generate_data()
            if data is not None:
                ingest_ckan_dataset(ckan_site_url, authorization_key, data)
        except Exception as e:
            print("\nckan_site_url: " + ckan_site_url)
            print("ERROR", file=sys.stderr)
            print(e, file=sys.stderr)
            print("While trying to create: " + dataset.name + " | " + dataset.title, file=sys.stderr)
            logging.error("Check the CKAN Log: '" + str(e) + "' |  While trying to create: " + dataset.name + " | " + dataset.title)
            pprint(dataset.dataset_dict(), stream=sys.stderr)
            print("\n", file=sys.stderr)
            ckan_count = ckan_count - 1

    return ckan_count, server_count