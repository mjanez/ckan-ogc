from ckan_datasets.base import Dataset as BaseDataset, Distribution as BaseDistribution
from ckan_datasets.geodcatap import Dataset as GeoDataset, Distribution as GeoDistribution
from ckan_datasets.resources.datadictionary import DataDictionary as BaseDataDictionary, DataDictionaryField as BaseDataDictionaryField

# CKAN Schemas available
CKAN_DATASET_SCHEMAS = {
    "geodcatap": {
        "dataset": GeoDataset,
        "distribution": GeoDistribution,
        "datadictionary": BaseDataDictionary,
        "datadictionaryfield": BaseDataDictionaryField
    },
    "default": {
        "dataset": BaseDataset,
        "distribution": BaseDistribution,
        "datadictionary": BaseDataDictionary,
        "datadictionaryfield": BaseDataDictionaryField
    }
}

