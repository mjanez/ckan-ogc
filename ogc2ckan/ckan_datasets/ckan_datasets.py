from ckan_datasets.base import Dataset as BaseDataset, Distribution as BaseDistribution
from ckan_datasets.geodcatap import Dataset as GeoDataset, Distribution as GeoDistribution

# CKAN Schemas available
CKAN_DATASET_SCHEMAS = {
    "geodcatap": {
        "dataset": GeoDataset,
        "distribution": GeoDistribution
    },
    "default": {
        "dataset": BaseDataset,
        "distribution": BaseDistribution
    }
}

