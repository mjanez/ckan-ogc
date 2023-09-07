# Metadata of a Dataset and Distributions. CKAN Data Dictionary Fields https://docs.ckan.org/en/2.9/maintaining/datastore.html#fields

## Import libraries   
import urllib.parse
import json

class DataDictionaryField:
    """
    Metadata of a distribution. CKAN Data Dictionary Fields.
    
    See https://docs.ckan.org/en/2.9/maintaining/datastore.html#fields for more information.
    
    Attributes:
        id (str): The ID (fieldname) of the data dictionary field
        type (str): The type of the data dictionary field.
        fields (list): A list of DataDictionaryField objects.
        label (str): A human-friendly label for this column.
        notes (str): A full description for this column in markdown format.
        type_override (str): The type to be used the next time DataPusher/xloader is run to load data into this column.
    """
    def __init__(self, id, type = 'text', label = '', notes = '', type_override = ''):
        self.id = id
        self.type = type
        self.label = label
        self.notes = notes
        self.type_override = type_override

    def set_id(self, id):
        self.id = id

    def set_type(self, type):
        self.type = type

    def set_label(self, label):
        self.label = label

    def set_notes(self, notes):
        self.notes = notes

    def set_type_override(self, type_override):
        self.type_override = type_override

    def to_dict(self):
        return {
            'fields': [{
                'id': self.id,
                'type': self.type,
                'info': {
                    'label': self.label,
                    'notes': self.notes,
                    'type_override': self.type_override 
                }
            }]               
        }

class DataDictionary:
    """
    Metadata of a Dataset and Distributions. CKAN Data Dictionary Fields.
    
    See https://docs.ckan.org/en/2.9/maintaining/datastore.html#fields for more information.
    
    Attributes:
        resource_id (str): The ID of the resource (distribution) to which the data dictionary fields belong.
    """
    def __init__(self, resource_id = None):
        """
        Initialize the object with default values.
        """
        self.resource_id = resource_id
        self.datadictionary_fields = [] 

    def set_datadictionary_resource_id(self, resource_id):
        self.resource_id = resource_id

    def set_datadictionary_fields(self, datadictionary_fields):
        self.datadictionary_fields = datadictionary_fields

    def add_datadictionary_field(self, datadictionary_field):
        self.datadictionary_fields.append(datadictionary_field)

    def dataset_dict(self):
        '''
        Return a dictionary representation of the dataset.
        
        See
            CKAN API 'resource_dictionary_create': https://docs.ckan.org/en/2.9/maintaining/datastore.html#fields
            
            Example:
                {
                    "resource_id":"7a50a2c8-7af5-46bc-b87d-272978c58a78 - REQUIRED",
                    "fields": [{
                            "id": "name - REQUIRED",
                            "type": "text - REQUIRED",
                            "info": {
                                "label": "",
                                "notes": "",
                                "type_override": ""
                            }
                        },{
                            "id": "time",
                            "type": "time",
                            "info": {
                                "label": "Time Label",
                                "notes": "This is the time field",
                                "type_override": "timestamp"
                            }
                        }
                        
                    ]
                }
        '''

        # Put the details of the dataset we're going to create into a dict.
        dataset_dict = {
            "resource_id": self.resource_id,
            "fields": [
                {
                    "id": field.id,
                    "type": field.type,
                    "info": {
                        "label": field.label,
                        "notes": field.notes,
                        "type_override": field.type_override
                    }
                }
                for field in self.datadictionary_fields
            ]
        }

        return dataset_dict

    def generate_data(self):
        """
        Generate data for posting to CKAN.
        """
        dataset_dict = self.dataset_dict()
        # Use the json module to dump the dictionary to a string for posting.
        quoted_data = urllib.parse.quote(json.dumps(dataset_dict))
        byte_data = quoted_data.encode('utf-8')
        return byte_data