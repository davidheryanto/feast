# Feast Python SDK

Feast (Feature Store) is a tool to manage storage and access of machine learning features.

It aims to:
- Support ingesting feature data via batch or streaming
- Provide scalable storage of feature data for serving and training
- Provide an API for low latency access of features
- Enable discovery and documentation of features
- Provide an overview of the general health of features in the system

## Pre-requisites

- A working Feast Cluster: Consult your Feast admin or [install your own](https://github.com/gojek/feast/blob/master/docs/install.md).

## Installation

```sh
 pip install -U feast
```

## Getting started

### Configuring Feast client
All interaction with feast cluster happens via an instance of `feast.sdk.client.Client`. 
It should be pointed to correct core/serving urls of the feast cluster:
```python
from feast.sdk.client import Client

feast_client = Client(
    core_url=FEAST_CORE_URL, 
    serving_url=FEAST_SERVING_URL, 
    verbose=True)
```

If you are running a local feast cluster, then `FEAST_CORE_URL` and `FEAST_SERVING_URL`
could be something like:
```python
FEAST_CORE_URL="localhost:8080"
FEAST_SERVING_URL="localhost:8081"
```

### Registering an entity and features
Entities and features could be registered explicitly beforehand or during the data ingestion.

Register your first entity:
```python
from feast.sdk.resources.entity import Entity

entity = Entity(
    name='word',
    description='word found in shakespearean works'
)

feast_client.apply(entity)
```
And a feature, that belongs to this entity:
```python
from feast.sdk.resources.feature import Feature, Datastore, ValueType

word_count_feature = Feature(
    entity='word',
    name='count',
    value_type=ValueType.INT32,                      
    description='number of times the word appears',
    tags=['tag1', 'tag2'],
    owner='bob@feast.com',
    uri='https://github.com/bob/example',
    warehouse_store=Datastore(id='WAREHOUSE'),
    serving_store=Datastore(id='SERVING')    
)

feast_client.apply(word_count_feature)
```
Read more on entity/feature fields here: [Entity Spec](https://github.com/gojek/feast/blob/master/docs/specs.md)


### Ingest data for your feature
Let's create a simple [pandas](https://pandas.pydata.org/) dataframe

```python
import pandas as pd

words_df = pd.DataFrame({
    'word': ['the', 'and', 'i', 'to', 'of', 'a', 'you', 'my', 'in', 'that', 'is', 'not', 'with', 'me', 'it'],
    'count': [28944, 27317, 21120, 20136, 17181, 14945, 13989, 12949, 11513, 11488, 9545, 8855, 8293, 8043, 8003]
    })
```

And import it into the feast store:

```python
from datetime import datetime
from feast.sdk.importer import Importer

STAGING_LOCATION = 'gs://your-bucket'  

importer = Importer.from_df(words_df, 
                           entity='word', 
                           owner='bob@feast.com',
                           id_column='word',
                           timestamp_value=datetime(2018, 1, 1),
                           staging_location=STAGING_LOCATION,
                           serving_store=Datastore(id='SERVING'),
                           warehouse_store=Datastore(id='WAREHOUSE'))
    
feast_client.run(importer)

```

This will start an import job and ingest data into both warehouse and serving feast stores.

You can also import data from:
- CSV file `Importer.from_csv(...)` 
- BigQuery `Importer.from_bq(...)`

### Query feature data for training your models
Now, when you have some data in the feast store, you may want to retrieve that 
dataset and train your model. Creating a training dataset allows you to isolate the data 
that goes into the model training step, allowing for reproduction and traceability. 

Also, it's possible to retrieve only these features required 
for training, by specifying them in a `FeatureSet`:

```python
from feast.sdk.resources.feature_set import FeatureSet

feature_set = FeatureSet(entity="word", features=["word.count"])

training_dataset = feast_client.create_dataset(
                                feature_set, 
                                start_date="2010-01-01", 
                                end_date="2018-01-01")

training_df = feast_client.download_dataset_to_df(training_dataset, staging_location=STAGING_LOCATION)

# train your model
# ...
``` 

### Query feature data for serving your models
Feast provides a means for accessing stored features in a serving environment, 
at low latency and high load. 

You have to provide IDs of entities, features of which you want to get served:

```python
feature_set = FeatureSet(entity="word", features=["word.count"])
serving_df = feast_client.get_serving_data(feature_set, entity_keys=["you", "and", "i"])
```

This will return the following DataFrame:

|     | word | count |
| --- | ---  | ---   |
| 0   | and	 | 27317 |
| 1   | you	 | 13989 |
| 2   | i	   | 21120 |
