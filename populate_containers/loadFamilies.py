from azure.cosmos import CosmosClient, PartitionKey, documents
from azure.cosmos import  exceptions, errors
import json
import uuid 

with open('.cosmosDBconfig.json', 'r') as jsonfile:
    config = json.load(jsonfile)

HOST = config['host']
MASTER_KEY = config['master_key']
DATABASE_NAME = config['database_id']
CONTAINER_ID = 'families'

client = CosmosClient(config['host'], config['master_key'])

# Create and get a database handle
try:
    database = client.create_database(DATABASE_NAME)
except exceptions.CosmosResourceExistsError:
    database = client.get_database_client(DATABASE_NAME)

# Create and/or get a container
container = database.create_container_if_not_exists( id=CONTAINER_ID ,
 partition_key=PartitionKey(path='/Address/state') )

# Open the json file and read it into a list
with open('json/families.json', 'r') as jsonDataFile:
    data = json.load(jsonDataFile)

print('\n Loading Data into CosmosDB...')
for doc in data['Families']:
    try:
        container.upsert_item(body=doc)
    except errors.CosmosHttpResponseError as e:
     raise

## Run a query against the container to see number of documents
query = 'SELECT VALUE COUNT(1) FROM c'
result = list(container.query_items(query, enable_cross_partition_query=True))

print('Container with id \'{0}\' contains \'{1}\' items'.format(container.id, result[0]))