
from azure.cosmos import CosmosClient, PartitionKey, documents
from azure.cosmos import  exceptions, errors
import json
import uuid 

with open('.cosmosDBconfig.json', 'r') as jsonfile:
    config = json.load(jsonfile)

HOST = config['host']
MASTER_KEY = config['master_key']
DATABASE_NAME = config['database_id']
CONTAINER_ID = 'customerOrders'

client = CosmosClient(config['host'], config['master_key'])

try:
    database = client.create_database(DATABASE_NAME)
except exceptions.CosmosResourceExistsError:
    database = client.get_database_client(DATABASE_NAME)

container = database.create_container_if_not_exists(id=CONTAINER_ID, 
 partition_key=PartitionKey(path='/email') )

# Open the json file and read it into a list
with open('json/CustomerSales.json', 'r') as jsonDataFile:
    data = json.load(jsonDataFile)

print('\n Loading Data into CosmosDB, this may take a few minutes...')
for doc in data:
    doc['id'] = str(doc['customer_id'])
    try:
        container.upsert_item(body=doc)
    except errors.CosmosHttpResponseError as e:
     raise

## Run a query against the container to see number of documents
query = 'SELECT VALUE COUNT(1) FROM c'
result = list(container.query_items(query, enable_cross_partition_query=True))

print('Container with id \'{0}\' contains \'{1}\' items'.format(container.id, result[0]))



