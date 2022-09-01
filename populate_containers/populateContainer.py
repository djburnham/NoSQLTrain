# connect to the cosmosDB database in the config file 
import json 
import urllib.request
from azure.cosmos import CosmosClient, PartitionKey, exceptions, errors

with open('.cosmosDBconfig.json', 'r') as jsonfile:
    config = json.load(jsonfile)

client = CosmosClient(config['host'], config['master_key'])
DATABASE_NAME = config['database_id']

#create the container for the web data
try:
    database = client.create_database(DATABASE_NAME)
except exceptions.CosmosResourceExistsError:
    database = client.get_database_client(DATABASE_NAME)

container = database.create_container_if_not_exists(id='WebsiteData', 
 partition_key=PartitionKey(path='/CartID') )

#populate the container with data from blob storage 
with urllib.request.urlopen("https://cosmosnotebooksdata.blob.core.windows.net/notebookdata/websiteData.json") as url:
    data = json.loads(url.read().decode())

print("Importing data. This will take a few minutes...\n")

for event in data:
    try:
        container.upsert_item(body=event)
    except errors.CosmosHttpResponseError as e:
     raise

## Run a query against the container to see number of documents
query = 'SELECT VALUE COUNT(1) FROM c'
result = list(container.query_items(query, enable_cross_partition_query=True))

print('Container with id \'{0}\' contains \'{1}\' items'.format(container.id, result[0]))