import azure.cosmos.documents as documents
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.exceptions as exceptions
from azure.cosmos.partition_key import PartitionKey
import json
import string
import random

# ----------------------------------------------------------------------------------------------------------
# Simple script to test that we can connect to cosmos DB
# ----------------------------------------------------------------------------------------------------------

with open('.cosmosDBconfig.json', 'r') as jsonfile:
    config = json.load(jsonfile)

HOST = config['host']
MASTER_KEY = config['master_key']
CONTAINER_ID = config['container_id']

def randStr(chars = string.ascii_lowercase, N=10):
    '''Function to generate random strings - defaults to 10 lowercase characters '''
    return ''.join(random.choice(chars) for _ in range(N))

def run_env_test():
    '''function to connect to cosmosdb, create a database and a container and then delete the container '''
    client = cosmos_client.CosmosClient(HOST, {'masterKey': MASTER_KEY}, user_agent="CosmosDBPythonQuickstart", user_agent_overwrite=True)
    try:
        # setup database for this sample
        try:
            DATABASE_ID = randStr()
            db = client.create_database(id=DATABASE_ID)
            print('\nDatabase with id \'{0}\' created'.format(DATABASE_ID))

        except exceptions.CosmosResourceExistsError:
            db = client.get_database_client(DATABASE_ID)
            print('\nDatabase with id \'{0}\' was found'.format(DATABASE_ID))

        # Create the container for this sample
        try:
            container = db.create_container(id=CONTAINER_ID, partition_key=PartitionKey(path='/partitionKey'))
            print('\nContainer with id \'{0}\' created'.format(CONTAINER_ID))

        except exceptions.CosmosResourceExistsError:
            container = db.get_container_client(CONTAINER_ID)
            print('Container with id \'{0}\' was found'.format(CONTAINER_ID))

        # cleanup database after sample
        try:
            client.delete_database(db)
            print('\nDatabase with id \'{0}\' was deleted'.format(db.id))
        except exceptions.CosmosResourceNotFoundError:
            print('\nDid not find database to delete - had you already deleted it ?')

    except exceptions.CosmosHttpResponseError as e:
        print('\nrun_sample has caught an error. {0}'.format(e.message))

    finally:
            print("\nenv_test done....\n")


if __name__ == '__main__':
    run_env_test()
