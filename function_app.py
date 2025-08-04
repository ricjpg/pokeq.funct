import azure.functions as func
import datetime
import json
import logging
import os, io
import requests
from dotenv import load_dotenv
import pandas as pd
from azure.storage.blob import BlobServiceClient



app = func.FunctionApp()
load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("__name__")


DOMAIN = os.getenv("DOMAIN")
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
BLOB_CONTAINER_NAME = os.getenv("BLOB_CONTAINER_NAME")
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")

@app.queue_trigger(arg_name="azqueue", 
                   queue_name="requests",
                   connection="QueueAzureWebJobStorage") 
def QueueTriggerPokeReport(azqueue: func.QueueMessage):
    body = azqueue.get_body().decode('utf-8')
    record = json.loads(body)
    id = record[0]["id"]
    update_request(id, "inprogress")
    requests_info = get_request(id)
    pokemons = get_pokemons(requests_info['type'])
    pokemon_bytes = generate_csv_to_blob(pokemons) #type: ignore
    # logger.info(pokemons)
    blob_name = f"poke_report_{id}.csv"
    upload_csv_to_blob(blob_name=blob_name, csv_data=pokemon_bytes)
    url_completa = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{BLOB_CONTAINER_NAME}/{blob_name}"

    update_request(id, "completed", url=url_completa)
    

def update_request(id: int, status: str, url: str = None) -> dict: #type: ignore
    payload = {
        "status": status,
        "id": id
    }
    if url:
        payload["url"] = url
    response = requests.put(f"{DOMAIN}/api/request", json=payload)
    return response.json()


def get_pokemons( type: str ) -> dict:
    pokeapi_url = f"https://pokeapi.co/api/v2/type/{type}"
    response = requests.get(pokeapi_url, timeout=3000)
    data = response.json()
    pokemon_entries = data.get("pokemon", [] )
    return [ p["pokemon"] for p in pokemon_entries ] # type: ignore

def generate_csv_to_blob( pokemon_list: list ) -> bytes:
    df = pd.DataFrame( pokemon_list )
    output = io.StringIO()
    df.to_csv( output , index=False, encoding='utf-8' )
    csv_bytes = output.getvalue().encode('utf-8')
    output.close()
    return csv_bytes

def upload_csv_to_blob( blob_name: str, csv_data: bytes ):
    try:
        blob_service_client = BlobServiceClient.from_connection_string( AZURE_STORAGE_CONNECTION_STRING) #type: ignore
        blob_client = blob_service_client.get_blob_client( container = BLOB_CONTAINER_NAME, blob=blob_name ) #type: ignore
        blob_client.upload_blob( csv_data , overwrite=True )
    except Exception as e:
        logger.error(f"Error al subir el archivo {e} ")
        raise

def get_request(id: int) -> dict:
    reponse = requests.get( f"{DOMAIN}/api/request/{id}"  )
    return reponse.json()[0]


def delete_request(blob_name: str)->bool:
    try:
        blob_service_client = BlobServiceClient.from_connection_string( AZURE_STORAGE_CONNECTION_STRING) #type: ignore
        blob_client = blob_service_client.get_blob_client( container = BLOB_CONTAINER_NAME, blob=blob_name ) #type: ignore
        if verify_exist(blob_name):
            logger.info(blob_client)
            blob_client.delete_blob()
            return True
        else:
            return False
    except Exception as e:
        logger.error(f"Error al subir el archivo {e} ")
        raise

def verify_exist(blob_name:str) -> bool:
    try:
        blob_service_client = BlobServiceClient.from_connection_string( AZURE_STORAGE_CONNECTION_STRING) #type: ignore
        blob_client = blob_service_client.get_blob_client( container = BLOB_CONTAINER_NAME, blob=blob_name ) #type: ignore
        if blob_client.exists():
            logger.info(f"blob client:  {blob_client}")
            return True
        else:
            return False
    except Exception as e:
        logger.error(f"Error al buscar el archivo {e} ")
        raise


@app.queue_trigger(arg_name="azqueue", 
                   queue_name="deletequeue",
                   connection="QueueAzureWebJobStorage") 
def DeleteQueueTriggerPokeReport(azqueue: func.QueueMessage):
    # logging.info('Python Queue trigger processed a message: %s',
    #             azqueue.get_body().decode('utf-8'))
    body = azqueue.get_body().decode('utf-8')
    record = json.loads(body)
    id = record[0]["id"]
    blob_name = f"poke_report_{id}.csv"
    print(f"tomado por el delete_func_app, id: {id}")
    if delete_request(blob_name):
        print (f"blob file: poke_report_{id}.csv deleted")
    else:
        print (f"blob file: doesnt exist")
    