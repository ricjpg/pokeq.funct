import azure.functions as func
import datetime
import json
import logging
import os, io
import requests
from dotenv import load_dotenv
import pandas as pd
from azure.storage.blob import BlobServiceClient
import random


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
    df = get_pokemon_full(requests_info['type'], requests_info['sample_size'])
    new_file = data_frame_to_bytes(df)

    

    # logger.info(pokemons)
    blob_name = f"poke_report_{id}.csv"
    upload_csv_to_blob(blob_name=blob_name, csv_data=new_file)
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


def generate_better_csv_to_blob( pokemon_list: pd.DataFrame ) -> bytes:
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
    print(reponse)
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
    
    

def get_pokemon_full(type:str, sample_size:int)-> pd.DataFrame:
    try:
        pokeapi_url = f"https://pokeapi.co/api/v2/type/{type}"
        response = requests.get(pokeapi_url, timeout=3000)
        data = response.json()
        pokemon_list = data.get("pokemon", [])
        list_len = len(pokemon_list)
        #Escenario no valido: Donde el sample es invalido o es mayor que los registros obtenidos
        if sample_size == 0 or sample_size >= list_len:
            sample_pokemon_list = pokemon_list
        #Escenario valido: Donde el sample es un numero entre 1 y el tamaño de registros obtenidos
        else:
            sample_pokemon_list = random.sample(pokemon_list, k = sample_size)

    except requests.RequestException as e:
        print(f"Error al obtener datos del Pokemon tipo: {type}: {e}")
        raise Exception

    # Crear DataFrame
    df = create_pokemon_dataframe(sample_pokemon_list)
    # df.to_csv(f"{type}.csv", index=False, encoding='utf-8')
    return df

def data_frame_to_bytes(data: pd.DataFrame)->bytes:
  output = io.StringIO()
  data.to_csv( output , index=False, encoding='utf-8' )
  csv_bytes = output.getvalue().encode('utf-8')
  output.close()
  return csv_bytes


# Crea y retorna un dataframe con todos los datos de los pokemons
def create_pokemon_dataframe(pokemon_list):
    flat_data_list = process_pokemon_list_for_dataframe(pokemon_list)

    # Crear DataFrame
    df = pd.DataFrame(flat_data_list)

    # Reordenar columnas para mejor legibilidad
    base_columns = ['name', 'url']
    stat_columns = ['hp', 'attack', 'defense', 'special-attack', 'special-defense', 'speed']
    ability_summary_columns = ['total_abilities', 'all_abilities']

    # Obtener columnas de abilities individuales
    ability_detail_columns = [col for col in df.columns if col.startswith('ability_')]
    ability_detail_columns.sort()  # Ordenar para consistencia

    # Crear orden final de columnas
    column_order = base_columns + stat_columns + ability_summary_columns + ability_detail_columns

    # Reordenar solo las columnas que existen
    existing_columns = [col for col in column_order if col in df.columns]
    df = df[existing_columns]

    return df


# Toma una lista de pokemos con sus url's y devuelve datos estructurados para el dataframe
def process_pokemon_list_for_dataframe(pokemon_list):
    pokemon_data_list = []

    for pokemon_item in pokemon_list:
        pokemon_data = extract_pokemon_data_for_dataframe(pokemon_item)
        if pokemon_data:
            pokemon_data_list.append(pokemon_data)
    return pokemon_data_list

# Extrae la información de cada pokemon y la estructura para el dataframe
def extract_pokemon_data_for_dataframe(pokemon_list_item):

    # Extraer nombre y URL del Pokemon
    pokemon_name = pokemon_list_item['pokemon']['name']
    pokemon_url = pokemon_list_item['pokemon']['url']

    # Hacer petición a la URL del Pokemon para obtener stats y abilities
    try:
        response = requests.get(pokemon_url)
        response.raise_for_status()
        pokemon_data = response.json()
    except requests.RequestException as e:
        print(f"Error al obtener datos del Pokemon {pokemon_name}: {e}")
        return None

    # Crear diccionario base
    flat_data = {
        "name": pokemon_name,
        "url": pokemon_url
    }

    # Extraer stats y agregarlos directamente al diccionario
    for stat_item in pokemon_data['stats']:
        stat_name = stat_item['stat']['name']
        base_stat = stat_item['base_stat']
        flat_data[stat_name] = base_stat

    # Extraer abilities y crear columnas separadas
    abilities = []
    for ability_item in pokemon_data['abilities']:
        ability_name = ability_item['ability']['name']
        abilities.append({
            'name': ability_name
        })

    # Agregar abilities como columnas separadas (ability_1, ability_2, etc.)
    for i, ability in enumerate(abilities, 1):
        flat_data[f'ability_{i}_name'] = ability['name']

    # También agregar una columna con todas las abilities como string separadas por comas
    ability_names = [ability['name'] for ability in abilities]
    flat_data['all_abilities'] = ', '.join(ability_names)
    flat_data['total_abilities'] = len(abilities)

    return flat_data