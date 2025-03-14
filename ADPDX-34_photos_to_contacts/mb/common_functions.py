"""
common_functions.py

This module contains common helper functions for connecting to Google Drive and Salesforce,
handling API retries, and performing image encoding/decoding. These functions can be
imported in multiple notebooks to avoid code duplication.
"""

import os
import io
import sys
import csv
import time
import base64
import logging
from functools import wraps

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
from bs4 import BeautifulSoup

# Optional: load environment variables from a .env file if available.
if os.path.exists('mb/.env'):
    from dotenv import load_dotenv
    load_dotenv()

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# --- Retry Decorator ---
def retry_on_failure(max_retries=3, exceptions=(HttpError,), initial_wait=1, backoff=2):
    """
    Decorator to retry a function upon specified exceptions.
    
    Args:
        max_retries (int): Maximum number of retries.
        exceptions (tuple): Exception types to catch.
        initial_wait (int): Initial wait time in seconds.
        backoff (int): Multiplier for wait time.
    
    Returns:
        The result of the function call if successful.
    """
    def decorator_retry(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            wait = initial_wait
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if hasattr(e, 'resp') and e.resp.status in [429, 500, 502, 503, 504]:
                        if retries < max_retries - 1:
                            logging.warning("Error in %s: %s. Retrying in %s seconds...", func.__name__, e, wait)
                            time.sleep(wait)
                            retries += 1
                            wait *= backoff
                        else:
                            logging.error("Max retries reached in %s with args: %s, kwargs: %s", func.__name__, args, kwargs)
                            raise
                    else:
                        logging.error("Error in %s: %s", func.__name__, e)
                        raise
        return wrapper
    return decorator_retry

# --- Google Drive Functions ---
@retry_on_failure()
def get_image_from_drive(service, file_id):
    """
    Downloads an image from Google Drive using retries on failure.
    
    Args:
        service: The authenticated Google Drive service object.
        file_id (str): The ID of the file to download.
    
    Returns:
        bytes: The downloaded image data.
    """
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    return fh.getvalue()

def check_drive_access(service, folder_id):
    """
    Checks if the specified Google Drive folder is accessible.
    
    Args:
        service: The authenticated Google Drive service object.
        folder_id (str): The ID of the folder to check.
    
    Returns:
        bool: True if accessible, False otherwise.
    """
    try:
        service.files().list(q=f"'{folder_id}' in parents", pageSize=1).execute()
        return True
    except HttpError as error:
        if error.resp.status == 403:
            logging.error("Access denied to folder %s", folder_id)
            return False
        else:
            raise error

def get_or_create_subdirectory(service, parent_folder_id, subdirectory_name):
    """
    Retrieves or creates a subdirectory within the specified parent folder in Google Drive.
    
    Args:
        service: The authenticated Google Drive service object.
        parent_folder_id (str): The ID of the parent folder.
        subdirectory_name (str): The name of the subdirectory.
    
    Returns:
        str: The ID of the subdirectory, or None if an error occurs.
    """
    try:
        results = service.files().list(
            q=f"'{parent_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and name='{subdirectory_name}'",
            fields="files(id)"
        ).execute()
        items = results.get('files', [])
        if items:
            return items[0]['id']
        else:
            file_metadata = {
                'name': subdirectory_name,
                'parents': [parent_folder_id],
                'mimeType': 'application/vnd.google-apps.folder'
            }
            file = service.files().create(body=file_metadata, fields='id').execute()
            return file.get('id')
    except HttpError as error:
        logging.error("Error getting/creating subdirectory %s: %s", subdirectory_name, error)
        return None

def get_service_account_creds(service_account_file, scopes):
    """
    Returns a credentials object using a service account file and scopes.
    
    Args:
        service_account_file (str): Path to the service account JSON file.
        scopes (list): List of scopes for the credentials.
    
    Returns:
        google.oauth2.service_account.Credentials: The credentials object.
    """
    return service_account.Credentials.from_service_account_file(service_account_file, scopes=scopes)

def init_drive_service(service_account_file, scopes):
    """
    Initializes the Google Drive service using the service account file and scopes.
    
    Args:
        service_account_file (str): Path to the service account JSON file.
        scopes (list): List of scopes for the credentials.
    
    Returns:
        googleapiclient.discovery.Resource: The Google Drive service object.
    """
    creds = get_service_account_creds(service_account_file, scopes)
    return build('drive', 'v3', credentials=creds)

# --- Utility Functions ---
def encode_image_to_base64(image_data):
    """
    Encodes image data to a base64 string.
    
    Args:
        image_data (bytes): The raw image data.
    
    Returns:
        str: The base64-encoded string.
    """
    return base64.b64encode(image_data).decode('utf-8')

def decode_image_from_base64(encoded_image):
    """
    Decodes a base64-encoded image back to bytes.
    
    Args:
        encoded_image (str): The base64-encoded image.
    
    Returns:
        bytes: The decoded image data.
    """
    return base64.b64decode(encoded_image)

def check_existing_image(sf, file_name, sf_object_id, existing_images, file_domain):
    """
    Checks if an image already exists in Salesforce for the account and returns its HTML tag.
    """
    if file_name in existing_images.get(sf_object_id, []):
        query = (
            f"SELECT Id, ContentDocumentId FROM ContentVersion "
            f"WHERE PathOnClient = '{file_name}' AND FirstPublishLocationId = '{sf_object_id}'"
        )
        result = sf.query(query)
        if result['totalSize'] > 0:
            content_version_id = result['records'][0]['Id']
            content_document_id = result['records'][0]['ContentDocumentId']
            image_url = (
                f'{file_domain}/sfc/servlet.shepherd/version/renditionDownload?'
                f'rendition=ORIGINAL_Jpg&versionId={content_version_id}&operationContext=CHATTER&contentId={content_document_id}'
            )
            return f'<p><img src="{image_url}" alt="{file_name}"></img></p>'
        else:
            raise Exception(f"Image {file_name} exists in Drive but not in Salesforce")
    return None
# Additional common functions can be added here if needed.

def get_sf_record(sf, object_name, record_id):
    """
    Retrieves a Salesforce record (Account, Contact, etc.) or raises an exception if not found.
    
    :param sf: Salesforce connection object
    :param object_name: Name of the Salesforce object (e.g., 'Account', 'Contact')
    :param record_id: ID of the record to retrieve
    :return: Salesforce record as a dictionary
    """
    try:
        record = getattr(sf, object_name).get(record_id)
        if not record:
            raise Exception(f"{object_name} with Id {record_id} not found")
        return record
    except AttributeError:
        raise Exception(f"Invalid Salesforce object: {object_name}")

def upload_new_image(sf, drive_service, file_id, file_name, sf_record_id, object_name, file_domain):
    """
    Uploads a new image to Salesforce and returns the HTML tag with the image URL.
    
    Args:
        sf: Salesforce connection object.
        drive_service: The authenticated Google Drive service object.
        file_id: ID of the file in Google Drive.
        file_name: Name of the file.
        sf_record_id: Salesforce record ID where the image will be uploaded.
        object_name: Salesforce object type (e.g., 'Account', 'Contact').
        file_domain: The Salesforce file domain.
    
    Returns:
        str: HTML tag with the image URL.
    """
    image_data = get_image_from_drive(drive_service, file_id)
    encoded_image = encode_image_to_base64(image_data)
    
    # Verify that the record exists
    get_sf_record(sf, object_name, sf_record_id)

    result = sf.ContentVersion.create({
        'PathOnClient': file_name,
        'Title': file_name,
        'VersionData': encoded_image,
        'FirstPublishLocationId': sf_record_id
    })
    
    content_version_id = result['id']
    content_version = sf.ContentVersion.get(content_version_id)
    content_document_id = content_version['ContentDocumentId']
    
    image_url = (
        f'{file_domain}/sfc/servlet.shepherd/version/renditionDownload?'
        f'rendition=ORIGINAL_Jpg&versionId={content_version_id}&operationContext=CHATTER&contentId={content_document_id}'
    )
    
    return f'<p><img src="{image_url}" alt="{file_name}" /></p>'

def process_image(sf, drive_service, file_info, existing_images, object_name, photo_field, file_domain):
    """
    Processes an image for a Salesforce record (Account, Contact, etc.):
    checks if it exists or uploads a new one, and returns update data along with a result message.
    
    Args:
        sf: Salesforce connection object.
        drive_service: The authenticated Google Drive service object.
        file_info: Dictionary with file details.
        existing_images: Dictionary of existing images for comparison.
        object_name: Salesforce object type (e.g., 'Account', 'Contact').
        photo_field: Field name where the image tag is stored.
        file_domain: The Salesforce file domain.
    
    Returns:
        tuple: (update_data, result_message)
            update_data (dict or None): Update data if changes are needed.
            result_message (str): A string indicating the result (e.g., "Updated", "Loaded-Linked (Pass)", "Skipped").
    """
    file_id = file_info['file_id']
    file_name = file_info['file_name']
    sf_record_id = file_info[f'sf_{object_name.lower()}_id']
    try:
        # Retrieve the Salesforce record
        sf_record = get_sf_record(sf, object_name, sf_record_id)

        # Check if the image already exists
        image_tag = check_existing_image(sf, file_name, sf_record_id, existing_images, file_domain)
        if image_tag:
            # Normalize HTML using BeautifulSoup
            soup_image_tag = BeautifulSoup(image_tag, 'html.parser')
            normalized_image_tag = str(soup_image_tag)
            field_value = sf_record.get(photo_field, '')
            if field_value is None:
                field_value = ""
            soup_field_value = BeautifulSoup(field_value, 'html.parser')
            normalized_field_value = str(soup_field_value)

            if normalized_field_value != normalized_image_tag:
                #logging.info("Existing image found but field needs update for %s", file_name)
                return ({'Id': sf_record_id, photo_field: image_tag}, "Updated")
            else:
                #logging.info("Image and field already updated for %s. Skipping.", file_name)
                return (None, "Skipped")
        else:
            # Upload new image
            image_tag = upload_new_image(sf, drive_service, file_id, file_name, sf_record_id, object_name, file_domain)
            return ({'Id': sf_record_id, photo_field: image_tag}, "Loaded-Linked (Pass)")
    except Exception as e:
        logging.error("Error processing image %s: %s", file_name, e)
        return (None, f"Error: {e}")

def load_sf_data(sf, object_name, photo_field):
    """
    Loads Salesforce data for a given object (Account, Contact, etc.) and returns:
    - A mapping of Migration ID -> Salesforce ID
    - A mapping of Migration ID -> Photo field value (if applicable)
    - A dictionary of existing images per record.

    Args:
        sf: Salesforce connection object.
        object_name: Salesforce object type (e.g., 'Account', 'Contact').
        photo_field: Field name where the image tag is stored.

    Returns:
        Tuple (id_map, photo_field_map, existing_images)
    """
    query = f"SELECT Id, Archdpdx_Migration_Id__c, {photo_field} FROM {object_name}"
    results = sf.query_all(query)

    id_map = {
        rec['Archdpdx_Migration_Id__c']: rec['Id']
        for rec in results['records'] if rec.get('Archdpdx_Migration_Id__c')
    }

    photo_field_map = {
        rec['Archdpdx_Migration_Id__c']: rec.get(photo_field, '')
        for rec in results['records'] if rec.get('Archdpdx_Migration_Id__c')
    }

    existing_images = {rec['Id']: [] for rec in results['records'] if 'Id' in rec}

    query_images = "SELECT Id, PathOnClient, FirstPublishLocationId FROM ContentVersion"
    images_results = sf.query_all(query_images)
    
    for image in images_results['records']:
        record_id = image['FirstPublishLocationId']
        if record_id in existing_images:
            existing_images[record_id].append(image['PathOnClient'])

    return id_map, photo_field_map, existing_images

import sys

import sys

import sys

def print_at(x, y, text):
    """Moves the cursor to position (x, y) and prints text without moving to a new line."""
    print(f"\033[{y};{x}H{text}", end="", flush=True)

def process_drive_files(sf, drive_service, folder_id, object_name, id_map, photo_field_map, existing_images, file_regex, photo_field, file_domain):
    """
    Procesa los archivos de Drive desde el subdirectorio 'Accounts' o 'Contacts' y devuelve una lista de registros de actualización.
    Mientras se procesa, muestra una barra de progreso en el lugar en una línea reservada que se actualiza sin sobrescribir
    los detalles del archivo impresos a continuación.
    
    Args:
        sf: Objeto de conexión de Salesforce.
        drive_service: El objeto de servicio autenticado de Google Drive.
        folder_id: El ID de la carpeta principal de Google Drive.
        object_name: El tipo de objeto de Salesforce ('Account' o 'Contact').
        id_map: Un diccionario que asigna los ID de migración a los ID de registro de Salesforce.
        photo_field_map: Un diccionario que asigna los ID de migración al valor actual del campo de la foto en Salesforce.
        existing_images: Un diccionario de las imágenes existentes en Salesforce.
        file_regex: Una expresión regular compilada para coincidir con los nombres de los archivos.
        photo_field: El nombre del campo que se va a actualizar con la etiqueta de la imagen.
        file_domain: El dominio del archivo de Salesforce.
    
    Returns:
        Una lista de diccionarios, donde cada diccionario contiene el ID y el campo de la foto para actualizar para un registro.
    """
    update_records = []
    object_id_key = f"sf_{object_name.lower()}_id"  # Construye la clave basada en object_name

    # Determina el nombre del subdirectorio basado en el tipo de objeto
    subdirectory_name = "Accounts" if object_name == "Account" else "Contacts"

    # Obtiene o crea el subdirectorio dentro de la carpeta principal
    subdirectory_id = get_or_create_subdirectory(drive_service, folder_id, subdirectory_name)
    if not subdirectory_id:
        logging.error(f"Error al encontrar o crear el subdirectorio '{subdirectory_name}' en la carpeta {folder_id}")
        return update_records

    # Primero, reúne todos los archivos de imagen del subdirectorio
    all_items = []
    page_token = None
    while True:
        try:
            results = drive_service.files().list(
                q=f"'{subdirectory_id}' in parents and mimeType contains 'image'",
                fields="nextPageToken, files(id, name)",
                pageSize=1000,
                pageToken=page_token
            ).execute()
            items = results.get('files',)
            all_items.extend(items)
            page_token = results.get('nextPageToken', None)
            if not page_token:
                break
        except HttpError as error:
            if error.resp.status == 429:
                retry_after = int(error.resp.headers.get('Retry-After', 1))
                logging.warning("Límite de velocidad excedido. Reintentando en %s segundos...", retry_after)
                time.sleep(retry_after + 1)
            else:
                logging.error("Error inesperado: %s", error)
                raise

    total_files = len(all_items)
    if total_files == 0:
        print(f"No se encontraron archivos de imagen en el subdirectorio '{subdirectory_name}'")
        return update_records
    
    # Línea en blanco
    print("")
    print("")
    
    # Separador
    header = f"{'Processing File':17} {'Record Name':80} {'External ID':14} {'Result':20}"
    separator = "-" * len(header)
    print(separator)
    
    # Encabezado
    print(header)
    
    # Otro separador
    print(separator)

    # details_count cuenta el número de líneas de detalles de archivo impresas.
    details_count = 0

    # Procesa cada archivo.
    for idx, item in enumerate(all_items, start=1):
        file_id = item['id']
        file_name = item['name']
        match = file_regex.match(file_name)
        
        # Calcular el progreso
        progress = idx / total_files
        bar_length = 20
        filled_length = int(progress * bar_length)
        progress_bar_str = '[' + '▒' * filled_length + ' ' * (bar_length - filled_length) + f'] {int(progress * 100)}%  Photo {idx} of {total_files}'
        
        # Guarda la posición actual del cursor
        sys.stdout.write("\033[s")
        # Mueve el cursor hacia arriba (details_count + 5) líneas para alcanzar la línea reservada de la barra de progreso.
        sys.stdout.write(f"\033[{details_count + 5}A")
        # Borra la línea de la barra de progreso e imprime la nueva barra de progreso
        sys.stdout.write("\033[K" + progress_bar_str + "\n")
        # Imprime una línea en blanco adicional
        sys.stdout.write("\033[K" + "\n")
        # Restaura la posición del cursor para que la nueva línea de detalles del archivo se imprima en la parte inferior
        sys.stdout.write("\033[u")
        sys.stdout.flush()
       

        
        if match:
            record_number = match.group(1).lstrip('0')
            migration_id = f"Parishes_{record_number}" if object_name == 'Account' else str(record_number)
            sf_record_id = id_map.get(migration_id)
            
            # Recupera los detalles del registro para el nombre del registro (si está disponible)
            if sf_record_id:
                record_details = get_sf_record(sf, object_name, sf_record_id)
                record_name = record_details.get("Name", "Unknown")
            else:
                record_name = "Unknown"
            
            # Imprime los detalles del archivo en una nueva línea (añade en la parte inferior)
            print(f"{file_name:17} {record_name:80} {migration_id:14}", end=" ")
            sf_photo_c = photo_field_map.get(migration_id, '') if object_name == 'Contact' else None

            if sf_record_id:
                file_info = {
                    'file_id': file_id,
                    'file_name': file_name,
                    object_id_key: sf_record_id
                }
                if sf_photo_c is not None:
                    file_info['sf_photo_c'] = sf_photo_c

                update_data, result_msg = process_image(sf, drive_service, file_info, existing_images, object_name, photo_field, file_domain)
                print(f"{result_msg:20}")
                if update_data:
                    update_records.append(update_data)
            else:
                logging.error(f"No se encontró {object_name} con Archdpdx_Migration_Id__c = {migration_id}")
                print(f"{'No encontrado':20}")
        else:
            logging.error(f"Formato de nombre de archivo no válido: {file_name}")
            print(f"{file_name:17} {'-':80} {'-':14} {'Formato no válido':20}")
        
        details_count += 1  # Incrementa el recuento de detalles del archivo después de imprimir una línea de detalles

    # Imprime una nueva línea final para que la salida subsiguiente no sea sobrescrita por la barra de progreso
    print()
    return update_records


def perform_bulk_updates(sf, update_records, object_name):
    """
    Performs bulk updates in Salesforce for a specified object (Account or Contact).

    Args:
        sf: The Salesforce connection object.
        update_records (list): A list of dictionaries, where each dictionary
                              represents a record to be updated.
        object_name (str): The Salesforce object API name ('Account' or 'Contact').

    Returns:
        list: A list of updated records.
    """
    updated_records_list = []
    if update_records:
        # Determine the batch size and the update method based on object_name
        batch_size = 100 if object_name == 'Account' else 50
        update_method = getattr(sf.bulk, object_name).update  # Dynamically get the update method

        for i in range(0, len(update_records), batch_size):
            batch = update_records[i:i + batch_size]
            update_method(batch)  # Use the dynamically selected update method
            updated_records_list.extend(batch)
    return updated_records_list

def write_updated_records_to_csv(sf, updated_records, id_map, object_name, output_file):
    """
    Writes updated record details (Accounts or Contacts) to a CSV file.

    Args:
        sf: The Salesforce connection object.
        updated_records (list): A list of dictionaries, where each dictionary
                              represents an updated record.
        id_map (dict): A mapping between migration IDs and Salesforce record IDs.
        object_name (str): The Salesforce object API name ('Account' or 'Contact').
        output_file (str): The name of the CSV file to write.
    """
    with open(output_file, 'w', newline='') as csvfile:
        fieldnames = ['Id', 'Name', 'Archdpdx_Migration_Id__c']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for record in updated_records:
            sf_record_id = record['Id']
            # Dynamically access the object with getattr
            record_details = getattr(sf, object_name).get(sf_record_id)
            # Find the migration ID
            migration_id = next(
                (k for k, v in id_map.items() if v == sf_record_id), None
            )
            writer.writerow({
                'Id': sf_record_id,
                'Name': record_details['Name'],
                'Archdpdx_Migration_Id__c': migration_id
            })