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
import re
from functools import wraps
from typing import Tuple, Dict, List, Optional

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
from bs4 import BeautifulSoup
from colorama import init, Fore, Style
from simple_salesforce.exceptions import SalesforceGeneralError # For more specific error handling if needed

# Optional: load environment variables from a .env file if available.
if os.path.exists('mb/.env'):
    from dotenv import load_dotenv
    load_dotenv()

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def strip_color_codes(s: str) -> str:
    """Removes ANSI escape codes (color codes) from a string."""
    return re.sub(r'\x1b\[[0-9;]*[mK]', '', s)

# --- Retry Decorator ---
def retry_on_failure(max_retries: int = 3, exceptions: Tuple = (HttpError,), initial_wait: int = 1, backoff: int = 2):
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
def get_image_from_drive(service, file_id: str) -> bytes:
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

def check_drive_access(service, folder_id: str) -> bool:
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

def get_or_create_subdirectory(service, parent_folder_id: str, subdirectory_name: str) -> Optional[str]:
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

def get_service_account_creds(service_account_file: str, scopes: List[str]):
    """
    Returns a credentials object using a service account file and scopes.
    
    Args:
        service_account_file (str): Path to the service account JSON file.
        scopes (list): List of scopes for the credentials.
    
    Returns:
        google.oauth2.service_account.Credentials: The credentials object.
    """
    return service_account.Credentials.from_service_account_file(service_account_file, scopes=scopes)

def init_drive_service(service_account_file: str, scopes: List[str]):
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
def encode_image_to_base64(image_data: bytes) -> str:
    """
    Encodes image data to a base64 string.
    
    Args:
        image_data (bytes): The raw image data.
    
    Returns:
        str: The base64-encoded string.
    """
    return base64.b64encode(image_data).decode('utf-8')

def decode_image_from_base64(encoded_image: str) -> bytes:
    """
    Decodes a base64-encoded image back to bytes.
    
    Args:
        encoded_image (str): The base64-encoded image.
    
    Returns:
        bytes: The decoded image data.
    """
    return base64.b64decode(encoded_image)

def check_existing_image(sf, file_name: str, sf_object_id: str, existing_images: Dict[str, List[str]], file_domain: str) -> Optional[str]:
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

def get_sf_record(sf, object_name: str, record_id: str) -> Dict:
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

def upload_new_image(sf, drive_service, file_id: str, file_name: str, sf_record_id: str, object_name: str, file_domain: str) -> str:
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

def process_image(sf, drive_service, file_info: Dict, existing_images: Dict[str, List[str]], object_name: str, photo_field: str, file_domain: str) -> Tuple[Optional[Dict], str]:
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
                return ({'Id': sf_record_id, photo_field: image_tag}, Fore.GREEN + "Updated" + Style.RESET_ALL)
            else:
                #logging.info("Image and field already updated for %s. Skipping.", file_name)
                return (None, "Skipped")
        else:
            # Upload new image
            image_tag = upload_new_image(sf, drive_service, file_id, file_name, sf_record_id, object_name, file_domain)
            return ({'Id': sf_record_id, photo_field: image_tag}, Fore.GREEN + "Loaded-Linked (Pass)" + Style.RESET_ALL)
    except Exception as e:
        logging.error("Error processing image %s: %s", file_name, e)
        return (None, f"Error: {e}")

def load_sf_data(sf, object_name: str, photo_field: str) -> Tuple[Dict[str, Dict[str,str]], Dict[str, List[str]]]:
    """
    Loads Salesforce data for a given object (Account, Contact, etc.) and returns:
    - A mapping of Migration ID -> {'Id': Salesforce ID, 'Name': Record Name}
    - A dictionary of existing images per record.

    Args:
        sf: Salesforce connection object.
        object_name: Salesforce object type (e.g., 'Account', 'Contact').
        photo_field: Field name where the image tag is stored (needed in query).

    Returns:
        Tuple (id_map, existing_images)
    """
    query = f"SELECT Id, Name, Archdpdx_Migration_Id__c, {photo_field} FROM {object_name}"
    results = sf.query_all(query)

    id_map = {
        rec['Archdpdx_Migration_Id__c']: {'Id': rec['Id'], 'Name': rec.get('Name', 'Unknown Name')}
        for rec in results['records'] if rec.get('Archdpdx_Migration_Id__c')
    }

    existing_images = {rec['Id']: [] for rec in results['records'] if 'Id' in rec}

    query_images = "SELECT Id, PathOnClient, FirstPublishLocationId FROM ContentVersion"
    images_results = sf.query_all(query_images)
    
    for image in images_results['records']:
        record_id = image['FirstPublishLocationId']
        if record_id in existing_images:
            existing_images[record_id].append(image['PathOnClient'])

    return id_map, existing_images

import sys

import sys

import sys

def print_at(x, y, text):
    """Moves the cursor to position (x, y) and prints text without moving to a new line."""
    print(f"\033[{y};{x}H{text}", end="", flush=True)

def process_drive_files(sf, drive_service, folder_id: str, object_name: str, id_map: Dict[str, Dict[str,str]], existing_images: Dict[str, List[str]], file_regex, photo_field: str, file_domain: str) -> Tuple[List[Dict[str, str]], List[Dict[str,str]]]:
    """
    Processes drive files from the 'Accounts' or 'Contacts' subdirectory.
    Returns a list of update records for Salesforce and a list of all processed Salesforce records info for CSV.
    While processing, displays an in-place progress bar.
    """
    update_records = []
    all_processed_sf_records_info = [] # List to store info for CSV output
    object_id_key = f"sf_{object_name.lower()}_id"

    subdirectory_name = "Accounts" if object_name == "Account" else "Contacts"
    subdirectory_id = get_or_create_subdirectory(drive_service, folder_id, subdirectory_name)
    if not subdirectory_id:
        logging.error(f"Error finding or creating the subdirectory '{subdirectory_name}' in folder {folder_id}")
        # Add an entry for the CSV to indicate this general error
        all_processed_sf_records_info.append({
            'Salesforce_Id': None,
            'Record_Name': None,
            'MigrationId': None,
            'Processed_File_Name': f"Subdirectory: {subdirectory_name}",
            'Processing_Result': f"Error finding or creating subdirectory '{subdirectory_name}'"
        })
        return update_records, all_processed_sf_records_info

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
            items = results.get('files', [])
            if items:
                 all_items.extend(items)
            page_token = results.get('nextPageToken', None)
            if not page_token:
                break
        except HttpError as error:
            if error.resp.status == 429:
                retry_after = int(error.resp.headers.get('Retry-After', 1))
                logging.warning("Rate limit exceeded. Retrying in %s seconds...", retry_after)
                time.sleep(retry_after + 1)
            else:
                logging.error("Unexpected error listing Drive files: %s", error)
                all_processed_sf_records_info.append({
                    'Salesforce_Id': None,
                    'Record_Name': None,
                    'MigrationId': None,
                    'Processed_File_Name': "Drive File Listing",
                    'Processing_Result': f"Unexpected error listing Drive files: {error}"
                })
                # Decide if to raise or return; returning allows CSV to be written with the error.
                return update_records, all_processed_sf_records_info # Or raise error

    total_files = len(all_items)
    if total_files == 0:
        print(f"No image files found in the subdirectory '{subdirectory_name}'")
        all_processed_sf_records_info.append({
            'Salesforce_Id': None,
            'Record_Name': None,
            'MigrationId': None,
            'Processed_File_Name': f"Subdirectory: {subdirectory_name}",
            'Processing_Result': "No image files found"
        })
        return update_records, all_processed_sf_records_info

    total_files_processed_successfully = 0 # Renamed for clarity
    records_updated = 0
    records_skipped = 0
    processing_errors_count = 0 # Renamed for clarity

    print("") 
    print("") 
    header = f"{'Processing File':17} {'Record Name':80} {'External ID':14} {'Result':20}"
    separator = "-" * len(header)
    print(separator)
    print(header)
    print(separator)
    details_count = 0 

    for idx, item in enumerate(all_items, start=1):
        file_id = item['id']
        file_name = item['name']
        
        # Initialize data for CSV row
        current_file_csv_info = {
            'Salesforce_Id': None,
            'Record_Name': "N/A",
            'MigrationId': "N/A",
            'Processed_File_Name': file_name,
            'Processing_Result': "Unknown"
        }

        match = file_regex.match(file_name)

        progress = idx / total_files
        bar_length = 20 
        filled_length = int(progress * bar_length)
        
        progress_bar_visual = Fore.GREEN + '▒' * filled_length + Style.RESET_ALL + ' ' * (bar_length - filled_length)
        idx_str = str(idx)
        total_files_str = str(total_files)
        max_len_idx = len(total_files_str)
        
        progress_bar_str = f'[{progress_bar_visual}] {int(progress * 100):3}%  Photo {idx_str:>{max_len_idx}} of {total_files_str}'
        
        sys.stdout.write("\033[s") 
        sys.stdout.write(f"\033[{details_count + 4}A")
        sys.stdout.write("\033[K" + progress_bar_str + " " * 10) 
        sys.stdout.write("\033[u") 
        sys.stdout.flush()

        console_record_name = "-"
        console_migration_id = "-"
        console_result_msg_colored = ""

        if match:
            record_number = match.group(1).lstrip('0')
            migration_id = f"Parishes_{record_number}" if object_name == 'Account' else str(record_number)
            current_file_csv_info['MigrationId'] = migration_id
            console_migration_id = migration_id
            
            record_data_from_map = id_map.get(migration_id)
            sf_record_id = None
            record_name = "Unknown"

            if record_data_from_map:
                sf_record_id = record_data_from_map['Id']
                record_name = record_data_from_map['Name']
                current_file_csv_info['Salesforce_Id'] = sf_record_id
                current_file_csv_info['Record_Name'] = record_name
                console_record_name = record_name
            
            print(f"{file_name:17} {console_record_name:80} {console_migration_id:14}", end=" ")

            if sf_record_id:
                file_info = {
                    'file_id': file_id,
                    'file_name': file_name,
                    object_id_key: sf_record_id
                }
                
                update_data, result_msg_raw = process_image(sf, drive_service, file_info, existing_images, object_name, photo_field, file_domain)
                current_file_csv_info['Processing_Result'] = result_msg_raw # Store raw message for CSV
                
                # Colorize for console
                if "Error:" in result_msg_raw:
                    console_result_msg_colored = f"{Fore.RED}{result_msg_raw:20}{Style.RESET_ALL}"
                    processing_errors_count += 1
                elif result_msg_raw == "Skipped":
                    console_result_msg_colored = f"{Fore.YELLOW}{result_msg_raw:20}{Style.RESET_ALL}"
                    records_skipped += 1
                elif "Loaded-Linked & Updated" in result_msg_raw: # Success case
                    console_result_msg_colored = f"{Fore.GREEN}{result_msg_raw:20}{Style.RESET_ALL}"
                    records_updated += 1
                else: # Other neutral messages
                    console_result_msg_colored = f"{result_msg_raw:20}"

                print(console_result_msg_colored)
                total_files_processed_successfully += 1 # Count files that had an SF record to process

                if update_data:
                    update_records.append(update_data)
            else:
                logging.warning(f"No {object_name} found with Archdpdx_Migration_Id__c = {migration_id} for file {file_name}")
                console_result_msg_colored = f"{Fore.YELLOW}{'SF Record Not Found':20}{Style.RESET_ALL}"
                current_file_csv_info['Processing_Result'] = "SF Record Not Found"
                current_file_csv_info['Record_Name'] = "N/A" # Explicitly N/A if no SF record
                print(console_result_msg_colored)
                processing_errors_count += 1
        else:
            logging.error(f"Invalid file name format: {file_name}")
            console_result_msg_colored = f"{Fore.RED}{'Invalid format':20}{Style.RESET_ALL}"
            current_file_csv_info['Processing_Result'] = "Invalid file name format"
            current_file_csv_info['MigrationId'] = "N/A"
            current_file_csv_info['Record_Name'] = "N/A"
            print(f"{file_name:17} {'-':80} {'-':14} {console_result_msg_colored}")
            processing_errors_count += 1
        
        all_processed_sf_records_info.append(current_file_csv_info)
        details_count += 1 
    
    print() 
    print("\n-----------------------------------------------------------")
    print("                  Processing Summary  ")
    print("-----------------------------------------------------------")
    print(f"                      Total files in Drive: {total_files}")
    print(f"    Files processed against SF records: {total_files_processed_successfully}")
    print(f" {object_name}s updated (Loaded-Linked & Updated): {records_updated}")
    print(f"                           {object_name}s skipped: {records_skipped}")
    print(f"                          Processing errors: {processing_errors_count}")
    print("-----------------------------------------------------------")

    return update_records, all_processed_sf_records_info

def perform_bulk_updates(sf, update_records: List[Dict[str, str]], object_name: str) -> List[Dict[str, str]]:
    """
    Performs bulk updates in Salesforce and logs errors to specific files.

    Args:
        sf: Salesforce connection object.
        update_records (list): A list of dictionaries for records to update.
        object_name (str): The Salesforce object API name ('Account' or 'Contact').

    Returns:
        list: A list of results from the bulk update operation.
    """
    if not update_records:
        logging.info(f"No records to update for {object_name}.")
        return []

    # Determine error log filename
    error_log_filename = f'{object_name.lower()}_update_errors.log'
    
    # Get or create a specific logger for these update errors
    error_logger_name = f'{object_name.lower()}_update_errors_logger'
    error_logger = logging.getLogger(error_logger_name)
    error_logger.setLevel(logging.ERROR)
    # Prevent error messages from propagating to the root logger if it's already handling them
    error_logger.propagate = False 

    # Add a file handler for the specific error log, if not already added
    # This check ensures that if this function is called multiple times for the same object,
    # we don't add redundant handlers.
    handler_already_exists = any(
        isinstance(h, logging.FileHandler) and h.baseFilename.endswith(error_log_filename)
        for h in error_logger.handlers
    )

    if not handler_already_exists:
        fh = logging.FileHandler(error_log_filename, mode='a', encoding='utf-8')
        fh.setLevel(logging.ERROR)
        # Custom formatter to include record_id, which we'll pass via 'extra'
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - Record ID: %(record_id)s - Errors: %(message)s')
        fh.setFormatter(formatter)
        error_logger.addHandler(fh)

    results = []
    try:
        logging.info(f"Attempting to bulk update {len(update_records)} {object_name} records.")
        # The simple-salesforce bulk update returns a list of dictionaries
        results = sf.bulk.__getattr__(object_name).update(update_records, batch_size=200, use_serial=True)
        
        successful_updates = 0
        failed_updates = 0

        for result in results:
            if result.get('success'):
                successful_updates += 1
            else:
                failed_updates += 1
                record_id_failed = result.get('id', 'Unknown ID')
                error_messages = result.get('errors', ['Unknown error'])
                # Ensure error_messages is a string for logging
                error_details_str = "; ".join(str(e) for e in error_messages) if isinstance(error_messages, list) else str(error_messages)
                
                log_entry = f"Failed to update {object_name} record."
                # Log to the specific error logger
                error_logger.error(error_details_str, extra={'record_id': record_id_failed})
                # Also log a general message to the main logger if desired, or rely on specific logger
                logging.error(f"Failed to update {object_name} ID {record_id_failed}. Errors: {error_details_str}. See {error_log_filename} for details.")

        logging.info(f"Bulk update for {object_name} completed. Successful: {successful_updates}, Failed: {failed_updates}.")
        if failed_updates > 0:
            logging.warning(f"{failed_updates} {object_name} records failed to update. Check {error_log_filename}.")

    except SalesforceGeneralError as e:
        logging.error(f"Salesforce API error during bulk update for {object_name}: {e}")
        # Log all record IDs that were part of this failed batch, if possible, or a general message
        for record_data in update_records:
            record_id_failed = record_data.get('Id', 'Unknown ID in batch')
            error_logger.error(f"Batch failed due to Salesforce API error: {e}", extra={'record_id': record_id_failed})
    except Exception as e:
        logging.error(f"An unexpected error occurred during bulk update for {object_name}: {e}")
        for record_data in update_records:
            record_id_failed = record_data.get('Id', 'Unknown ID in batch')
            error_logger.error(f"Batch failed due to unexpected error: {e}", extra={'record_id': record_id_failed})
            
    return results

def write_updated_records_to_csv(records_to_write: List[Dict[str, str]], object_name: str, output_file: str):
    """
    Writes processed record details (Accounts or Contacts) to a CSV file,
    matching the information shown on screen.

    Args:
        records_to_write (list): A list of dictionaries, where each dictionary
                                 represents a processed file and its outcome.
                                 Expected keys: 'Salesforce_Id', 'Record_Name', 
                                 'MigrationId', 'Processed_File_Name', 'Processing_Result'.
        object_name (str): The Salesforce object API name ('Account' or 'Contact').
        output_file (str): The name of the CSV file to write.
    """
    if not records_to_write:
        logging.info(f"No records to write to CSV {output_file} for {object_name}.")
        # Create an empty CSV with headers if no records, or just skip
        # For consistency, let's create it with headers if the list is empty but was called
        # Or, if the list can be empty due to "No image files found", then this check is fine.
        # The current logic in process_photos.py calls this only if all_processed_sf_records_info is not empty.

    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        # Define fieldnames based on the keys in all_processed_sf_records_info dictionaries
        fieldnames = ['Processed_File_Name', 'Record_Name', 'Archdpdx_Migration_Id__c', 'Salesforce_Id', 'Processing_Result']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for record_info in records_to_write:
            # Map the keys from record_info to the CSV fieldnames
            row_to_write = {
                'Processed_File_Name': record_info.get('Processed_File_Name'),
                'Record_Name': record_info.get('Record_Name'),
                'Archdpdx_Migration_Id__c': record_info.get('MigrationId'), # Key in dict is MigrationId
                'Salesforce_Id': record_info.get('Salesforce_Id'),
                'Processing_Result': strip_color_codes(str(record_info.get('Processing_Result'))) # Ensure no color codes
            }
            writer.writerow(row_to_write)
    logging.info(f"Successfully wrote {len(records_to_write)} records to {output_file}")