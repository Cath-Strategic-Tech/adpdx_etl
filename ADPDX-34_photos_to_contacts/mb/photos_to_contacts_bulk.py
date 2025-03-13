import os
import re
import time
import csv
import logging
from dotenv import load_dotenv
from simple_salesforce import Salesforce
from googleapiclient.errors import HttpError

# Import common functions from common_functions.py
from common_functions import (
    check_drive_access,
    get_or_create_subdirectory,
    init_drive_service,
    load_sf_data,
    process_drive_files,
    perform_bulk_updates,
    write_updated_records_to_csv
)

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='photo_upload.log',
    filemode='a'
)

# --- Load environment variables ---
load_dotenv()

# --- Salesforce Configuration ---
SF_USERNAME = os.getenv('SF_USERNAME')
SF_PASSWORD = os.getenv('SF_PASSWORD')
SF_SECURITY_TOKEN = os.getenv('SF_SECURITY_TOKEN')
SF_DOMAIN = os.getenv('SF_DOMAIN')
sf = Salesforce(username=SF_USERNAME, password=SF_PASSWORD, security_token=SF_SECURITY_TOKEN, domain=SF_DOMAIN)

# --- Google Drive Configuration ---
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
SERVICE_ACCOUNT_FILE = os.getenv('SERVICE_ACCOUNT_FILE')
FOLDER_ID = os.getenv('FOLDER_ID')
FILE_DOMAIN = os.getenv('FILE_DOMAIN')  # Base URL for Salesforce Files

# Photo field to update in Salesforce Contact
PHOTO_FIELD = os.getenv('PHOTO_FIELD')

# Initialize Google Drive service using common function
drive_service = init_drive_service(SERVICE_ACCOUNT_FILE, SCOPES)

def main():
    # Load contact mapping and existing images from Salesforce
    contact_id_map, contact_photo_field_map, existing_images = load_sf_data(sf, 'Contact', PHOTO_FIELD)
    
    # Check Google Drive folder access
    if not check_drive_access(drive_service, FOLDER_ID):
        logging.error("Access denied to Google Drive folder %s", FOLDER_ID)
        return
    
    # Compile regex for file name parsing (expected format: "photo<number>.jpg")
    file_regex = re.compile(r"photo(\d+)\.jpg", re.IGNORECASE)
    
    # Process drive files to collect update records
    update_records = process_drive_files(
        service=drive_service,
        folder_id=FOLDER_ID,
        object_name='Contact',  # Specify the object type
        id_map=contact_id_map,
        photo_field_map=contact_photo_field_map,
        existing_images=existing_images,
        file_regex=file_regex,
        photo_field=PHOTO_FIELD
    )
    
    # Perform bulk updates in Salesforce
    updated_contacts = perform_bulk_updates(sf, update_records, 'Contact')
    
    # Write updated contact details to CSV
    write_updated_records_to_csv(sf, updated_contacts, contact_id_map, 'Contact', 'updated_contacts.csv')

if __name__ == '__main__':
    main()