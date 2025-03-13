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
sf = Salesforce(
    username=SF_USERNAME,
    password=SF_PASSWORD,
    security_token=SF_SECURITY_TOKEN,
    domain=SF_DOMAIN
)

# --- Google Drive Configuration ---
SCOPES = ['https://www.googleapis.com/auth/drive.readonly'] 
SERVICE_ACCOUNT_FILE = os.getenv('SERVICE_ACCOUNT_FILE')
FOLDER_ID = os.getenv('FOLDER_ID')
FILE_DOMAIN = os.getenv('FILE_DOMAIN')  # Base URL for Salesforce Files

# Account photo field to update in Salesforce
PHOTO_FIELD_ACCOUNT = os.getenv('PHOTO_FIELD_ACCOUNT')

# Initialize Google Drive service using common function
drive_service = init_drive_service(SERVICE_ACCOUNT_FILE, SCOPES)

def main():
    # Load account mapping and existing images from Salesforce
    account_id_map, account_photo_field_map, existing_images = load_sf_data(sf, 'Account', PHOTO_FIELD_ACCOUNT)
    
    # Check Google Drive folder access
    if not check_drive_access(drive_service, FOLDER_ID):
        logging.error("Access denied to Google Drive folder %s", FOLDER_ID)
        return
    
    # Get or create the 'Accounts' subdirectory (ensure this logic is correct 
    # if you need a specific subfolder, otherwise FOLDER_ID is fine)
    # accounts_folder_id = get_or_create_subdirectory(drive_service, FOLDER_ID, 'Accounts')
    # if not accounts_folder_id:
    #     logging.error("Failed to get or create the 'Accounts' subdirectory.")
    #     return
    
    # Compile regex for file name parsing (expected format: "photo<number>.jpg")
    file_regex = re.compile(r"photo(\d+)\.jpg", re.IGNORECASE)
    
    # Process drive files to collect update records
    update_records = process_drive_files(
        service=drive_service,
        folder_id=FOLDER_ID,  # Or accounts_folder_id if used
        object_name='Account',
        id_map=account_id_map,
        photo_field_map=account_photo_field_map,
        existing_images=existing_images,
        file_regex=file_regex,
        photo_field=PHOTO_FIELD_ACCOUNT
    )
    
    # Perform bulk updates in Salesforce
    updated_accounts = perform_bulk_updates(sf, update_records, 'Account')

    
    # Write updated account details to CSV
    write_updated_records_to_csv(sf, updated_accounts, account_id_map, 'Account', 'updated_accounts.csv')



if __name__ == '__main__':
    main()
