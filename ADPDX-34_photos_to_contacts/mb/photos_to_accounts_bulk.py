import os
import re
import logging
import sys
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


# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(script_dir, '.env')

# Try to load environment variables from .env if it exists,
# otherwise assume system environment variables are set.
if os.path.exists(env_path):
    from dotenv import load_dotenv
    load_dotenv(env_path)
    print(f"Loaded environment variables from {env_path}")
else:
    print(f".env file not found in {env_path}. Assuming system environment variables are defined.")

# List of required environment variables
required_vars = [
    "SF_USERNAME",
    "SF_SECURITY_TOKEN",
    "SF_DOMAIN",
    "FILE_DOMAIN",
    "SERVICE_ACCOUNT_FILE",
    "FOLDER_ID",
    "PHOTO_FIELD_ACCOUNT"
]

# Check if all required variables are defined; abort if any is missing.
missing_vars = [var for var in required_vars if not os.getenv(var)]
if missing_vars:
    print(f"Error: The following required environment variables are missing: {', '.join(missing_vars)}")
    sys.exit(1)


# Now load the environment variables (they are guaranteed to be defined) and other paramters requeried
# --- Salesforce Configuration ---
SF_USERNAME = os.getenv('SF_USERNAME')
SF_PASSWORD = os.getenv('SF_PASSWORD')
SF_SECURITY_TOKEN = os.getenv('SF_SECURITY_TOKEN')
SF_DOMAIN = os.getenv('SF_DOMAIN')

# --- Google Drive Configuration ---
SCOPES = ['https://www.googleapis.com/auth/drive.readonly'] 
SERVICE_ACCOUNT_FILE = os.getenv('SERVICE_ACCOUNT_FILE')
FOLDER_ID = os.getenv('FOLDER_ID')
FILE_DOMAIN = os.getenv('FILE_DOMAIN')  # Base URL for Salesforce Files

# Account photo field to update in Salesforce
PHOTO_FIELD_ACCOUNT = os.getenv('PHOTO_FIELD_ACCOUNT')
print("All required environment variables are loaded successfully.")

# --- Connect to Salesforce Configuration---
sf = Salesforce(
    username=SF_USERNAME,
    password=SF_PASSWORD,
    security_token=SF_SECURITY_TOKEN,
    domain=SF_DOMAIN
)

# Initialize Google Drive service using common function
drive_service = init_drive_service(script_dir+'/'+SERVICE_ACCOUNT_FILE, SCOPES)

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
        sf,
        service=drive_service,
        folder_id=FOLDER_ID,  # Or accounts_folder_id if used
        object_name='Account',
        id_map=account_id_map,
        photo_field_map=account_photo_field_map,
        existing_images=existing_images,
        file_regex=file_regex,
        photo_field=PHOTO_FIELD_ACCOUNT,
        file_domain=FILE_DOMAIN
    )
    
    # Perform bulk updates in Salesforce
    updated_accounts = perform_bulk_updates(sf, update_records, 'Account')

    
    # Write updated account details to CSV
    write_updated_records_to_csv(sf, updated_accounts, account_id_map, 'Account', 'updated_accounts.csv')



if __name__ == '__main__':
    main()
