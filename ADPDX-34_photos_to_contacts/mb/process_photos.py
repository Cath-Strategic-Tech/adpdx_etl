import os
import re
import logging
import sys
from dotenv import load_dotenv
from simple_salesforce import Salesforce
from googleapiclient.errors import HttpError
from colorama import init, Fore, Style
init() # Initialize colorama


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

def setup_logging(filename='photo_upload.log', level=logging.INFO, filemode='a'):
    """Configures logging for the script."""
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        filename=filename,
        filemode=filemode
    )

def clear_screen():
    """Clears the terminal screen."""
    os.system('cls' if os.name == 'nt' else 'clear')

def load_environment_variables(script_dir):
    """
    Loads environment variables from a .env file or assumes they are
    defined in the system.

    Args:
        script_dir (str): The directory of the current script.

    Returns:
        str: Path to the .env file (if loaded), or None.
    """
    env_path = os.path.join(script_dir, '.env')
    if os.path.exists(env_path):
        load_dotenv(env_path)
        print(f"Loaded environment variables from {env_path}")
        return env_path
    else:
        print(f".env file not found in {env_path}. Assuming system environment variables are defined.")
        return None

def validate_environment_variables(required_vars):
    """
    Validates that all required environment variables are defined.

    Args:
        required_vars (list): A list of strings, each the name of an environment variable.

    Raises:
        SystemExit: If any required variables are missing.
    """
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        print(f"Error: The following required environment variables are missing: {', '.join(missing_vars)}")
        sys.exit(1)

def initialize_salesforce_connection():
    """
    Initializes a connection to Salesforce using environment variables.

    Returns:
        Salesforce: A Salesforce connection object.
    """
    SF_USERNAME = os.getenv('SF_USERNAME')
    SF_PASSWORD = os.getenv('SF_PASSWORD')
    SF_SECURITY_TOKEN = os.getenv('SF_SECURITY_TOKEN')
    SF_DOMAIN = os.getenv('SF_DOMAIN')

    return Salesforce(
        username=SF_USERNAME,
        password=SF_PASSWORD,
        security_token=SF_SECURITY_TOKEN,
        domain=SF_DOMAIN
    )

def initialize_drive_service(script_dir):
    """
    Initializes the Google Drive service using a service account file.

    Args:
        script_dir (str): The directory of the current script.

    Returns:
        googleapiclient.discovery.Resource: The Google Drive service object.
    """
    SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
    SERVICE_ACCOUNT_FILE = os.getenv('SERVICE_ACCOUNT_FILE')
    return init_drive_service(os.path.join(script_dir, SERVICE_ACCOUNT_FILE), SCOPES)

def process_photos(sf, drive_service, object_name):
    """
    Processes photos for either Accounts or Contacts.

    Args:
        sf (Salesforce): A Salesforce connection object.
        drive_service: The authenticated Google Drive service object.
        object_name (str): The Salesforce object type ('Account' or 'Contact').
    """
    FOLDER_ID = os.getenv('FOLDER_ID')
    FILE_DOMAIN = os.getenv('FILE_DOMAIN')
    
    if object_name == 'Account':
        PHOTO_FIELD = os.getenv('PHOTO_FIELD_ACCOUNT')
        id_map, photo_field_map, existing_images = load_sf_data(sf, object_name, PHOTO_FIELD)
        file_regex = re.compile(r"photo(\d+)\.jpg", re.IGNORECASE)
    elif object_name == 'Contact':
        PHOTO_FIELD = os.getenv('PHOTO_FIELD')
        id_map, photo_field_map, existing_images = load_sf_data(sf, object_name, PHOTO_FIELD)
        file_regex = re.compile(r"photo(\d+)\.jpg", re.IGNORECASE)
    else:
        raise ValueError("Invalid object_name. Must be 'Account' or 'Contact'.")

    if not check_drive_access(drive_service, FOLDER_ID):
        logging.error("Access denied to Google Drive folder %s", FOLDER_ID)
        return

    update_records = process_drive_files(
        sf,  # Pass sf connection
        drive_service=drive_service,
        folder_id=FOLDER_ID,
        object_name=object_name,
        id_map=id_map,
        photo_field_map=photo_field_map,
        existing_images=existing_images,
        file_regex=file_regex,
        photo_field=PHOTO_FIELD,
        file_domain=FILE_DOMAIN
    )

    updated_records = perform_bulk_updates(sf, update_records, object_name)
    write_updated_records_to_csv(sf, updated_records, id_map, object_name, f'updated_{object_name.lower()}s.csv')

def main():
    """
    Main function to orchestrate the photo processing for Accounts or Contacts.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))

    setup_logging()
    load_environment_variables(script_dir)

    required_vars_common = [
        "SF_USERNAME",
        "SF_SECURITY_TOKEN",
        "SF_DOMAIN",
        "FILE_DOMAIN",
        "SERVICE_ACCOUNT_FILE",
        "FOLDER_ID"
    ]

    validate_environment_variables(required_vars_common)

    sf = initialize_salesforce_connection()
    drive_service = initialize_drive_service(script_dir)
    

    while True:
        clear_screen()

        print(" --------------------------------------- ")
        print("    Loading Photos to Salesforce Menu    ")
        print(" --------------------------------------- ")
        print(Fore.GREEN + "  1. PROCESS ACCOUNTS" + Style.RESET_ALL)
        print(Fore.BLUE + "  2. PROCESS CONTACTS" + Style.RESET_ALL)
        print(Fore.RED + "  3. EXIT" + Style.RESET_ALL)1
        print("---------------------------------------")

        choice = input("  Enter your choice (1-3): ")

        if choice == '1':
            clear_screen()
            print(" --------------------------------------- ")
            print(Fore.GREEN+"         PROCESSING ACCOUNTS             " + Style.RESET_ALL)
            print(" --------------------------------------- ")
            print("")
            required_vars_account = required_vars_common + ["PHOTO_FIELD_ACCOUNT"]
            validate_environment_variables(required_vars_account)
            process_photos(sf, drive_service, 'Account')
            print("")
            input("  Press Enter to continue...")  # Pause to show the message
        elif choice == '2':
            clear_screen()
            print(" --------------------------------------- ")
            print(Fore.BLUE + "         PROCESSING CONTACTS             " + Style.RESET_ALL)
            print(" --------------------------------------- ")
            print("")
            required_vars_contact = required_vars_common + ["PHOTO_FIELD"]
            validate_environment_variables(required_vars_contact)
            process_photos(sf, drive_service, 'Contact')
            print("")
            input("  Press Enter to continue...")  # Pause to show the message
        elif choice == '3':
            print("  Exiting program. Goodbye!")
            break
        else:
            print(Fore.YELLOW + "  Invalid choice. Please enter a number between 1 and 3." + Style.RESET_ALL)
            input("  Press Enter to continue...")  # Pause to show the message

if __name__ == "__main__":
    main()