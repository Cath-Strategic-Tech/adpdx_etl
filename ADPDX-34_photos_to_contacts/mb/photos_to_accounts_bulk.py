import os
import io
from googleapiclient.discovery import build
from google.oauth2 import service_account
from simple_salesforce import Salesforce
from googleapiclient.http import MediaIoBaseDownload
import base64
from dotenv import load_dotenv
import logging
import csv
import time
from googleapiclient.errors import HttpError

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

# URL base de Salesforce Files
FILE_DOMAIN = os.getenv('FILE_DOMAIN') 

# Photo field to update in Salesforce
PHOTO_FIELD_ACCOUNT = os.getenv('PHOTO_FIELD_ACCOUNT')

# Initialize Google Drive service
creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
drive_service = build('drive', 'v3', credentials=creds)

def check_drive_access(drive_service, folder_id):
    """Verifica si se puede acceder a la carpeta de Google Drive."""
    try:
        drive_service.files().list(
            q=f"'{folder_id}' in parents",
            pageSize=1  # Solo necesitamos verificar si podemos listar algo
        ).execute()
        return True  # Acceso concedido
    except HttpError as error:
        if error.resp.status == 403:  # Permiso denegado
            return False
        else:
            raise error  # Otros errores deben ser manejados

def get_image_from_drive(service, file_id):
    """Downloads an image from Google Drive given its file ID,
    with retry logic for rate limiting."""
    retries = 3
    for attempt in range(retries):
        try:
            request = service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
            return fh.getvalue()
        except HttpError as error:
            if error.resp.status in [429, 500, 502, 503, 504]: 
                if attempt < retries - 1: 
                    wait_time = 2 ** attempt 
                    print(f"Rate limit exceeded. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    raise error 
            else:
                raise error

def upload_image_and_get_update_data(sf, file_info, existing_images):
    """Uploads image to Salesforce, links to Account, and updates PHOTO_FIELD_ACCOUNT."""
    try:
        file_id, file_name, sf_account_id = file_info['file_id'], file_info['file_name'], file_info['sf_account_id']
        
        # Scenario 2: Image exists, check PHOTO_FIELD format
        account = sf.Account.get(sf_account_id)
        if not account:
            raise Exception(f"No account found with Id = {sf_account_id}")           
        # Verifica si la imagen ya existe en Salesforce para este contacto (usando sf_account_id)
        if file_name in existing_images.get(sf_account_id, []):
            # Query para encontrar la versión de contenido (ContentVersion) correspondiente
            query = f"SELECT Id, ContentDocumentId FROM ContentVersion WHERE PathOnClient = '{file_name}' AND FirstPublishLocationId = '{sf_account_id}'"
            result = sf.query(query)

            if result['totalSize'] > 0:
                content_version_id = result['records'][0]['Id']
                content_document_id = result['records'][0]['ContentDocumentId']

                image_url = f'{FILE_DOMAIN}/sfc/servlet.shepherd/version/renditionDownload?rendition=ORIGINAL_Jpg&amp;versionId={content_version_id}&amp;operationContext=CHATTER&amp;contentId={content_document_id}'
                expected_image_tag = f'<p><img src="{image_url}" alt="{file_name}"></img></p>'

                if account.get(PHOTO_FIELD_ACCOUNT,'') != expected_image_tag:
                    print(f"Image exists, but {PHOTO_FIELD_ACCOUNT} needs update for {file_name}. Updating field...")
                    return {'Id': sf_account_id, PHOTO_FIELD_ACCOUNT: expected_image_tag}
                else:
                    print(f"Image already exists and {PHOTO_FIELD_ACCOUNT} is correct for {file_name}. Skipping.")
                    return None

            else:
                raise Exception(f"Image file found in drive but not in Salesforce for {file_name}.")
           
        # Scenario 1: Image does not exist, upload and update
        image_data = get_image_from_drive(drive_service, file_id)
        encoded_image = base64.b64encode(image_data).decode('utf-8')

        # Check if the account exists
        account = sf.Account.get(sf_account_id)
        if not account:
            raise Exception(f"No account found with Id = {sf_account_id}")        

        result = sf.ContentVersion.create({
            'PathOnClient': file_name,
            'Title': file_name,
            'VersionData': encoded_image,
            'FirstPublishLocationId': sf_account_id  
        })

        content_version_id = result['id']  
        content_version = sf.ContentVersion.get(content_version_id)
        content_document_id = content_version['ContentDocumentId']

        # Create HTML tag with ContentDocument URL
        image_url = f'{FILE_DOMAIN}/sfc/servlet.shepherd/version/renditionDownload?rendition=ORIGINAL_Jpg&versionId={content_version_id}&operationContext=CHATTER&contentId={content_document_id}'
        image_tag = f'<p><img src="{image_url}" alt="{file_name}" /></p>'

        return {'Id': sf_account_id, PHOTO_FIELD_ACCOUNT: image_tag}  
    except Exception as e:
        print(f"Error uploading/linking image {file_name} to Account: {e}")
        logging.error(f"Error processing {file_name}: {e}")
        return None 

def main():
    # Fetch all account IDs in one query, for efficiency
    query = f"SELECT Id, Archdpdx_Migration_Id__c, {PHOTO_FIELD_ACCOUNT} FROM Account"
    account_results = sf.query_all(query)
    account_id_map = {c['Archdpdx_Migration_Id__c']: c['Id'] 
           for c in account_results['records'] 
           if 'Archdpdx_Migration_Id__c' in c and c['Archdpdx_Migration_Id__c']}

    # Verify access to Google Drive folder on startup
    if not check_drive_access(drive_service, FOLDER_ID):
        print("Error: Access denied to Google Drive folder. Check your credentials and permissions.")
        return  # Exit the program if no access

    updated_accounts = []
    page_token = None

    # Consulta única para obtener todas las imágenes existentes
    query = "SELECT Id, PathOnClient, FirstPublishLocationId FROM ContentVersion"
    existing_images_results = sf.query_all(query)

    # Crear un diccionario para almacenar las imágenes existentes por contacto
    existing_images = {}
    for image in existing_images_results['records']:
        account_id = image['FirstPublishLocationId']
        if account_id not in existing_images:
            existing_images[account_id] = []
        existing_images[account_id].append(image['PathOnClient'])

    while True:
        try:
            results = drive_service.files().list(
                q=f"'{FOLDER_ID}' in parents and mimeType contains 'image'",
                fields="nextPageToken, files(id, name)",
                pageSize=1000,  
                pageToken=page_token
            ).execute()
            items = results.get('files', [])

            # Initialize update_records before the for loop
            update_records = []

            batch_size = 50
            for i in range(0, len(items), batch_size):
                batch = items[i:i+batch_size]
                for item in batch:  
                    file_id = item['id']
                    file_name = item['name']
                    if file_name.startswith("photo") and file_name.endswith(".jpg"):
                        try:
                            account_id = file_name[5:11].lstrip('0')
                            sf_account_id = account_id_map.get(f"Parishes_{account_id}")
                            if sf_account_id is not None:
                                update_record = upload_image_and_get_update_data(sf, {'file_id': file_id, 'file_name': file_name, 'sf_account_id': sf_account_id}, existing_images)
                                if update_record is not None:
                                    update_records.append(update_record)
                            else:
                                print(f"Error: No account found with Archdpdx_Migration_Id__c = {account_id}")
                        except ValueError:
                            print(f"Error: Invalid file name format for {file_name}. Skipping.")

            if update_records:
                for i in range(0, len(update_records), 100): 
                    sf.bulk.Account.update(update_records[i:i+100])
                updated_accounts.extend(update_records)

            page_token = results.get('nextPageToken', None)
            if page_token is None:
                break

        except HttpError as error:
            if error.resp.status == 429: 
                retry_after = int(error.resp.headers.get('Retry-After', 1))
                print(f"Rate limit exceeded. Retrying in {retry_after} seconds...")
                time.sleep(retry_after + 1) 
            else:
                raise error  

    # Write updated accounts to a CSV file
    with open('updated_accounts.csv', 'w', newline='') as csvfile:
        fieldnames = ['Id', 'Name', 'Archdpdx_Migration_Id__c']  
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for account in updated_accounts:
            sf_account_id = account['Id']
            account_details = sf.Account.get(sf_account_id)
            migration_id = next((k for k, v in account_id_map.items() if v == sf_account_id), None)
            writer.writerow({
                'Id': sf_account_id,
                'Name': account_details['Name'], 
                'Archdpdx_Migration_Id__c': migration_id
            })


if __name__ == '__main__':
    main()
