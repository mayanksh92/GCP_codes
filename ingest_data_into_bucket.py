from google.cloud import storage
from google.oauth2 import service_account

def create_bucket(bucket_name, credentials):
    """Creates a new bucket."""
    storage_client = storage.Client(credentials=credentials)
    bucket = storage_client.create_bucket(bucket_name)
    print(f"Bucket {bucket.name} created.")

def upload_file(bucket_name, source_file_name, destination_blob_name, credentials):
    """Uploads a file to the bucket."""
    storage_client = storage.Client(credentials=credentials)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)
    print(f"File {source_file_name} uploaded to {destination_blob_name}.")

if __name__ == "__main__":
    key_path = 'C:\\Users\\Akanksha\\Downloads\\erp_pipeline_key.json'
    credentials = service_account.Credentials.from_service_account_file(key_path)
    bucket_name = 'mayank13891'
    source_file_name = 'C:\\Users\\Akanksha\\Downloads\\ERP\\orders.csv'
    destination_blob_name = 'orders.csv'

    # create_bucket(bucket_name, credentials)
    upload_file(bucket_name, source_file_name, destination_blob_name, credentials)


