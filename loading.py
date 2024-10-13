import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv # type: ignore
import os


# Data loading
def run_loading():
  #loading the  dataset
  data = pd.read_csv('clean_data.csv')
  data = pd.read_csv('products.csv')
  data = pd.read_csv('staff.csv')
  data = pd.read_csv('customers.csv')
  data = pd.read_csv('transaction.csv')
  
  # load the environment variables from the .env file
  load_dotenv()
  
# loading date into azure blob storage
  connect_str = os.getenv('AZURE_CONNECTION_STRING_VALUE')
  container_name = os.getenv("CONTAINER_NAME")

  # Create a BlobServiceClient object
  blob_service_client = BlobServiceClient.from_connection_string(connect_str)
  container_client = blob_service_client.get_container_client(container_name)

  # Code to Load data to Azure blob storage
  files = [
      (data, 'rawdata/clean_data.csv'),
      (products, 'cleaneddata/products.csv'),
      (customers, 'cleaneddata/customers.csv'),
      (staff, 'cleaneddata/staff.csv'),
      (transaction, 'cleaneddata/transaction.csv')
  ]

  for file, blob_name in files:
      blob_client = container_client.get_blob_client(blob_name)
      output = file.to_csv(index=False)
      blob_client.upload_blob(output, overwrite=True)
      print(f'{blob_name} loaded into Azure Blob Storage')