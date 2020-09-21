import os, uuid, re
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

connect_str = ""

#param of how old data we want to delete
date_from_delete = datetime.today() + timedelta(days = -180)

print(date_from_delete)

#looping thru folders and deleting the ones that are older than date_from_delete
try:

    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    container_client = ContainerClient.from_connection_string(
        connect_str, container_name="datalake")

    my_blobs = container_client.list_blobs()
    for b in my_blobs:
        if b.name.startswith('data/date='):
            if datetime.strptime(b.name[10:20], '%Y-%m-%d').date() <= date_from_delete.date():
                container_client.delete_blob(b.name)


except Exception as ex:
    print('Exception:')
    print(ex)


