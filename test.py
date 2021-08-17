from google.cloud import storage
import os
import sys
name = sys.argv[0]
path = sys.argv[1]
project_id = sys.argv[2]
import time
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=""
blob_list = []
bucket_name = path
storage_client = storage.Client()
bucket = storage_client.get_bucket(bucket_name)

def list_blobs_with_prefix(prefix, delimiter=None):
    """Lists all the blobs in the bucket that begin with the prefix."""
    blobs = bucket.list_blobs(prefix=prefix, delimiter=delimiter)
    print('Blobs:')
    for blob in blobs:
        print(blob.name)
list_blobs_with_prefix('worker_new/')
print(project_id)

