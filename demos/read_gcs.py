from google.cloud import storage
# create storage client
storage_client = storage.Client.from_service_account_json('/Users/dickye/gcp.json')
# get bucket with name
bucket = storage_client.get_bucket('dy-awesome-bucket')
# get bucket data as blob
blob = bucket.get_blob('kinglear.txt')
# convert to string
json_data = blob.download_as_string()

print(json_data)
