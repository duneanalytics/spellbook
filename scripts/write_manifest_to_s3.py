import json
from s3fs import S3FileSystem

f = open('../manifest.json')
json_object = json.load(f)
path_to_s3_object = 's3://manifest-spellbook/manifest.json'

s3 = S3FileSystem()
with s3.open(path_to_s3_object, 'w') as file:
    json.dump(json_object, file)