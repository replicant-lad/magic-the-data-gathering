import urllib3
import json
import logging
import boto3
from datetime import datetime


def lambda_handler(event, context):
    '''
    Retrieves the latest bulk data dump from Scryfall and uploads it to s3
    '''

    api_url = "https://api.scryfall.com/bulk-data"
    http = urllib3.PoolManager()

    try:
        response = http.request("GET", api_url)
        response_json = json.loads(response.data)
        print("Successfully got latest bulk-data metadata")

    except Exception as e:
        print(e)
    
    for i in response_json["data"]:
            if i["type"] == "oracle_cards":
                 uri = i["download_uri"]
    
    try:
        download = http.request("GET", uri)
        print("got the download object")
        download_json = json.loads(download.data)
        print("Successfully got latest bulk-data load")
    except Exception as e:
         print(e)


    s3 = boto3.client('s3')
    print("Connected to aws")
    bucket ='bulk-data-landing'

    fileName = 'dump_' + datetime.now().strftime("%d-%m-%y-%H:%M:%S")

    print(f"Attempting to upload to {fileName}")
    uploadByteStream = bytes(json.dumps(download_json).encode('UTF-8'))

    s3.put_object(Bucket=bucket, Key=fileName, Body=uploadByteStream)

    print("Upload to s3 complete")