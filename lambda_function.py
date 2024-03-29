import urllib3
import json
import logging
import boto3
from datetime import datetime


def lambda_handler(event, context):

    api_url: str = "https://api.scryfall.com/bulk-data"
    bulk_uri : str
    target_keys : list = ["oracle_id","name","set","released_at","rarity","cmc","colors","type_line","power","toughness","legalities","prices"]

    http = urllib3.PoolManager()

    def flatten_json(json_data):
        """
        Input: Multileveled json supplied by Scryfall

        Flattens the legalities and prices of the card json for easier consumption, additionally converts the string array of colors into a simple comma delineated string.

        Returns: Flattened json for eventual db consumption
        """
        flattened_data = []
        for item in json_data:
            flattened_item = {
                "oracle_id": item["oracle_id"],
                "name": item["name"],
                "set": item["set"],
                "released_at": item["released_at"],
                "rarity": item["rarity"],
                "cmc": item["cmc"],
                "colors": ",".join(item.get("colors", [])),
                "type_line": item["type_line"],
                "standard_legal": item["legalities"]["standard"],
                "pioneer_legal": item["legalities"]["pioneer"],
                "modern_legal": item["legalities"]["modern"],
                "pauper_legal": item["legalities"]["pauper"],
                "vintage_legal": item["legalities"]["vintage"],
                "commander_legal": item["legalities"]["commander"],
                "power": item.get("power"),
                "toughness": item.get("toughness"),
                "prices_usd": item["prices"]["usd"]
            }
            flattened_data.append(flattened_item)
        return flattened_data

    # Scryfall supplies a URI to the actual bulk data, the following code retrieves that then queries for the json itself, and finally loads it.
    try:
        response = http.request("GET", api_url)
        response_json = json.loads(response.data)
        print("Successfully got latest bulk-data metadata")

    except Exception as e:
        print(e)
        
    for i in response_json["data"]:
            if i["type"] == "oracle_cards":
                bulk_uri = i["download_uri"]

    try:
        data = http.request("GET", bulk_uri).data
        loaded_json = json.loads(data)
        
        print("Successfully got latest bulk-data")

    except Exception as e:
        print(e)

    # Filters the json down to the actual keys we want for the final db, and then runs the flattening function.

    filtered_json = []
    for card in loaded_json:
        filtered_card = {}
        for key in target_keys:
            if key in card:
                filtered_card[key] = card[key]
        filtered_json.append(filtered_card)


    flattened_json = flatten_json(filtered_json)


    # Uploads to the s3 bucket.

    s3_client = boto3.client('s3')

    bucket_name = 'magic-the-data-gathering'
    key = 'landing/bulk_load.json'

    s3_client.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(flattened_json, indent=4).encode('utf-8')
    )

    print(f"Bulk data file uploaded to S3 bucket: {bucket_name}/{key}")



