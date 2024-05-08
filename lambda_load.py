import sys
import pymysql
import json
import os
import datetime
import boto3
import botocore

execution_time = datetime.datetime.now()

'''
Quick Note: This script requires loading pymysql to the Lambda instance to run, as well as building environment variables for the db connection
'''

# rds settings
user_name = os.environ['USER_NAME']
password = os.environ['PASSWORD']
rds_proxy_host = os.environ['RDS_PROXY_HOST']
db_name = os.environ['DB_NAME']   
    
try:
    conn = pymysql.connect(host=rds_proxy_host, user=user_name, passwd=password, db=db_name, connect_timeout=5)
    
except pymysql.MySQLError as e:
    print(e)
    sys.exit(1)
    
print("Success, connection to MySQL instance succeeded")

    
    # retrieving the s3 object
try:
    s3_client = boto3.resource('s3')
    s3_object = s3_client.Object('magic-the-data-gathering', 'bulk_load.json')
    s3_content = s3_object.get()['Body'].read().decode('utf-8')
    json_load = json.loads(s3_content)
except Exception as e:
    print(e)
    sys.exit(1)
print("s3 object retrieved")


def lambda_handler(event, context):


    for card in json_load:
        card_sql_text = f"""INSERT INTO card (oracle_id, name, card_set, released_at, update_timestamp) VALUES (%s, %s, %s, %s, %s) AS CardUpdate
                ON DUPLICATE KEY UPDATE
                oracle_id = CardUpdate.oracle_id, name = CardUpdate.name, card_set = CardUpdate.card_set, released_at = CardUpdate.released_at, update_timestamp = CardUpdate.update_timestamp;"""
    
        card_info_text = f"""INSERT INTO card_info (id, rarity, cmc, colors, type_line, power, toughness) VALUES (%s, %s, %s, %s, %s, %s, %s) AS CardUpdate
                ON DUPLICATE KEY UPDATE
                id = CardUpdate.id, rarity = CardUpdate.rarity, cmc = CardUpdate.cmc, colors = CardUpdate.colors, type_line = CardUpdate.type_line, power = CardUpdate.power, toughness = CardUpdate.toughness;"""
            
        card_price_text = f"""INSERT INTO card_price(id, most_recent) VALUES (%s, %s) AS CardUpdate
                ON DUPLICATE KEY UPDATE
                id = CardUpdate.id, most_recent = CardUpdate.most_recent;"""
    
        card_legality_text = f"""INSERT INTO legality (id,standard,pioneer,modern,pauper,vintage,commander) VALUES (%s, %s, %s, %s, %s, %s, %s) AS CardUpdate
                ON DUPLICATE KEY UPDATE
                id = CardUpdate.id, standard = CardUpdate.standard, pioneer = CardUpdate.pioneer, modern = CardUpdate.modern, pauper = CardUpdate.pauper, vintage = CardUpdate.vintage, commander = CardUpdate.commander;"""
                
        price_table_update_text = f"""UPDATE card_price SET last_day_6 = last_day_5, last_day_5 = last_day_4, last_day_4 = last_day_3, last_day_3 = last_day_2, last_day_2 = last_day_1, last_day_1 = most_recent WHERE id like (%s);"""        
        
        try:
            with conn.cursor() as cur:
                cur.execute(card_sql_text, (card['oracle_id'],card['name'],card['set'],card['released_at'],execution_time))
                cur.execute(card_info_text, (card['oracle_id'],card['rarity'],card['cmc'],card['colors'],card['type_line'],card['power'],card['toughness']))
                cur.execute(card_legality_text, (card['oracle_id'],card['standard_legal'],card['pioneer_legal'],card['modern_legal'],card['pauper_legal'],card['vintage_legal'],card['commander_legal']))
                cur.execute(price_table_update_text,(card['oracle_id'],))
                cur.execute(card_price_text,(card['oracle_id'],card['prices_usd']))
                conn.commit()
        except Exception as e:
            print(e)
            print("ERROR IN HANDLING QUERY, THE FOLLOWING CARD MAY HAVE ISSUE: \n")
            print(str(card) + "\n")
            sys.exit(1)

    return "SUCCESS - BULK DATA HAS BEEN MIGRATED TO MYSQL"
