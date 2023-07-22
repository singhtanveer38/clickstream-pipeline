import psycopg2
from kafka import KafkaConsumer
from datetime import datetime
import json
import time

topic = "yo-events"

consumer = KafkaConsumer(
    topic,
     bootstrap_servers='localhost:9092',
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     value_deserializer=lambda x: json.loads(x.decode('utf-8')))

conn = psycopg2.connect( database="clickstream", user='postgres', password='password', host='127.0.0.1', port= '5432')
cursor = conn.cursor()


for message in consumer:
    message = message.value

    cursor.execute("insert into clicks (eventID, userID, dt, url, country, city, browser, os, device) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)", (message["row_key"]["eventID"], message["column_family"]["click_data"]["userID"], datetime.strptime(message["column_family"]["click_data"]["timestamp"], "%d/%m/%y %H:%M:%S"), message["column_family"]["click_data"]["url"], message["column_family"]["geo_data"]["country"], message["column_family"]["geo_data"]["city"], message["column_family"]["user_agent_data"]["browser"], message["column_family"]["user_agent_data"]["os"], message["column_family"]["user_agent_data"]["device"]))

    conn.commit()

    print('Data at {} added to POSTGRESQL'.format(message))
        #time.sleep(1)
    
    
conn.close()
