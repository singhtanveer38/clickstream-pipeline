from faker import Faker
import random
from datetime import datetime
from kafka import KafkaProducer
import json
import time

fake = Faker()

producer = KafkaProducer(bootstrap_servers="localhost:9092",
        value_serializer=lambda x:json.dumps(x).encode('utf-8'))

topic = "yo-events"
# generate fake sample URLs
samplePages = []
for i in range(10):
    samplePages.append(fake.domain_name())

# country
countryList = []
for i in range(10):
    countryList.append(fake.country())
# browser list
browserList = ["chrome", "firefox", "safari", "brave"]

# os
osList = ["linux", "windows", "osx"]

# device
deviceList = ["mobile", "pc"]
# creating fake clickstream data
eventID = 0
#while True:
for i in range(1000):
    eventID += 1
    userID = random.randint(1,100)
    dt = datetime.strftime(datetime.now(), "%d/%m/%y %H:%M:%S")
    url = random.choice(samplePages)[:30]
    country = random.choice(countryList)[:30]
    city = fake.city()[:30]
    browser = random.choice(browserList)
    os = random.choice(osList)
    device = random.choice(deviceList)
    #message = {eventID}, {userID}, {dt}, {url}, {country}, {city}, {browser}, {os}, {device}
    message = {
                "row_key":
                    {
                        "eventID": f"{eventID}"
                    },
                "column_family":
                    {
                        "click_data":
                            {
                                "userID": f"{userID}",
                                "timestamp": f"{dt}",
                                "url": f"{url}"
                            },
                        "geo_data":
                            {
                                "country": f"{country}",
                                "city": f"{city}"
                            },
                        "user_agent_data":
                            {
                                "browser": f"{browser}",
                                "os": f"{os}",
                                "device": f"{device}"
                            }

                    }
            }
    
    producer.send(topic, value=message)

    #time.sleep(1)   

