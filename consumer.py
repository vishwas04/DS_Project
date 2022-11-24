import os
import json
from json import loads
from kafka import KafkaConsumer
HASHTAGS = ["Karnataka","Kannada","KGF"]
consumer = KafkaConsumer(*HASHTAGS)

loc = dict()
for message in consumer:
    data = message.value
    data = json.loads(data.decode('utf-8'))
    if data["hashtag"] not in loc:
        loc[data["hashtag"]] = {
            "start": data["start_time"],
            "end": data["end_time"],
            "count": data["count"]
        }
    else:
        loc[data["hashtag"]]["count"] += data["count"]
    for i in HASHTAGS:
         if(i in loc):
                 print(i,loc[i]["count"])

