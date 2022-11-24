import os
import json
import time
import logging
import datetime
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import window
from kafka import KafkaProducer
from pyspark.streaming import StreamingContext


def create_topics(hashtags):
    for i in hashtags:
        os.system(
            'kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic {}'.format(i))
        #os.system('kafka-configs.sh --zookeeper localhost:2181 --entity-type topics --alter --entity-name {} --add-config retention.ms=60000'.format(i))


def get_hashtag_dataframes(df):
    dfs = {hashtag: df.filter(df.tweet.contains(hashtag))
           for hashtag in HASHTAGS}
    return dfs


def get_tumbling_window_dataframes(hashtag_dfs):
    windows = dict()
    for hashtag, hashtag_df in hashtag_dfs.items():
        w = hashtag_df.groupBy(
            window(timeColumn="time", windowDuration="30 seconds")).count().alias("count")
        windows[hashtag] = w
    return windows


def process_stream(tweets):
    print(tweets.collect())
    print(type(tweets),"----------------------------------------------->")
    tweets = tweets.collect()
    print(type(tweets),"----------------------------------------------->")
    
    if not tweets:
        return
    for i in tweets:
       print(i)
    tweets = [json.loads(tweet) for tweet in tweets]
    print(tweets,"==",len(tweets),"----------------------------------------------->")
    
    result = list()
    for tweet in tweets:
        tweet_time = datetime.datetime.strptime(
            tweet["time"], '%Y-%m-%dT%H:%M:%S.%fZ')
        result.append((tweet_time, tweet["tweet"]))

    df = spark_sql_context.createDataFrame(
        result, ["time", "tweet"]).toDF("time", "tweet")

    hashtag_dfs = get_hashtag_dataframes(df)

    tumbling_window_dfs = get_tumbling_window_dataframes(hashtag_dfs)
    print("\n\n\n")
    spark_sql_context.createDataFrame(result, ["time", "tweet"]).show(n=len(result))
    print("\n\n\n")
    #create_topics(["Karnataka","Kannada","KGF"])
    publish(tumbling_window_dfs)

    
def publish(latest_count):
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda m: json.dumps(m).encode('utf-8'))
    for hashtag, df in latest_count.items():
        num = 0
        start = ""
        end = ""

        if len(df.collect()) == 0:
            num = 0
        else:
            num = df.collect()[0][1]
            start = df.collect()[0][0]["start"].strftime("%Y-%m-%d %H:%M:%S")
            end = df.collect()[0][0]["end"].strftime("%Y-%m-%d %H:%M:%S")
        data = {
            "hashtag": hashtag,
            "count": num,
            "start_time": start,
            "end_time": end
        }
        print("KafkaProducer---=====================-=-=-=-=-=- >>>>>>>>>>>>>>>>>>>>>>>>",hashtag,data)
        
        producer.send(hashtag, data)


if __name__ == "__main__":
    HASHTAGS = ["Karnataka","Kannada","KGF"]
    STREAM_HOSTNAME = 'localhost' #os.getenv("STREAM_HOSTNAME")
    STREAM_PORT = 3001 #int(os.getenv("STREAM_PORT"))

    spark_context = SparkContext.getOrCreate()
    spark_streaming_context = StreamingContext(spark_context, 5)
    spark_sql_context = SQLContext(spark_context)

    spark_streaming_context.socketTextStream(STREAM_HOSTNAME, STREAM_PORT).foreachRDD(process_stream)

    spark_streaming_context.start()
    spark_streaming_context.awaitTermination(1000)
    spark_streaming_context.stop()
