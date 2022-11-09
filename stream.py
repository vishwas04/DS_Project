import os
import json
import socket
import tweepy
import logging
from tweepy import Stream ,StreamingClient
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - - %(filename)s - %(funcName)s - %(lineno)d -  %(message)s',
    level=logging.NOTSET,
    filename='stream.log',
    filemode='w'
)


class TweetsListener(StreamingClient):
    # def on_data(self, data):
    #     try:
    #         tweet_content = json.loads(data)["text"]
    #         tweet_time = json.loads(data)["created_at"]
    #         data = {
    #             "tweet": tweet_content,
    #             "time": tweet_time
    #         }
    #         logging.debug(data)
    #         client_socket.send((json.dumps(data) + "\n").encode())
    #         return True

    #     except BaseException as e:
    #         logging.error(f"Error parsing data: {e}")
    #         return True
    def on_tweet(self, tweet):
        if tweet.referenced_tweets == None:
            # self.new_tweet[“tweet”] = tweet.text
            
            data = {"tweet": tweet.data['text'],"time": tweet.data['created_at']}
            logging.debug(data)
            print(tweet.data,type(tweet.data),(json.dumps(data) + "\n").encode())
            client_socket.send((json.dumps(data) + "\n").encode())
            return True

    def on_error(self, status):
        logging.error(status)


class TwitterStreamer():
    def __init__(self, client_socket=None):
        self.CONSUMER_KEY =   'HIbqfxpZ8DZ4hHhNxEkVN0Dhk' #os.environ.get('CONSUMER_KEY')
        self.CONSUMER_SECRET = 'Ruc3a2cnMuU6e1QZVj505xLuCtCicR0fExvMjOm9Gk2SwqFMWs'  #os.environ.get('CONSUMER_SECRET')
        self.ACCESS_TOKEN = '1585148546025590784-JTCg3v9gpvFGB20sbtKGGgnAAk4pT9' #os.environ.get('ACCESS_TOKEN')
        self.ACCESS_SECRET = 'BPcw66JoHAF2yRpORUePMELhzYUwijOFS5kTwGTJbkyuU' #os.environ.get('ACCESS_SECRET')
        self.BEARER_TOKEN = 'AAAAAAAAAAAAAAAAAAAAACVdigEAAAAAWYxo9Lcfw4zFmJ6DXnWHdOMgC8w%3DS8JAvuB3Ld2TDknUJzU3mJ7QvwO3x7TqghJdH62uYLQe3sS6Vk'
        self.client_socket = client_socket
        self.auth = tweepy.OAuthHandler(self.CONSUMER_KEY, self.CONSUMER_SECRET)
        self.auth.set_access_token(self.ACCESS_TOKEN, self.ACCESS_SECRET)
        self.api = tweepy.API(self.auth)

    def stream_tweets(self,hashtags):
        stream = TweetsListener(bearer_token=self.BEARER_TOKEN)
        if(stream.get_rules().data != None) :
            for i in stream.get_rules().data:
                stream.delete_rules(i.id)
        #prev_id = stream.get_rules().data[0].id
        print("\n\n\n\n",stream.get_rules(),"\n\n\n\n")
        #stream.delete_rules(prev_id)
        # add new query
        stream.add_rules([tweepy.StreamRule("Karnataka") , tweepy.StreamRule("Kannada") , tweepy.StreamRule("KGF")])
        stream.filter(tweet_fields=["created_at", "lang"],expansions=["author_id"],user_fields=["username", "name"],)


if __name__ == "__main__":
    HASHTAGS = ["Karnataka","Kannada","KGF"]
    STREAM_HOSTNAME = "localhost"
    STREAM_PORT = 3001
    STREAM_SOCKET = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    STREAM_SOCKET.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    STREAM_SOCKET.bind((STREAM_HOSTNAME, STREAM_PORT))
    STREAM_SOCKET.listen(1)
    logging.info(f"Listening for connections on {STREAM_HOSTNAME}:{STREAM_PORT}")
    print(f'Listening for connections on {STREAM_HOSTNAME}:{STREAM_PORT}')

    client_socket, client_address = STREAM_SOCKET.accept()
    logging.info(f"Connection from {client_address} has been established!")
    print(f"Connection from {client_address} has been established!")
    twitter_streamer = TwitterStreamer(client_socket)
    twitter_streamer.stream_tweets(HASHTAGS)
    # stream = TweetsListener('AAAAAAAAAAAAAAAAAAAAACVdigEAAAAAWYxo9Lcfw4zFmJ6DXnWHdOMgC8w%3DS8JAvuB3Ld2TDknUJzU3mJ7QvwO3x7TqghJdH62uYLQe3sS6Vk')
    # stream.filter(track=HASHTAGS, threaded=True)

