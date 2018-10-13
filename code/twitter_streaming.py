#!/usr/bin/python

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from datetime import datetime
import os
from shutil import move
import json
from auth import TwitterAuth
import time
import sys
import gzip

#This script will open a stream every 20 minutes
#collect tweets identified by keyword and save them to a gzip file in a given directory
#every 20 minutes, stream will close and the file will be saved to external HD 

outputDir = "../data"

class FileDumperListener(StreamListener):
    def __init__(self,filepath):
          super(FileDumperListener,self).__init__(self)
          self.basePath=filepath
          os.system("mkdir -p %s"%(filepath))

          d=datetime.today()
          c = ((d.hour * 60) + d.minute) / 20 #what file we are on for the day
          
          self.filename = "%i-%02d-%02d-%02d.json.gzip"%(d.year,d.month,d.day,c)
          self.fh = gzip.open(self.basePath + "/" + self.filename,"a")
          self.tweetCount=0
          self.errorCount=0
          self.limitCount=0
          self.last=datetime.now()
     
     #This function gets called every time a new tweet is received on the stream
    def on_data(self, data):
          datajson = json.loads(data)

          print >> self.fh, json.dumps(datajson)
          self.tweetCount+=1
          
          #Check if we should end the stream
          self.status()
          return True
          
    def close(self):
          try:
               self.fh.close()
          except:
               pass
     
    def on_error(self, statusCode):
          print("%s - ERROR with status code %s"%(datetime.now(),statusCode))
          self.errorCount+=1
     
    def on_timeout(self):
          raise TimeoutException()
     
    def on_limit(self, track):
          print("%s - LIMIT message recieved %s"%(datetime.now(),track))
          self.limitCount+=1

    def status(self):
          now=datetime.now()
          #If its been 20 minutes, print status and end stream
          #copy file to external drive
          if (now-self.last).total_seconds()>1200:
               #print status
               print("%s - %i tweets, %i limits, %i errors in previous twenty minutes."%(now,self.tweetCount,self.limitCount,self.errorCount))
               print("Closing stream and moving file to external HD")
               #close stream
               listener.close()
               stream.disconnect()

               #stream will now restart

class TimeoutException(Exception):
     pass

if __name__ == '__main__':
     now=datetime.now()
     while True:
          try:
               #Create the listener
               listener = FileDumperListener(outputDir)
               auth = OAuthHandler(TwitterAuth.API_key, TwitterAuth.API_secret)
               auth.set_access_token(TwitterAuth.access_token, TwitterAuth.access_token_secret)

               terms = ['Manchin', 'Murkowski', 'Collins','Heitkamp','Cruz','Hyde-Smith', 'Flake']

               print("%s - Starting stream to track %s"%(datetime.now(),",".join(terms)))
               print("hit ctrl+c to interrupt")

               #Connect to the Twitter stream
               stream = Stream(auth, listener)
               stream.filter(track=terms)

          except KeyboardInterrupt:
               #User pressed ctrl+c or cmd+c -- get ready to exit the program
               print("%s - KeyboardInterrupt caught. Closing stream and exiting."%datetime.now())
               listener.close()
               stream.disconnect()
               break
          except TimeoutException:
               #Timeout error, network problems? reconnect.
               print("%s - Timeout exception caught. Closing stream and reopening."%datetime.now())
               try:
                    listener.close()
                    stream.disconnect()
               except:
                    pass
               continue
          except Exception as e:
               #Anything else
               try:
                    info = str(e)
                    sys.stderr.write("%s - Unexpected exception. %s\n"%(datetime.now(),info))
               except:
                    pass
