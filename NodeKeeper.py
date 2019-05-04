
#import uu
#uu.encode('scream.mp4', 'video.txt')
#uu.decode('video.txt', 'video-copy.mp4')
import zmq
import sys
import random
import time
import threading
import os

class NodeKeeper:
   ip = "tcp://localhost:"
   ports = ("1300","1400","1500")
   files = ()
   zmqContext = zmq.Context()


   def __init__(self, clientId):
      self.start()

   def start(self):
      print("NodeKeeper is Ready")
      print("waiting for Service")
      self.clientSocket.bind("tcp://*:%s" % self.port)

f = open("scream.mp4", "rb")

FileData=[]
chunk = 1024
while True:
     data = f.read(chunk)
     if not data:
        break
     FileData.append(data)
ff = open("scream2.mp4", "wb")
for chunks in FileData:
    ff.write(chunks)
ff.close()     

