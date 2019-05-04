
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
   masterPorts=["9999"]
   ip = "tcp://localhost:"
   ports = ("1300","1400","1500")
   zmqContext = zmq.Context()

   def __init__(self, clientId):
      self.start()



# make 
   def start(self):
      print("NodeKeeper is Ready")
      print("waiting for Service")
      clientSocket = self.zmqContext.socket(zmq.REP)
      for p in self.ports:
           clientSocket.bind("tcp://*:%s" % p)
           
      dataSocket = self.zmqContext.socket(zmq.REP)
      for p in self.masterPorst:
          dataSocket.connect("tcp://*:%s" % p)
      while True:
          ID , FileName , operation , data   =clientSocket.recv_pyobj()
          ff = open(FileName, "wb")
          ff.write(data)
          dataSocket.send_pyobj((ID , self.ip , FileName))
       
       
    
    

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

