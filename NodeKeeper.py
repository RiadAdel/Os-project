
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
   

   def __init__(self):
      self.start()



# make 
   def start(self):
      print("NodeKeeper is Ready")
      print("waiting for Service")
      clientSocket = self.zmqContext.socket(zmq.REP)
      for p in self.ports:
           clientSocket.bind("tcp://*:%s" % p)
           
      dataSocket = self.zmqContext.socket(zmq.REQ)
      for p in self.masterPorts:
          dataSocket.connect("tcp://localhost:%s" % p)
      while True:
          ID , FileName , operation , data =clientSocket.recv_pyobj()
          print("first recieve finished")
          clientSocket.send_string("A")
          ff = open("shit.mp4", "wb")
          ff.write(data)
          ff.close()
          dataSocket.send_pyobj((ID , self.ip , FileName))
          dataSocket.recv_string()
       
    
    


c = NodeKeeper()
