
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
  #MasterPorts=["9999","9989","9979"]
   MasterPorts=["9999"]
  #IamAlivePorts=["9899", "9799","9699"]
   IamAlivePorts=["9899"]
   ip = "tcp://localhost:"
   MyPorts = ["1300","1400","1500"]
   zmqContext = zmq.Context()
   

   def __init__(self , port1=None , port2=None , port3=None , IP=None):
        if IP != None:
              self.ip = IP
        if port1 != None:
            self.MyPorts[0] = port1
        if port2  !=None:
            self.MyPorts[1] = port2
        if port3 !=None:
            self.MyPorts[2] = port3 
        self.start()


   def start(self):
      print(self.MyPorts)
      print("NodeKeeper is Ready")
      print("waiting for Service")
      t = threading.Thread(target = self.NodeAction, args = (self.MyPorts[0],) )
      t.start()
      t1 = threading.Thread(target = self.NodeAction, args = (self.MyPorts[1],) )
      t1.start()
      t2 = threading.Thread(target = self.NodeAction, args = (self.MyPorts[2],) )
      t2.start()
      #t3 = threading.Thread(target = self.IamAlive, args = ())
      #t3.start()
      t.join()
      t1.join()
      t2.join()
      #t3.join()

   def NodeAction(self,port):
        ClientSocket = self.zmqContext.socket(zmq.REP)
        ClientSocket.bind("tcp://*:%s" % port)
        MasterSocket = self.zmqContext.socket(zmq.REQ)
        for p in self.MasterPorts:
          MasterSocket.connect("tcp://localhost:%s" % p)
        while True:
          ID , FileName , operation, index , DataOrSize =ClientSocket.recv_pyobj()
          print("operation is " + operation)
          if (operation =="Upload"):
              ff = open(ID+FileName, "wb")
              ff.write(DataOrSize)
              ff.close()
              print("operation is Upload done")
              MasterSocket.send_pyobj((ID , self.ip , FileName))
              MasterSocket.recv_string()
              ClientSocket.send_string("Done")
          elif(operation == "Download"):
              ff = open(ID+FileName, "rb")
              Data = ff.read()
              Length = len(Data)
              print(Length)
              FirstPart =int ( ( Length * (index-1) ) /DataOrSize )
              SecondPart = int ( (( Length * index ) /DataOrSize ) )
              DataToSend = Data[FirstPart:SecondPart]
              print(len(DataToSend))
              ClientSocket.send_pyobj(DataToSend)
              MasterSocket.send_pyobj((ID , self.ip , ""))
              MasterSocket.recv_string()
       
   def IamAlive(self):
        AliveSocket = self.zmqContext.socket(zmq.REQ)
        for p in self.IamAlivePorts:
          AliveSocket.connect("tcp://localhwost:%s" % p)
        while True:
          AliveSocket.send_pyobj((self.ip , self.MyPorts[0] , self.MyPorts[1] , self.MyPorts[2] ))
          AliveSocket.recv_string()
          time.sleep(1)
          
            
       

if (len(sys.argv)==4):
    c = NodeKeeper(sys.argv[1] ,sys.argv[2] ,sys.argv[3] ,sys.argv[4])
elif (len(sys.argv)==5):
    c = NodeKeeper(sys.argv[1] ,sys.argv[2] ,sys.argv[3] ,sys.argv[4] , sys.argv[5] )
else:
    c = NodeKeeper()
