
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
   topic="1"
   IamAlivePort="9899"
   ReplicationPort= "9990"
   ip = "tcp://localhost:"
   MyPorts = ["23041","24041","25041"]
   zmqContext = zmq.Context()
   

   def __init__(self ,Topic = None , port1=None , port2=None , port3=None ,Iamalive=None,ReplicationPort=None, IP=None):
        if IP != None:
            self.ip = IP
        if port1 != None:
            self.MyPorts[0] = port1
        if port2  !=None:
            self.MyPorts[1] = port2
        if port3 !=None:
            self.MyPorts[2] = port3 
        if Topic != None:
            self.topic = Topic
        if Iamalive != None:
            self.IamAlivePort = Iamalive
        if ReplicationPort != None:
            self.ReplicationPort = ReplicationPort
            
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
      t3 = threading.Thread(target = self.IamAlive, args = ())
      t3.start()
      t4 = threading.Thread(target = self.Replication, args = ())
      t4.start()
      
      t.join()
      t1.join()
      t2.join()
      t3.join()
      t4.join()
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
              ff.close()
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
        AliveSocket = self.zmqContext.socket(zmq.PUB)
        AliveSocket.bind("tcp://*:%s" % self.IamAlivePort)
        while True:
          AliveSocket.send_string(self.topic+" "+self.ip+" "+ self.MyPorts[0]+" "+self.MyPorts[1]+" "+self.MyPorts[2])
          time.sleep(1)
   
   def Replication(self):
       ReplicationSocket = self.zmqContext.socket(zmq.REP)
       ReplicationSocket.bind("tcp://*:%s" % self.IamAlivePort)
       FileName , Data , ID , IpList , PortList =ReplicationSocket.recv_pyobj()
       #master send me to replicate to iplist and portlist
       if Data=="":
            ff = open(ID+FileName, "rb")
            ActualData = ff.read()
            ff.close()
            for indx , p in enumerate(IpList):
               DataSocket=self.zmqContext.socket(zmq.REQ)
               DataSocket.connect (p+PortList[indx])
               DataSocket.send_pyobj((FileName ,ActualData ,ID, [] , [] ))
               DataSocket.recv_string()
           
            ReplicationSocket.send_string()
           # data was sent from other node to be replicated
       else:
           ff = open(ID+FileName, "wb")
           ff.write(Data)
           ff.close()
           ReplicationSocket.send_string()
           
           
           
           
           
           
       
       
                 
       

if (len(sys.argv)==7):
    c = NodeKeeper(sys.argv[1] ,sys.argv[2] ,sys.argv[3] ,sys.argv[4],sys.argv[5],sys.argv[6])
elif (len(sys.argv)==8):
    c = NodeKeeper(sys.argv[1] ,sys.argv[2] ,sys.argv[3] ,sys.argv[4] , sys.argv[5]  , sys.argv[6], sys.argv[7] )
else:
    c = NodeKeeper()
