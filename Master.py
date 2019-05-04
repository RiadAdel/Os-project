import zmq
import sys
import random
import time
import threading
import pymongo


class Master:
    ip = "tcp://localhost:"
    ClientPort , DataPort , IAmAlivePort , Replication  = ("9998" , "9999" , "9997" , "9990")
    nodesIps_Ports = [("tcp://localhost:" ,"1300","1400","1500") , ("tcp://localhost:" ,"1300","1400","1500")]
    nodesIps_Ports_conditinos = [("tcp://localhost:","","","") , ("tcp://localhost:","","","") ]
    zmqContext = zmq.Context()
   

    def __init__(self, ip = None , ClientPort = None ,DataPort=None  , IAmAlivePort=None , Replication = None ):
        if ip != None:
              self.ip = ip
        if ClientPort != None:
            self.ClientPort = ClientPort
        if DataPort  !=None:
            self.DataPort = DataPort
        if IAmAlivePort !=None:
            self.IAmAlivePort = IAmAlivePort 
        if Replication !=None:
            self.Replication = Replication
        self.start()
    
    def start(self):
        myclient = pymongo.MongoClient("mongodb://localhost:27017/")
        mydb = myclient["mydatabase"]
        mycol = mydb["LookUpTable"]
        print("Server is Ready")
        print("waiting for client")
        t = threading.Thread(target = self.ClientHandle, args = ())
        t.start()
        t1 = threading.Thread(target = self.DataHandle, args = (mycol))
        t1.start()
        t2 = threading.Thread(target = self.AliveHandle, args = ())
        t2.start()
        
    
                 
    def ClientHandle (self):
        clientSocket = self.zmqContext.socket(zmq.REP)
        clientSocket.bind("tcp://*:%s" % self.ClientPort)
        while (True):
            query = clientSocket.recv_pyobj()
            print("client with ID " + str(query[0]) + "want to" + str(query[1]))
            if query[1] == "Download":
                     x =" "
            elif query[1] == "Upload":
                index = random.randint(0, len(self.nodesIps_Ports)-1)
                l = self.nodesIps_Ports[index]
                portsTosend=[]
                for idx, val in enumerate(l):
                    if (self.nodesIps_Ports_conditinos[index][idx] == ""  or idx==0 ):
                         portsTosend.append(self.nodesIps_Ports_conditinos[index][idx])
              
                print(portsTosend)
                T = tuple(portsTosend)
                clientSocket.send_pyobj(T)
        
        
        
        
    def DataHandle (self , mycol):
        DataSocket = self.zmqContext.socket(zmq.REP)
        DataSocket.bind("tcp://*:%s" % self.DataPort)
        while (True):
            ID , Ip , FileName = DataSocket.recv_pyobj()
            if (FileName !=""):
                mydict = {"ID": ID, "IP": Ip, "FileName": FileName , "Alive":"True"}
                x = mycol.insert_one(mydict)
                # then make it free
            else:
                # make it free




    def AliveHandle (self):
           x =" "
           
           
           
    def ReplicationHandle (self):
        x=" "
                

print("server starts")
m = Master()