import zmq
import sys
import random
import time
import threading
import pymongo


class Master:
    Topics=["1" , "2" , "3"]
    IamAlivePorts=["9899", "9799","9699"]
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["mydatabase"]
    LookUpTable = mydb["LookUpTable"]
    ip = "tcp://localhost:"
    ClientPort , DataPort , Replication  = ("9998" , "9999"  , "9990")
    nodesIps_Ports = [["tcp://localhost:" ,"2301","2401","2501"]]
    nodesIps_Ports_conditinos = [["tcp://localhost:" ,"","",""]]
    zmqContext = zmq.Context()


    def __init__(self  , ClientPort=None  ,DataPort=None , IAmAlivePort=None , Replication=None , ip = None ):
        
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
       
        print("Server is Ready")
        print("waiting for client")
        t = threading.Thread(target = self.ClientHandle, args = ())
        t.start()
        t1 = threading.Thread(target = self.DataHandle, args = ())
        t1.start()
        #t2 = threading.Thread(target = self.AliveHandle, args = ())
        #t2.start()
        
        t1.join()
        #t2.join()
        t.join()
        
    
                 
    def ClientHandle (self):
        clientSocket = self.zmqContext.socket(zmq.REP)
        clientSocket.bind("tcp://*:%s" % self.ClientPort)
        print("binded to " + self.ClientPort)
        while (True):
            ID , operation , FileName = clientSocket.recv_pyobj()
            print("client with ID " + str(ID) + "want to" + str(operation))
            if operation == "Download":
                
                myquery = { "FileName": ID+FileName ,"Alive":"True"}
                ListofDataNodes =list( self.LookUpTable.find(myquery))
                ListofIps = []
                for node in ListofDataNodes:
                    ListofIps.append(node["IP"])
                ListToSend=[]
                count = 0
                for p in ListofIps:
                    for IpIndx , node in enumerate(self.nodesIps_Ports):
                        if(node[0]==p):
                            for portIndx in range(1,4):
                                if(self.nodesIps_Ports_conditinos[IpIndx][portIndx]==""):
                                     ListToSend.append(p)
                                     ListToSend.append(node[portIndx])
                                     self.nodesIps_Ports_conditinos[IpIndx][portIndx]=ID
                                     count+=1
                                if(count==6): break
                            break
                                    
                        if(count==6): break            
                            
                    if(count==6): break
                if(len(ListofIps)==0):ListToSend.append("NotFound")    
                if(len(ListToSend)>1 ): 
                    print("made ports busy" ) 
                    print(ListToSend) 
                    print(self.nodesIps_Ports_conditinos)
                TupleToSend=tuple(ListToSend)  
                clientSocket.send_pyobj(TupleToSend)
                     
            #handle if the chosin ip is busy   
            elif operation == "Upload":
                
                index = random.randint(0, len(self.nodesIps_Ports)-1)
                l = self.nodesIps_Ports[index]
                portsTosend=[]
                for idx, val in enumerate(l):
                    if (self.nodesIps_Ports_conditinos[index][idx] == ""  or idx==0 ):
                         portsTosend.append(self.nodesIps_Ports[index][idx])
                index2 = random.randint(1, len(portsTosend)-1)
                portsToSendFinal=[]
                portsToSendFinal.append(portsTosend[0])
                portsToSendFinal.append(portsTosend[index2])
                T = tuple(portsToSendFinal)
                print(T)
                print("changed port to busy")
                self.nodesIps_Ports_conditinos[index][index2]=ID
                print(self.nodesIps_Ports_conditinos)
                clientSocket.send_pyobj(T)
        
    def DataHandle (self ):
        DataSocket = self.zmqContext.socket(zmq.REP)
        DataSocket.bind("tcp://*:%s" % self.DataPort)
        print("binded to " + self.DataPort)
        while (True):
            ID , Ip , FileName = DataSocket.recv_pyobj()
            #upload
            if (FileName !=""):
                mydict = {"ID": ID, "IP": Ip, "FileName": ID+FileName , "Alive":"True"}
                x = self.LookUpTable.insert_one(mydict)
                print("updated the data base")
                for IpIndx , node in enumerate(self.nodesIps_Ports):
                    if(Ip==node[0]):
                        for portIndx in range(1,4):
                             if(self.nodesIps_Ports_conditinos[IpIndx][portIndx]==ID):
                                 self.nodesIps_Ports_conditinos[IpIndx][portIndx]=""
                                 break
                        break
                print("freeing port in upload")
                print(self.nodesIps_Ports_conditinos)
                DataSocket.send_string("Done")
            #download
            else:
                for IpIndx , node in enumerate(self.nodesIps_Ports):
                    if(Ip==node[0]):
                        for portIndx in range(1,4):
                             if(self.nodesIps_Ports_conditinos[IpIndx][portIndx]==ID):
                                 self.nodesIps_Ports_conditinos[IpIndx][portIndx]=""
                                 
                        break
                print("freeing ports used in download")
                print(self.nodesIps_Ports_conditinos)
                DataSocket.send_string("Done")
                

    def AliveHandle (self):
        for t in self.Topics:
            t = threading.Thread(target = self.AliveHandleForTopic, args = (t,))
            t.start()
        t.join()
            
           
    def AliveHandleForTopic(self , topic):
        poller = zmq.Poller()
        
        AliveSocket = self.zmqContext.socket(zmq.SUB)
        for p in self.IamAlivePorts:
            AliveSocket.connect("tcp://*:%s" % p)
        poller.register(AliveSocket , zmq.POLLIN)
        L=AliveSocket.recv_string()
        nodesIps_Ports.append()
        nodesIps_Ports_conditinos()
        while True:
            if(poller.poll(1000))
                AliveSocket.recv_string()
            else:
        
        
           
    def ReplicationHandle (self):
        x=" "
                
print("master starts")
if (len(sys.argv)==4):
    m = Master(sys.argv[1] ,sys.argv[2] ,sys.argv[3] ,sys.argv[4])
elif (len(sys.argv)-1 ==5):
    m = Master(sys.argv[1] ,sys.argv[2] ,sys.argv[3] ,sys.argv[4] , sys.argv[5])
else:
    m = Master()
   