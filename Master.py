import zmq
import sys
import random
import time
import threading
import pymongo


class Master:
    Topics=["1" , "2" , "3"]
    IamAlivePorts=["9899", "9799","9699"]
    ReplicationPorts=["9990" , "9991" , "9992"]
    DataNodesIp=["tcp://localhost:" , "tcp://localhost:" , "tcp://localhost:"]
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["mydatabase"]
    LookUpTable = mydb["LookUpTable"]
    ip = "tcp://localhost:"
    ClientPort , DataPort   = ("9998" , "9999")
    nodesIps_Ports = []
    nodesIps_Ports_conditinos = []
    zmqContext = zmq.Context()


    def __init__(self  , ClientPort=None  ,DataPort=None , ip = None ):
        
        if ip != None:
              self.ip = ip
        if ClientPort != None:
            self.ClientPort = ClientPort
        if DataPort  !=None:
            self.DataPort = DataPort
        self.start()
    
    
    
    def start(self):
       
        print("Server is Ready")
        print("waiting for client")
        t = threading.Thread(target = self.ClientHandle, args = ())
        t.start()
        t1 = threading.Thread(target = self.DataHandle, args = ())
        t1.start()
        t2 = threading.Thread(target = self.AliveHandle, args = ())
        t2.start()
        t3 = threading.Thread(target = self.ReplicationHandle, args = ())
        t3.start()
        t1.join()
        t2.join()
        t.join()
        t3.join()
    
                 
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
        for index ,p in enumerate(self.IamAlivePorts):
            AliveSocket.connect(self.DataNodesIp[index]+ p)
        AliveSocket.setsockopt_string(zmq.SUBSCRIBE,topic)
        poller.register(AliveSocket , zmq.POLLIN)
        L=AliveSocket.recv_string()
        topic, ip , port1 , port2 , port3 = L.split()
        PortInfo=[ip , port1 , port2 , port3]
        self.nodesIps_Ports.append(PortInfo)
        self.nodesIps_Ports_conditinos.append([ip , "" , "" , ""])
        print("node is alive")
        print(self.nodesIps_Ports  , self.nodesIps_Ports_conditinos)
        DontAcessDB = False
        while True:
            if(poller.poll(1500)):
                if(DontAcessDB==True):
                    myquery = { "IP": ip }
                    newvalues = { "$set": { "Alive": "True" } }
                    x = self.LookUpTable.update_many(myquery, newvalues)
                AliveSocket.recv_string()
                if (PortInfo not in self.nodesIps_Ports):
                    print("node is alive again")
                    self.nodesIps_Ports.append(PortInfo)
                    self.nodesIps_Ports_conditinos.append([ip , "" , "" , ""])
                    print(self.nodesIps_Ports ,self.nodesIps_Ports_conditinos )
                
                DontAcessDB=False
            else:
                if(DontAcessDB==False):
                    myquery = { "IP": ip }
                    newvalues = { "$set": { "Alive": "False" } }
                    x = self.LookUpTable.update_many(myquery, newvalues)
                if(PortInfo  in self.nodesIps_Ports ):
                    DontAcessDB = True
                    index = self.nodesIps_Ports.index(PortInfo)
                    del self.nodesIps_Ports[index]
                    del self.nodesIps_Ports_conditinos[index]
                print("node is down")
                print(self.nodesIps_Ports ,self.nodesIps_Ports_conditinos )
        
           
    def ReplicationHandle (self):
        ReplicationSocket = self.zmqContext.socket(zmq.REQ)
        for index, p in enumerate(self.ReplicationPorts):
            ReplicationSocket.connect(self.DataNodesIp[index]+p)
        while True:
            AllData=list(self.LookUpTable.find())
            FileNamesToReplicate=[]
            InfoOfFileNames=[]
            for doc in AllData:
                if (doc["Alive"]== "True"):
                    if (doc["FileName"] not in FileNamesToReplicate):
                        FileNamesToReplicate.append(doc["FileName"])
                        InfoOfFileNames.append( [[doc["IP"]] , 1 ,doc["ID"] ] )
                    else:
                        Index = FileNamesToReplicate.index(doc["FileName"])
                        InfoOfFileNames[Index][1]+=1
                        InfoOfFileNames[Index][0].append(doc["IP"])
            count = 0
            #filter Files in more than or equal to 3 nodes
            while (count<len(FileNamesToReplicate)):
                if(InfoOfFileNames[count][1]>=3):
                    del InfoOfFileNames[count]
                    del FileNamesToReplicate[count]
                else:
                    count+=1
            
            # to replicate to     FileName , Data , ID , IpList , PortList
            
            for indx , F in enumerate(FileNamesToReplicate):
                ipList=[]
                portList=[]
                portsToFree=[]
                count = 0
                for  indx2 , p in enumerate(self.nodesIps_Ports):
                    if (p[0] not in InfoOfFileNames[indx][0]):
                        for i in range(1 , 4):
                            if(self.nodesIps_Ports_conditinos[indx2][i]=="" ):
                               self.nodesIps_Ports_conditinos[indx2][i]== InfoOfFileNames[indx][2]+"REP"
                               ipList.append(p[0])
                               portList.append(p[i])
                               portsToFree.append( (indx2 , i))
                               count+=1
                               break
                            if(count == (3-InfoOfFileNames[indx][1])):break   
                    if(count == (3-InfoOfFileNames[indx][1])):break               
                #start replication
                # if he is sad but , after portlist
                ReplicationSocket.send_pyobj((F , "" , InfoOfFileNames[indx][2] ,ipList ,portList ) )
                ReplicationSocket.recv_string()
                for ele in portsToFree:
                    self.nodesIps_Ports_conditinos[ele[0]][ele[1]]=""
                    
                
                
                    
                    
                    
        
                
print("master starts")
if (len(sys.argv)==3):
    m = Master(sys.argv[1] ,sys.argv[2] )
elif (len(sys.argv) ==4):
    m = Master(sys.argv[1] ,sys.argv[2] ,sys.argv[3])
else:
    m = Master()
   