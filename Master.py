import zmq
import sys
import random
import time
import threading
import multiprocessing
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
    ClientPorts =["9998" , "9997" , "9996"]  
    DataPorts=  ["9999" , "9989" , "9979"]
    zmqContext = zmq.Context()

    def __init__(self , ip = None ):
        if ip != None:
              self.ip = ip
        self.preStart()
    
    
    def preStart(self):
        with multiprocessing.Manager() as manager:  
            nodesIps_Ports = manager.list([]) 
            nodesIps_Ports_conditinos = manager.list([]) 
            p1 = multiprocessing.Process(target=self.start, args=(nodesIps_Ports ,nodesIps_Ports_conditinos ,self.ClientPorts[0] ,self.DataPorts[0] ,self.Topics[0] ))
            p2 = multiprocessing.Process(target=self.start, args=(nodesIps_Ports ,nodesIps_Ports_conditinos ,self.ClientPorts[1] ,self.DataPorts[1] ,self.Topics[1]))
            p3 = multiprocessing.Process(target=self.start, args=(nodesIps_Ports ,nodesIps_Ports_conditinos ,self.ClientPorts[2] ,self.DataPorts[2] ,self.Topics[2]))
            
            p1.start()
            p2.start()
            p3.start()
            p1.join() 
            p2.join()
            p3.join()
        
    
    def start(self , nodesIps_Ports ,nodesIps_Ports_conditinos ,ClientPort,DataPort , T ):
       
        print("Server is Ready")
        print("waiting for client")
        # can change those to processes if you want 
        t = threading.Thread(target = self.ClientHandle, args = (nodesIps_Ports ,nodesIps_Ports_conditinos ,ClientPort))
        t.start()
        t1 = threading.Thread(target = self.DataHandle, args = (nodesIps_Ports ,nodesIps_Ports_conditinos ,DataPort))
        t1.start()
        t2 = threading.Thread(target = self.AliveHandle, args = (T , nodesIps_Ports ,nodesIps_Ports_conditinos ))
        t2.start()
        #t3 = threading.Thread(target = self.ReplicationHandle, args = (nodesIps_Ports ,nodesIps_Ports_conditinos))
        #t3.start()
        
        t1.join()
        t2.join()
        t.join()
        #t3.join()
    
                 
    def ClientHandle (self ,nodesIps_Ports ,nodesIps_Ports_conditinos ,ClientPort):
        clientSocket = self.zmqContext.socket(zmq.REP)
        clientSocket.bind("tcp://*:%s" % ClientPort)
        print("binded to " + ClientPort)
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
                    for IpIndx , node in enumerate(nodesIps_Ports):
                        if(node[0]==p):
                            for portIndx in range(1,4):
                                if(nodesIps_Ports_conditinos[IpIndx][portIndx]==""):
                                     ListToSend.append(p)
                                     ListToSend.append(node[portIndx])
                                     A=nodesIps_Ports_conditinos[IpIndx]
                                     A[portIndx]=ID
                                     nodesIps_Ports_conditinos[IpIndx][portIndx]=A
                                     count+=1
                                if(count==6): break
                            break
                                    
                        if(count==6): break            
                            
                    if(count==6): break
                if(len(ListofIps)==0):ListToSend.append("NotFound")    
                if(len(ListToSend)>1 ): 
                    print("made ports busy" ) 
                    print(ListToSend) 
                    print(nodesIps_Ports_conditinos)
                TupleToSend=tuple(ListToSend)  
                clientSocket.send_pyobj(TupleToSend)
                     
            #handle if the chosin ip is busy   
            elif operation == "Upload":
                
                index = random.randint(0, len(nodesIps_Ports)-1)
                l = nodesIps_Ports[index]
                portsTosend=[]
                for idx, val in enumerate(l):
                    if (nodesIps_Ports_conditinos[index][idx] == ""  or idx==0 ):
                         portsTosend.append(nodesIps_Ports[index][idx])
                index2 = random.randint(1, len(portsTosend)-1)
                portsToSendFinal=[]
                portsToSendFinal.append(portsTosend[0])
                portsToSendFinal.append(portsTosend[index2])
                T = tuple(portsToSendFinal)
                print(T)
                print("changed port to busy")
                A = nodesIps_Ports_conditinos[index]
                A[index2]=ID
                nodesIps_Ports_conditinos[index]=A
                print(nodesIps_Ports_conditinos)
                clientSocket.send_pyobj(T)
        
    def DataHandle (self  , nodesIps_Ports ,nodesIps_Ports_conditinos ,DataPort):
        DataSocket = self.zmqContext.socket(zmq.REP)
        DataSocket.bind("tcp://*:%s" % DataPort)
        print("binded to " + DataPort)
        while (True):
            ID , Ip , FileName = DataSocket.recv_pyobj()
            print(FileName)
            #upload
            if (FileName !=""):
                mydict = {"ID": ID, "IP": Ip, "FileName": ID+FileName , "Alive":"True"}
                x = self.LookUpTable.insert_one(mydict)
                print("updated the data base")
                for IpIndx , node in enumerate(nodesIps_Ports):
                    if(Ip==node[0]):
                        for portIndx in range(1,4):
                             if(nodesIps_Ports_conditinos[IpIndx][portIndx]==ID):
                                 A=nodesIps_Ports_conditinos[IpIndx]
                                 A[portIndx]=""
                                 nodesIps_Ports_conditinos[IpIndx]=A
                                 break
                        break
                print("freeing port in upload")
                print(nodesIps_Ports_conditinos)
                DataSocket.send_string("Done")
            #download
            else:
                for IpIndx , node in enumerate(nodesIps_Ports):
                    if(Ip==node[0]):
                        for portIndx in range(1,4):
                             if(nodesIps_Ports_conditinos[IpIndx][portIndx]==ID):
                                 A=nodesIps_Ports_conditinos[IpIndx]
                                 A[portIndx]=""
                                 nodesIps_Ports_conditinos[IpIndx]=A
                                 
                                 
                        break
                print("freeing ports used in download")
                print(nodesIps_Ports_conditinos)
                DataSocket.send_string("Done")
                

    def AliveHandle (self ,topic, nodesIps_Ports ,nodesIps_Ports_conditinos ):
        #for t in self.Topics:
         #   t = threading.Thread(target = self.AliveHandleForTopic, args = (t,nodesIps_Ports ,nodesIps_Ports_conditinos))
          #  t.start()
        #t.join()
        self.AliveHandleForTopic( topic, nodesIps_Ports , nodesIps_Ports_conditinos)
        
        
    def AliveHandleForTopic(self , topic , nodesIps_Ports ,nodesIps_Ports_conditinos):
        poller = zmq.Poller()
        
        AliveSocket = self.zmqContext.socket(zmq.SUB)
        for index ,p in enumerate(self.IamAlivePorts):
            AliveSocket.connect(self.DataNodesIp[index]+ p)
        AliveSocket.setsockopt_string(zmq.SUBSCRIBE,topic)
        poller.register(AliveSocket , zmq.POLLIN)
        L=AliveSocket.recv_string()
        topic, ip , port1 , port2 , port3 = L.split()
        PortInfo=[ip , port1 , port2 , port3]
        nodesIps_Ports.append(PortInfo)
        nodesIps_Ports_conditinos.append([ip , "" , "" , ""])
        print("node is alive")
        print(nodesIps_Ports  , nodesIps_Ports_conditinos)
        DontAcessDB = False
        while True:
            if(poller.poll(1500)):
                if(DontAcessDB==True):
                    myquery = { "IP": ip }
                    newvalues = { "$set": { "Alive": "True" } }
                    x = self.LookUpTable.update_many(myquery, newvalues)
                AliveSocket.recv_string()
                if (PortInfo not in nodesIps_Ports):
                    print("node is alive again")
                    nodesIps_Ports.append(PortInfo)
                    nodesIps_Ports_conditinos.append([ip , "" , "" , ""])
                    print(nodesIps_Ports ,nodesIps_Ports_conditinos )
                
                DontAcessDB=False
            else:
                if(DontAcessDB==False):
                    myquery = { "IP": ip }
                    newvalues = { "$set": { "Alive": "False" } }
                    x = self.LookUpTable.update_many(myquery, newvalues)
                if(PortInfo  in nodesIps_Ports ):
                    DontAcessDB = True
                    index = nodesIps_Ports.index(PortInfo)
                    del nodesIps_Ports[index]
                    del nodesIps_Ports_conditinos[index]
                    print("node is down")
                    print(nodesIps_Ports ,nodesIps_Ports_conditinos )
        
           
    def ReplicationHandle (self , nodesIps_Ports ,nodesIps_Ports_conditinos):
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
                for  indx2 , p in enumerate(nodesIps_Ports):
                    if (p[0] not in InfoOfFileNames[indx][0]):
                        for i in range(1 , 4):
                            if(nodesIps_Ports_conditinos[indx2][i]=="" ):
                               A=nodesIps_Ports_conditinos[indx2]
                               A[i]= InfoOfFileNames[indx][2]+"REP"
                               nodesIps_Ports_conditinos[indx2]= A
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
                    nodesIps_Ports_conditinos[ele[0]][ele[1]]=""
                # remove break if you want o replicate many fiels together
                break
                    
                
                
                    
                    
                    
if __name__ == '__main__':        
                
    print("master starts")
    if (len(sys.argv)==2):
        m = Master(sys.argv[1] ,sys.argv[2] )
    else:
        m = Master()
   