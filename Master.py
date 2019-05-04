import zmq
import sys
import random
import time
import threading

class Master:
    ip = "tcp://localhost:"
    port = "9998" 
    nodesIps_Ports = [("tcp://localhost:" , "9991" , "9992" , "9993") , ("tcp://localhost:" , "9994" , "9995" , "9996")]
    zmqContext = zmq.Context()
   

    def __init__(self, ip = None , port = None):
        if ip != None:
              self.ip = ip
        if port != None:
            self.port = port

        self.start()
    
    def start(self):
        print("Server is Ready")
        print("waiting for client")
        while True:
            clientSocket = self.zmqContext.socket(zmq.REP)
            clientSocket.bind("tcp://*:%s" % self.port)
            query = clientSocket.recv_pyobj()
            print("client with ID " + str(query[0]) + "want to" + str(query[1]))
            if query[1] == "Download":
                
                
                
                l = (self.nodesIps[0],self.nodesPorts[0],self.nodesIps[1],self.nodesPorts[1])
                #self.clientSocket.send_pyobj(l)
            elif query[1] == "Upload":
                t = threading.Thread(target = self.upload, args = (clientSocket))
                t.start()
              
    def Download(self , ClientSocket):
        sdfsadf
    
     




    def upload (self , ClientSocket):
        index = random.randint(0, len(self.nodesIps_Ports)-1)
        l = self.nodesIps_Ports[index]
        self.clientSocket.send_pyobj(l)
        DataNodeSocket = self.zmqContext.socket(zmq.REP)
        ip , DataPorts =  self.nodesIps_Ports[index]
        for p in DataPorts:
             DataNodeSocket.connect(ip+p)
        FileName ,success , ID = clientSocket.recv_pyobj() 
                    

                        
                

print("server starts")
m = Master()