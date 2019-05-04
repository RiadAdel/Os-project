import zmq
import sys
import random
import time
import threading

class Master:
    ip = "tcp://localhost:"
    ClientPort , DataPort , IAmAlivePort = ("9998" , "9999" , "9997")
    nodesIps_Ports = [("tcp://localhost:" , "9991" , "9992" , "9993") , ("tcp://localhost:" , "9994" , "9995" , "9996")]
    zmqContext = zmq.Context()
   

    def __init__(self, ip = None , ClientPort = None ,DataPort=None  , IAmAlivePort=None):
        if ip != None:
              self.ip = ip
        if ClientPort != None:
            self.ClientPort = ClientPort
        if DataPort  !=None:
            self.DataPort = DataPort
        if IAmAlivePort !=None:
            self.IAmAlivePort = IAmAlivePort 
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
                print(l)
                time.sleep(10)
                clientSocket.send_pyobj(l)
        
        
        
        
    def DataHandle (self):
             x =" "
             



    def AliveHandle (self):
           x =" "
                

print("server starts")
m = Master()