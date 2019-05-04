import zmq
import sys
import random
import time
import threading

class Master:
    ip = "tcp://localhost:"
    port = "9998"
    nodesIps = ("tcp://localhost:","tcp://localhost:","tcp://localhost:")
    nodesPorts = ("3941","4594","31232")
    zmqContext = zmq.Context()
    clientSocket = zmqContext.socket(zmq.REP)

    def __init__(self, ip = None , port = None):
        if ip != None:
              self.ip = ip
        if port != None:
            self.port = port

        self.start()
    
    def start(self):
        print("Server is Ready")
        print("waiting for client")
        self.clientSocket.bind("tcp://*:%s" % self.port)
        query = self.clientSocket.recv_pyobj()
        print("client with ID " + str(query[0]) + "want to" + str(query[1]))
        if query[1] == "Download":
            l = (self.nodesIps[0],self.nodesPorts[0],self.nodesIps[1],self.nodesPorts[1])
            self.clientSocket.send_pyobj(l)
        elif query[1] == "Upload":
            l = (self.nodesIps[0],self.nodesPorts[0])
            self.clientSocket.send_pyobj(l)

print("server starts")
m = Master()