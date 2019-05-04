import zmq
import sys
import random
import time
import threading
import os

class Client:
    id = ""
    masterIp = "tcp://localhost:"
    masterPortList = ["9998" , "9999"]
    services = ["Download","Upload"]
    zmqContext = zmq.Context()
    masterSocket = zmqContext.socket(zmq.REQ)
    # constructor
    def __init__(self, clientId):
        self.id = clientId
        try:
            self.start()
        except Exception as e:
            print(e)

    # the main function of the client      
    def start(self):
        print("Connect to Master ...")
        for port in self.masterPortList:
            self.masterSocket.connect(self.masterIp+port)
        myChoice = 0
        while(int(myChoice) != 1 and int(myChoice) != 2):
            myChoice = input("Choose one of the two services below:\n1. download\n2. upload\n")

        myChoice = int(myChoice)
        self.masterSocket.send_pyobj((self.id,self.services[myChoice-1]))
        nodeKeepersData = self.masterSocket.recv_pyobj()
        fileName = input("Enter the file name:\n")
        if int(myChoice) == 2:
            self.upload(fileName,nodeKeepersData)
        else:
            self.download(fileName,nodeKeepersData)

    # uploading Mp4 File
    def upload(self,fileName,nodeKeepersData):
        f = os.stat(fileName)
        f = f.st_size()
        print(f)
        #s = self.zmqContext.socket(zmq.REQ)
        #s.connect(nodeKeepersData[0]+nodeKeepersData[1])
        #s.send()
        

    # downloading Mp4 File
    def download(self,fileName,nodeKeepersData):
        size = int(len(nodeKeepersData)/2)
        i = 0
        counter = 1
        while (i < (2*size)-1):
            t = threading.Thread(target = self.downloadPiece, args = (fileName,nodeKeepersData[i]+nodeKeepersData[i+1],counter,size))
            t.start()
            i+=2
            counter += 1

    def downloadPiece(self,name,target,count,size):
        socket = self.zmqContext.socket(zmq.REQ)
        socket.connect(target)
        socket.send(name+"\n"+count+"\n"+size)
        data = socket.recv()
    
    #stop connection when signout
    def __del__(self):
        for port in self.masterPortList:
            self.masterSocket.disconnect(self.masterIp+port)

print("Hello Guys")
c = Client(123)