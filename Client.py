import zmq
import sys
import random
import time
import threading
import os

class Client:
    id = ""
    masterIp = "tcp://localhost:"
    masterPortList = ["9998"]
    services = ["Download","Upload"]
    zmqContext = zmq.Context()
    poller = zmq.Poller()
    timeOut = 1000
    uploadTimeout = 120000
    # constructor
    def __init__(self, clientId):
        self.id = clientId
        while True:
            try:
                print()
                self.start()
            except zmq.NotDone:
                print("Request Time out, please try again")


            
            print("To do another Service, Please press 1")
            name = input()
            if int(name) != 1:
                print("Good bye")
                break


    # the main function of the client      
    def start(self):
        masterSocket = self.zmqContext.socket(zmq.REQ)
        masterSocket.linger = 0
        self.poller.register(masterSocket, zmq.POLLIN)
        for port in self.masterPortList:
            masterSocket.connect(self.masterIp+port)

        print("trying Connect to Master ...")
        myChoice = 0
        while(int(myChoice) != 1 and int(myChoice) != 2):
            myChoice = input("Choose one of the two services below:\n1. download\n2. upload\n")

        myChoice = int(myChoice)

        try:
            masterSocket.send_pyobj((self.id,self.services[myChoice-1]))
        except zmq.ZMQError:
            print("error connecting to server")
            self.poller.unregister(masterSocket)
            masterSocket.close()
            return

        nodeKeepersData = ()
        if(self.poller.poll(self.timeOut)):
            nodeKeepersData= masterSocket.recv_pyobj()
        else:
            print("request time out, while sending ID to server")
            self.poller.unregister(masterSocket)
            masterSocket.close()
            return

        if len(nodeKeepersData) == 0:
            print("All servers are busy, Please try later")
            self.poller.unregister(masterSocket)
            masterSocket.close()
            return
        fileName = input("Enter the file name:\n")
        self.poller.unregister(masterSocket)
        masterSocket.close()
        if int(myChoice) == 2:
            self.upload(fileName,nodeKeepersData,self.services[myChoice-1])
        else:
            self.download(fileName,nodeKeepersData,self.services[myChoice-1])


    # uploading Mp4 File
    def upload(self,fileName,nodeKeepersData,service):
        wrong = True
        name = fileName
        data = None
        while wrong:
            try:
                f = open(name,"rb")
            except IOError:
                print("Enter correct file name:")
                name = input()
                continue
            wrong = False
            data = f.read()
        
        print(f)
        print(nodeKeepersData)

        s = self.zmqContext.socket(zmq.REQ)
        s.linger = 0
        s.connect(nodeKeepersData[0]+nodeKeepersData[1])
        s.connect(nodeKeepersData[0]+nodeKeepersData[2])
        s.connect(nodeKeepersData[0]+nodeKeepersData[3])
        s.send_pyobj((self.id,name,service,data))
        if(self.poller.poll(self.uploadTimeout)):
            s.recv_string()
            print("File "+name+" has been uploaded successfully")
        else:
            print("Can't upload the file, Please try again")
        s.close()
    
    
    # downloading Mp4 File
    def download(self,fileName,nodeKeepersData,service):
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
        socket.send_str(str(name)+"\n"+str(count)+"\n"+str(size))
        data = socket.recv()
    
    #stop connection when signout
    def __del__(self):
        x = 0

c = Client(123)