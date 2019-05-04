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
    poller = zmq.Poller()
    poller.register(masterSocket, zmq.POLLIN)
    timeOut = 1000
    # constructor
    def __init__(self, clientId):
        self.id = clientId
        while True:
            try:
                print()
                self.start()
            except zmq.NotDone:
                print("Request Time out, please try again")
            except zmq.ZMQError:
                print("Not connected to server")

            
            print("To do another Service, press 1")
            name = input()
            if int(name) != 1:
                sys.exit(1)
            else:
                continue

    # the main function of the client      
    def start(self):
        for port in self.masterPortList:
            self.masterSocket.connect(self.masterIp+port)

        print("trying Connect to Master ...")
        myChoice = 0
        while(int(myChoice) != 1 and int(myChoice) != 2):
            myChoice = input("Choose one of the two services below:\n1. download\n2. upload\n")

        myChoice = int(myChoice)

        self.masterSocket.send_pyobj((self.id,self.services[myChoice-1]))


        nodeKeepersData = ()
        if(self.poller.poll(self.timeOut)):
            nodeKeepersData= self.masterSocket.recv_pyobj()
        else:
            print("request time out.")
            return

        if len(nodeKeepersData) == 0:
            print("All servers are busy, Please try later")
            return
        fileName = input("Enter the file name:\n")
        
        if int(myChoice) == 2:
            self.upload(fileName,nodeKeepersData,self.services[myChoice-1])
        else:
            self.download(fileName,nodeKeepersData)


    # uploading Mp4 File
    def upload(self,fileName,nodeKeepersData,service):
        wrong = True
        name = fileName
        data = None
        while wrong:
            try:
                f = open(name,"rb")
            except IOError:
                print("Enter correct name")
                name = input()
                continue
            wrong = False
            data = f.read()
        
        print(f)
        print(nodeKeepersData)

        s = self.zmqContext.socket(zmq.REQ)
        s.connect(nodeKeepersData[0]+nodeKeepersData[1])
        s.connect(nodeKeepersData[0]+nodeKeepersData[2])
        s.connect(nodeKeepersData[0]+nodeKeepersData[3])
        s.send_pyobj((self.id,name,service))
        x = s.recv_string()
        print(x)
        s.send_pyobj(data)
        x = s.recv_string()
        print(x)

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
        socket.send_str(str(name)+"\n"+str(count)+"\n"+str(size))
        data = socket.recv()
    
    #stop connection when signout
    def __del__(self):
        self.zmqContext.destroy()

c = Client(123)