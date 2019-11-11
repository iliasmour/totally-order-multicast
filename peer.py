#
# Event-driven code that behaves as either a client or a server
# depending on the argument.  When acting as client, it connects
# to a server and periodically sends an update message to it.
# Each update is acked by the server.  When acting as server, it
# periodically accepts messages from connected clients.  Each
# message is followed by an acknowledgment.
#
# Tested with Python 2.7.8 and Twisted 14.0.2
## Assignement 
#ILIAS MOURTOS AM: 2302 email: iliasmourtos@gmail.com
#sunergasia me ARTEMIS-MARIA CHATZIROUFA AM:2857 email:ar.chatziroufa@gmail.com

import optparse
import sys

from twisted.internet.protocol import Protocol, ClientFactory
from twisted.internet import reactor
import time

global writeFile
proc_list = []
lamportClock= []

def parse_args():
    usage = """usage: %prog [options] [client|server] [hostname]:port

    python peer.py server 127.0.0.1:port """

    parser = optparse.OptionParser(usage)

    _, args = parser.parse_args()

    if len(args) != 4:
        print parser.format_help()
        parser.exit()
    hostname1,hostname2, idProcess, port = args
    if ((hostname2 == "None"  or hostname1== "None") and int(idProcess)>=1):
        if(hostname1 =="None"):
            print "You gave None as first hostname  and you cannot connect to process 0\n"
        else:
            print "You gave None as second hostname  and you cannot connect to process 1\n"
        print "try again"
        exit(0)
    else:
        return hostname1,hostname2,idProcess,port


class Peer(Protocol):
    global proc_list,lamportClock
    acks = 0
    connected = False
    numMessage= 1
    numberSendMessages = 20
    sendafter = 0
    idOfprocess = -1
    listOfMessagesInBuffer = []
    allDeliveredMessages = []

    def __init__(self, factory, peer_type):
        self.pt = peer_type
        self.factory = factory

    def connectionMade(self):
        proc_list.append(self)
        self.initIdMsg()
        if len(proc_list)>1:
            self.initLambortClock()
            self.sendMessage()
                

    def initIdMsg(self):
        self.getIdOfProcess()

    def getIdOfProcess(self):
        if self.pt == "process0":
            self.idOfprocess = 0
            self.sendAfter(1)
        elif self.pt == "process1":
            self.IdOfProcess = 1
            self.sendAfter(2)
        else:
            self.idOfprocess = 2
            self.sendAfter(3)
    def getMyId(self):
        if self.pt == "process0":
            return 0
        elif self.pt == "process1":
            return 1
        else:
            return 2

    def initLambortClock(self):

        for i in range (0,3):
            lamportClock.append(1.0)

    def sendAfter(self,number):
        self.sendafter = number

    def sendMessage(self):         
        self.increaseMyClock()
        print "Sending Message"
        try:
            self.idOfprocess = self.getMyId()
            message =Message('startMsg:<Message '+str(self.numMessage)+'>',int(self.idOfprocess),float(lamportClock[self.idOfprocess]))
            print "from  "+" "+self.pt+" "+"to other nodes-processes(0,1,2)"
            print "(Content of Message) : <Message"+" "+str(self.numMessage)+">"+" "+"Id process"+" "+str(self.idOfprocess)+" "+"Time"+" "+str(lamportClock[self.idOfprocess])
            self.allDeliveredMessages.append(message)
            print '\n'
            for i in proc_list:
                i.transport.write("startMsg:<Message "+str(self.numMessage)+">"+"  "+"-"+" " +str(self.idOfprocess)+"  "+"-"+" "+str(lamportClock[self.idOfprocess])+'<end>')
            self.numMessage += 1
        except Exception, ex1:
            print "Exception trying to send: ", ex1.args[0]
        if self.connected == True:
            if self.numMessage <= self.numberSendMessages:
                reactor.callLater(self.sendafter,self.sendMessage)

    def increaseMyClock(self):
        print "Increase my clock after an event\n"
        lamportClock[self.getMyId()] += 1.0
        self.printMyClock()

    def updateLamportClock(self,idproc,lamportClockOther):
        lamportClock[self.getMyId()] = max(lamportClock[self.getMyId()],lamportClockOther) + 1.0
        lamportClock[idproc] = lamportClockOther
        self.printMyClock()

    def printMyClock(self):
        print "********************LamportClock**************************************"
        print '\n'
        print 'LamportClock[0] = ' + str(lamportClock[0])
        print 'LamportClock[1] = ' + str(lamportClock[1])
        print 'LamportClock[2] = ' + str(lamportClock[2])
        print '\n'
        print "********************--END--***************************************"
    def sortClock(self):
        copyListOfMessages=sorted(self.allDeliveredMessages,key=lambda k:( k.timestamp,k.getIdMsg()))
        
    def sendAck(self,msg):
        print "sendAck"
        self.idOfprocess = self.getMyId()
        self.ts = time.time()
        try:
            for i in proc_list:
                i.transport.write("startAck:<Ack>" +"   "+"-"+" "+str(self.idOfprocess)+"  "+"-"+" "+str(lamportClock[self.idOfprocess]) +'<end>')
        except Exception, e:
            print e.args[0]



    def saveMessageInBuffer(self,data):
        self.listOfMessagesInBuffer.append(data)
        msg = self.listOfMessagesInBuffer[0]
        self.listOfMessagesInBuffer.pop(0)
        return msg

    def splitMessage(self,message):
        if "startMsg" in message:
            print "Received Message"
            storeMessage = message
            m = message.split('-')
            msgName = m[0]
            process = int(m[1])
            time =float(m[2])
            print "From process"+" "+str(process)
            print  "Content of Message :" +storeMessage
            print '\n'
            msg = Message(msgName,process,time)
            self.updateLamportClock(process,time)
            self.allDeliveredMessages.append(msg)
            self.increaseMyClock()
            self.sendAck(msg)
        elif "startAck" in message:
            print "Received Ack\n"
            storeMessage = message
            m = message.split('-')
            process = int(m[1])
            time =float(m[2])
            print "From process"+" "+str(process)
            print  "Content of Message :" +storeMessage
            print '\n'
            self.acks += 1
            self.updateLamportClock(process,time)


    def configureMessage(self,data):
        messages = data.split("<end>")
        for message in messages:
            self.splitMessage(message)


    def deliverMessage(self):
        for message in self.allDeliveredMessages:
            countMessage = 0
            for timestamp in lamportClock:
                if message.getTimestamp()<= timestamp:
                   countMessage = countMessage + 1
            if countMessage == 3:
                msg = Message(message.getMessage(),message.getIdMsg(),message.getTimestamp())
                #self.writeInFile(msg)
                self.allDeliveredMessages.remove(message)
        

    def dataReceived(self, data):
        msg = self.saveMessageInBuffer(data)
        self.configureMessage(msg)
        self.sortClock()
        self.deliverMessage()


    def writeInFile(self,message):
        global writeFile
        #print "Content of Message:      From idProcess      Time  \n"
        if(self.getMyId()== 0):
            filename0 = "delivered-messages-0"
            writeFile = open(filename0,'a')
            writeFile.write(str(message.getMessage())+'   '+str(message.getIdMsg())+'  '+str(message.getTimestamp())+'\n')
        elif (self.getMyId()==1):
             filename1 = "delivered-messages-1"
             writeFile = open(filename1,'a')
        else:
            filename2 = "delivered-messages-2"
            writeFile = open(filename2,'a')
        writeFile.write(str(message.getMessage())+'   '+str(message.getIdMsg())+'  '+str(message.getTimestamp())+'\n')
        writeFile.flush()

    def connectionLost(self, reason):
        print "Disconnected"
        if self.pt == 'server':
            self.connected = False
            self.done()

    def done(self):
        self.factory.finished(self.acks)


class Message:
    idMsg = -1
    message = ''
    timestamp = -1.0
    def __init__(self,message,idMsg,timestamp):
        self.idMsg = idMsg
        self.message = message
        self.timestamp = timestamp

    def getTimestamp(self):
        return self.timestamp

    def getIdMsg(self):
        return self.idMsg

    def getMessage(self):
        return self.message



class PeerFactory(ClientFactory):

    def __init__(self, peertype, fname):
        print '@__init__'
        self.pt = peertype
        self.acks = 0
        self.fname = fname
        self.records = []

    def finished(self, arg):
        self.acks = arg
        self.report()

    def report(self):
        print 'Received %d acks' % self.acks

    def clientConnectionFailed(self, connector, reason):
        print 'Failed to connect to:', connector.getDestination()
        self.finished(0)

    def clientConnectionLost(self, connector, reason):
        print 'Lost connection.  Reason:', reason

    def startFactory(self):
        print "@startFactory"
        if self.pt == 'server':
            self.fp = open(self.fname, 'w+')

    def stopFactory(self):
        print "@stopFactory"
        if self.pt == 'server':
            self.fp.close()

    def buildProtocol(self, addr):
        print "@buildProtocol"
        protocol = Peer(self, self.pt)
        return protocol


if __name__ == '__main__':
    hostname0,hostname1,idProcess,port = parse_args()


    if int(idProcess) == 0 :
        portProccess0=int(port)
        factoryProc = PeerFactory("process0"," connect")
        reactor.listenTCP(int(port), factoryProc)
        print "Starting server @" + hostname0 + " port " + str(portProccess0)

    elif int(idProcess) == 1 :
        portProccess1 =int(port)
        connectToZeroProcess = PeerFactory("process1"," connect")
        print "Connecting to host " + hostname0 + " port " + str(portProccess1-1)
        reactor.connectTCP(hostname0,portProccess1-1, connectToZeroProcess)
        factoryProc= PeerFactory("process1"," listen")
        print "Starting server @" + hostname1 + " port " + str(port)
        reactor.listenTCP(int(port), factoryProc)


    else: 
        connectToZeroProcess = PeerFactory("process2", "connect")
        connectToOneProcess = PeerFactory("process2"," connect")
        portProccess2 =int(port)
        print "Connecting to host " + hostname0 + " port " + str(portProccess2-2)
        reactor.connectTCP(hostname0, portProccess2-2 ,connectToZeroProcess)
        print "Connecting to host " + hostname1 + " port " + str(portProccess2-1)
        reactor.connectTCP(hostname1, portProccess2-1,connectToOneProcess)

    reactor.run()
