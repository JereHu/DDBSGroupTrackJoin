import os
import time
from multiprocessing import Queue, Process
import pandas as pd
import csv
from logging import debug


class Node(Process):

    def __init__(self, q, nodeID, hashing_fun):

        super().__init__()
        self.queue = q

        self.nodeID = nodeID
        self.hashing_fun = hashing_fun

        self.TableR = pd.read_csv('nodes/{}/r.csv'.format(nodeID))
        self.TableS = pd.read_csv('nodes/{}/s.csv'.format(nodeID))
        self.TableTrs = []
        self.TableTr = {}
        self.TableTs = {}

        self.messageSentOUT = 0
        self.messageSentIN = 0
        self.messageReceived = 0

        self.messageOUT = []
        self.messageIN = []

        print('Successfully started node', self.nodeID, 'on', self)

    def sendTo(self, sendTo, key, sentBy):
        self.messageOUT.append([key, sendTo, sentBy])

    def send(self):
        with open("nodes/{}/OUT.csv".format(self.nodeID), 'w+', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['ID', 'sendTo', 'sentBy'])
            for message in self.messageOUT:
                csvwriter.writerow(message)
                self.messageSentOUT += 1
        csvfile.closed
        self.messageOUT = []

    def sendToT(self, ID, nodeRef):
        with open("nodes/{}/t.csv".format(self.nodeID), 'a+', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow([ID, nodeRef])
            self.messageSentIN += 1
        csvfile.closed
    def processR1(self):
        '''
        First part of process R: Send all distinct keys to the hashed node
        '''
        for key, payload in self.TableR.iterrows():
            if payload['ID'] not in self.TableTr:
                sendTo_node = self.hashing_fun(payload['ID'])

                if self.nodeID == sendTo_node:
                    self.sendToT(ID=payload['ID'], nodeRef='R{}'.format(self.nodeID))
                else:
                    self.sendTo(sendTo=sendTo_node, key=payload['ID'], sentBy='R{}'.format(self.nodeID))
                print('R{}'.format(self.nodeID), 'sent', payload['ID'], 'to', sendTo_node)
                self.TableTr[payload['ID']] = []

            self.TableTr[payload['ID']].append(payload.values.flatten().tolist()[1:])

    def processR2(self):
        '''
        Second part of process R: Get inbound messages
        '''
        inbound_mes = pd.read_csv('nodes/{}/IN.csv'.format(self.nodeID))

        with open("nodes/{}/payloadOUT.csv".format(self.nodeID), 'w+', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['ID', 'payload', 'sendTo'])
            for key, payload in inbound_mes.iterrows():
                ID = payload['ID']
                sendTo = payload['sendTo']
                row = ','.join([' '.join([str(c) for c in lst]) for lst in self.TableTr[ID]])
                message = [ID, row, sendTo]
                print(message)
                csvwriter.writerow(message)
                self.messageReceived += 1
                self.messageSentOUT += 1
                csvfile.flush()
        csvfile.closed
        print(self.nodeID, 'sent payloads')

    def processS1(self):
        '''
        First part of process S: Send all distinct keys to the hashed node
        '''
        for key, payload in self.TableS.iterrows():
            if payload['ID'] not in self.TableTs:
                sendTo_node = self.hashing_fun(payload['ID'])
                if self.nodeID == sendTo_node:
                    self.sendToT(ID=payload['ID'], nodeRef='S{}'.format(self.nodeID))
                    self.messageSentIN += 1
                else:
                    self.sendTo(sendTo=sendTo_node, key=payload['ID'], sentBy='S{}'.format(self.nodeID))
                    self.messageSentOUT += 1
                self.TableTs[payload['ID']] = []
            self.TableTs[payload['ID']].append(payload.values.flatten().tolist())

    def processS2(self):
        '''
        Second part of process S: Get inbound messages from all R nodes and join them
        '''
        inbound_mes = pd.read_csv('nodes/{}/payloadIN.csv'.format(self.nodeID))
        with open("nodes/{}/JoinedTable.csv".format(self.nodeID), 'w+', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['ID', 'Family Name', 'First Name'])
            for key, message in inbound_mes.iterrows():
                ID = message['ID']
                payload = message['payload']
                for itemS in self.TableTs[int(ID)]:
                    for itemR in payload.split(' '):
                        message = [ID, itemS[1], itemR]
                csvwriter.writerow(message)
                self.messageReceived += 1
        csvfile.closed






    def processT(self):
        '''
        First part of process T: read all inboud messages and add them to Trs
        '''
        inbound_mes = pd.read_csv('nodes/{}/IN.csv'.format(self.nodeID))

        with open("nodes/{}/t.csv".format(self.nodeID), 'a+', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            for key, payload in inbound_mes.iterrows():
                ID = payload['ID']
                nodeRef = payload['sentBy']
                csvwriter.writerow([ID, nodeRef])
                self.messageReceived += 1
        csvfile.closed
        '''
        Second part of process T: Iterate through all distinct key,R_node pairs and for all processes that have the same 
        key but are on S sent the key and the corresponding node S to right node R
        '''
        self.TableTrs = pd.read_csv('nodes/{}/t.csv'.format(self.nodeID))

        TableTrs_cpy = self.TableTrs
        for key, row in self.TableTrs.iterrows():
            for key2, row2 in TableTrs_cpy.iterrows():
                if row['nodeID'][0] == 'R' and row2['nodeID'][0] == 'S' and row['ID'] == row2['ID']:
                    self.sendTo(sentBy=row['nodeID'], key=row2['ID'], sendTo=row2['nodeID'])
                    print(row['nodeID'], 'should send', payload['ID'], 'to', row2['nodeID'])

    def run(self):

        readbuffer = ""
        self.processR1()
        self.processS1()
        self.send()
        self.queue.put(False)

        time.sleep(1)
        phaseID = 0
        while True:
            print(self.nodeID, 'waiting for message, completed:', phaseID)
            phaseID = self.queue.get()  # This will block until a message is sent to the queue.
            print(self.nodeID,'got message', phaseID)

            if phaseID == 1:
                print(self.nodeID, 'reading inbound messages')
                self.processT()
                self.send()
                self.queue.put(False)
            elif phaseID == 2:
                print(self.nodeID, 'Reading inbound messages for Process R')
                self.processR2()
                self.send()
                self.queue.put(False)
            elif phaseID == 3:
                self.processS2()
                self.send()
                self.queue.put(False)
                break

            time.sleep(5)
        print('{} summary:\n   messages sent:     {}\n   messages received: {}'.format(self.nodeID, self.messageSentOUT, self.messageReceived))
        print('done')

