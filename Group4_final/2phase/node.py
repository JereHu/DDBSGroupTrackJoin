import os
import time
from multiprocessing import Queue, Process
import pandas as pd
import csv
from logging import debug


class Node(Process):

    def __init__(self, q, nodeID, hashing_fun):
        '''
        length_R = length of payload R
        length_S = length of payload S
        MessageSize = Message size

        debuggin = if True it will print a lot of debugging informations

        queue = message queue used by the processes to communicate

        nodeID = id of the node in range(0,n_nodes)
        hashing_fun = hashing function passed from the main

        TableR = Table used by process R
        TableS = Table used by process S
        TableT = Table used by process T

        MessageSentOUT = Amount of messages sent from S out of the node during S1
        MessageSentIN = Amount of messages sent from S to R in the same node

        messageSentOUT_payload = Payloads sent out of the node from process S0
        messageSentIN_payload = Pyaloads sent in the same node from S

        messageOut = list containing all messages that will be sent at the end of the respective phases
        '''
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

        self.messageSentOUT_payload = 0
        self.messageSentIN_payload = 0

        self.messageReceived = 0
        
        self.debug_text = False
        
        self.messageOUT = []
        self.messageIN = []

        if self.debug_text: print('Successfully started node', self.nodeID, 'on', self)

    def sendTo(self, sendTo, key, sentBy):
        self.messageOUT.append([key, sendTo, sentBy])

    def send(self):
        with open("nodes/{}/OUT.csv".format(self.nodeID), 'w+', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['ID', 'sendTo', 'sentBy'])
            for message in self.messageOUT:
                csvwriter.writerow(message)
        csvfile.closed
        self.messageOUT = []

    def sendToT(self, ID, nodeRef):
        with open("nodes/{}/t.csv".format(self.nodeID), 'a+', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow([ID, nodeRef])
        csvfile.closed

    def processR1(self):
        '''
        First part of process R: Send all distinct keys to the hashed node
        '''
        for key, payload in self.TableR.iterrows():
            if payload['ID'] not in self.TableTr:
                sendTo_node = self.hashing_fun(payload['ID'])

                if self.nodeID == sendTo_node:
                    self.messageSentIN += 1
                    self.sendToT(ID=payload['ID'], nodeRef='R{}'.format(self.nodeID))
                else:
                    self.messageSentOUT += 1
                    self.sendTo(sendTo=sendTo_node, key=payload['ID'], sentBy='R{}'.format(self.nodeID))
                if self.debug_text: print('R{}'.format(self.nodeID), 'sent', payload['ID'], 'to', sendTo_node)
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
                if self.debug_text: print(message)
                csvwriter.writerow(message)
                self.messageReceived += 1
                if sendTo[1] == str(self.nodeID):
                    self.messageSentIN_payload += 1
                else:
                    self.messageSentOUT_payload += 1
                csvfile.flush()
        csvfile.closed
        if self.debug_text: print(self.nodeID, 'sent payloads')

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
        if self.nodeID == 0: print('      Running Process T1')
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
        if self.nodeID == 0: print('      Running Process T2\n        This takes time 5-10min')
        TableTrs_cpy = self.TableTrs
        for key, row in self.TableTrs.iterrows():
            for key2, row2 in TableTrs_cpy.iterrows():
                if row['nodeID'][0] == 'R' and row2['nodeID'][0] == 'S' and row['ID'] == row2['ID']:
                    self.sendTo(sentBy=row['nodeID'], key=row2['ID'], sendTo=row2['nodeID'])
                    if self.debug_text: print(row['nodeID'], 'should send', payload['ID'], 'to', row2['nodeID'])

    def run(self):

        readbuffer = ""
        if self.nodeID == 0: print('    Starting Process R1,S1')
        self.processR1()
        self.processS1()
        self.send()
        self.queue.put(False)

        time.sleep(1)
        phaseID = 0
        while True:
            if self.debug_text: print(self.nodeID, 'waiting for message, completed:', phaseID)
            phaseID = self.queue.get()  # This will block until a message is sent to the queue.
            if isinstance(phaseID, bool):
                self.queue.put(phaseID)
                continue
            if self.debug_text: print(self.nodeID,'got message', phaseID)

            if phaseID == 1:
                if self.debug_text: print(self.nodeID, 'reading inbound messages')
                if self.nodeID == 0: print('    Starting Process T')
                self.processT()
                self.send()
                self.queue.put(False)
            elif phaseID == 2:
                if self.debug_text: print(self.nodeID, 'Reading inbound messages for Process R')
                if self.nodeID == 0: print('    Starting Process R2')
                self.processR2()
                self.send()
                self.queue.put(False)
            elif phaseID == 3:
                self.processS2()
                if self.nodeID == 0: print('    Starting Process S2')
                self.send()
                self.queue.put(False)
                break

            time.sleep(5)
        if self.debug_text: print('{} summary:\n   messages sent:     {}\n   messages sent intra node:     {}\n   messages payload out: {}\n   messages payload in: {}'.format(self.nodeID, self.messageSentOUT, self.messageSentIN,  self.messageSentOUT_payload, self.messageSentIN_payload))

        '''
        Write all paramethers to the analytics file
        '''
        with open("nodes/{}/analytics.csv".format(self.nodeID), 'a+', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['message_sent_out_1',self.messageSentOUT])
            csvwriter.writerow(['message_sent_in_1',self.messageSentIN])
            csvwriter.writerow(['payloads_IN',self.messageSentIN_payload])
            csvwriter.writerow(['payloads_OUT',self.messageSentOUT_payload])
        csvfile.closed
        if self.debug_text: print('done')

