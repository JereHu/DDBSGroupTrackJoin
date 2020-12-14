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

        self.length_R = 219
        self.length_S = 165
        self.messageSize = 10
        self.debugging = False
        
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

        # messages sent out during first phase
        self.messageOUT_1 = []
        self.messageIN_1 = []

        # messages sent out during second phaseused
        self.messageOUT_2 = []
        self.messageIN_2 = []

        # messages sent out during third phase
        self.messageOUT_3 = []
        self.messageIN_3 = []

        print('Successfully started node', self.nodeID, 'on', self)

    def analyticsWrite(self, name, value):
        with open("nodes/{}/analytics.csv".format(self.nodeID), 'a+', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow([name,value])
        csvfile.closed

    '''
    Sending functions part 1 
    '''
    def sendTo_1(self, sendTo, key, count, sentBy):
        self.messageOUT_1.append([key, count, sendTo, sentBy])
        if self.debugging: print('added message', self.messageOUT_1)

    def send_1(self):
        if self.debugging: print('nodeID, message OUT:',self.nodeID,self.messageOUT_1)
        with open("nodes/{}/OUT.csv".format(self.nodeID), 'w+', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['ID', 'count', 'sendTo', 'sentBy'])
            for message in self.messageOUT_1:
                csvwriter.writerow(message)
                if self.debugging: print(message)
                self.messageSentOUT += 1
        csvfile.closed
        self.analyticsWrite('message_sent_out_1', self.messageSentOUT)
        self.analyticsWrite('message_sent_in_1', self.messageSentIN)
        self.messageOUT_1 = []

    def sendToT(self, ID, count, nodeRef):
        with open("nodes/{}/t.csv".format(self.nodeID), 'a+', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow([ID, count, nodeRef])
            self.messageSentIN += 1
        csvfile.closed


    '''
    Sending functions part 2 
    '''
    def sendTo_2(self, sendTo, key, shouldSend):
        self.messageOUT_2.append([key, sendTo, shouldSend])

    def send_2(self):
        messageSentOUT_2 = 0
        messageSentIN_2 = 0
        if self.debugging: print('nodeID, message OUT:',self.nodeID,self.messageOUT_2)
        with open("nodes/{}/OUT_2.csv".format(self.nodeID), 'w+', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['ID', 'sendTo', 'sentBy'])
            for message in self.messageOUT_2:
                csvwriter.writerow(message)
                if self.debugging: print(message)
                if message[2][1] == str(self.nodeID):
                    messageSentIN_2+=1
                else:
                   messageSentOUT_2 += 1
        csvfile.closed

        self.analyticsWrite('message_sent_out_2', messageSentOUT_2)
        self.analyticsWrite('message_sent_in_2', messageSentIN_2)

        self.messageOUT_2 = []

    def processR1(self):
        '''
        Count how many key duplicates there are
        Then
        First part of process R: Send all distinct keys to the hashed node
        '''
        for key, payload in self.TableR.iterrows():
            if payload['ID'] not in self.TableTr:
                self.TableTr[payload['ID']] = [0,[]]

            self.TableTr[payload['ID']][0] += 1
            self.TableTr[payload['ID']][1].extend(payload.values.flatten().tolist()[1:])
        if self.debugging: print('Process R1 - Node, table', self.nodeID, self.TableTr)
        # Barrier
        for key in self.TableTr:
            sendTo_node = self.hashing_fun(key)
            if self.nodeID == sendTo_node:
                self.sendToT(ID=key, count=self.TableTr[key][0], nodeRef='R{}'.format(self.nodeID))
            else:
                self.sendTo_1(sendTo=sendTo_node, key=key, count=self.TableTr[key][0], sentBy='R{}'.format(self.nodeID))
            if self.debugging: print('R{}'.format(self.nodeID), 'sent', key, 'to', sendTo_node)

    def processS1(self):
        '''
        First part of process S: Send all distinct keys to the hashed node
        '''
        for key, payload in self.TableS.iterrows():
            if payload['ID'] not in self.TableTs:
                self.TableTs[payload['ID']] = [0,[]]

            self.TableTs[payload['ID']][0] += 1
            self.TableTs[payload['ID']][1].extend(payload.values.flatten().tolist()[1:])
        if self.debugging: print('Process S1 - Node, table', self.nodeID, self.TableTs)
        # Barrier
        for key in self.TableTs:
            sendTo_node = self.hashing_fun(key)
            if self.nodeID == sendTo_node:
                self.sendToT(ID=key, count=self.TableTs[key][0], nodeRef='S{}'.format(self.nodeID))
            else:
                self.sendTo_1(sendTo=sendTo_node, key=key, count=self.TableTs[key][0], sentBy='S{}'.format(self.nodeID))
            if self.debugging: print('S{}'.format(self.nodeID), 'sent', key, 'to', sendTo_node)

    def processR2S2_send(self):
        '''
        Second part of process R: Get inbound messages
        '''
        inbound_mes = pd.read_csv('nodes/{}/IN_2.csv'.format(self.nodeID))
        payload_message_IN_R = 0
        payload_message_OUT_R = 0
        payload_message_IN_S = 0
        payload_message_OUT_S = 0

        with open("nodes/{}/payloadOUT.csv".format(self.nodeID), 'w+', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['ID', 'payload', 'sendTo'])
            for key, payload in inbound_mes.iterrows():
                ID = payload['ID']
                sendTo = payload['sendTo']
                if payload['sentBy'][0] == 'R':
                    row = '‖'.join(fstn for fstn in self.TableTr[ID][1])
                else:
                    row = '‖'.join([lst for lst in self.TableTs[ID][1]])
                message = [ID, row, sendTo]
                if self.debugging: print(message)
                csvwriter.writerow(message)
                if sendTo[1] == f'{self.nodeID}':
                    if sendTo[0] == 'S':
                        payload_message_IN_R += 1
                    else:
                        payload_message_IN_S += 1
                else:
                    if sendTo[0] == 'S':
                        payload_message_OUT_R += 1
                    else:
                        payload_message_OUT_S += 1

                csvfile.flush()
        csvfile.closed
        self.analyticsWrite('payloads_IN_R', payload_message_IN_R)
        self.analyticsWrite('payloads_OUT_R', payload_message_OUT_R)
        self.analyticsWrite('payloads_IN_S', payload_message_IN_S)
        self.analyticsWrite('payloads_OUT_S', payload_message_OUT_S)
        print(self.nodeID, 'sent payloads')

    def processR3S3_recieve(self):
        '''
        Third part of process R: Get inbound messages from all S nodes and join them
        '''
        inbound_mes = pd.read_csv('nodes/{}/payloadIN.csv'.format(self.nodeID))
        with open("nodes/{}/JoinedTable.csv".format(self.nodeID), 'a+', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            for key, message in inbound_mes.iterrows():

                ID = message['ID']
                payload = message['payload']
                nodeRef = message['sendTo'][0]

                if ID in self.TableTr and nodeRef == 'R':
                    for itemR in self.TableTr[int(ID)][1]:
                        for itemS in payload.split('‖'):
                            message = [ID, itemS, itemR]
                            csvwriter.writerow(message)
                '''
                Third part of process S: Get inbound messages from all R nodes and join them
                '''
                if ID in self.TableTs and nodeRef == 'S':
                    for itemS in self.TableTs[int(ID)][1]:
                        for itemR in payload.split('‖'):
                            message = [ID, itemS, itemR]
                            csvwriter.writerow(message)
        csvfile.closed


    def processT(self):
        '''
        First part of process T: read all inboud messages and add them to Trs
        '''
        inbound_mes = pd.read_csv('nodes/{}/IN.csv'.format(self.nodeID))
        if self.debugging: print(inbound_mes)
        with open("nodes/{}/t.csv".format(self.nodeID), 'a+', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            for key, payload in inbound_mes.iterrows():
                ID = payload['ID']
                count = payload['count']
                nodeRef = payload['sentBy']
                csvwriter.writerow([ID,count, nodeRef])
                self.messageReceived += 1
        csvfile.closed
        '''
        Second Part of process T: calculate all costs
        '''
        self.TableTrs = pd.read_csv('nodes/{}/t.csv'.format(self.nodeID))
        TableTrs_cpy1 = self.TableTrs
        TableTrs_cpy2 = self.TableTrs
        distinct_ids_R = []
        distinct_ids_S = []
        dict_costs = {}
        for key, row in self.TableTrs.iterrows():
            if row['ID'] not in dict_costs:
                dict_costs[row['ID']] = [0,0]
            '''
            Cost broadcast R to S:
            '''
            if row['ID'] not in distinct_ids_R and row['nodeID'][0] == 'R':
                distinct_ids_R.append(row['ID'])

                # Paramether initialization:
                R_all = 0
                R_local = 0
                R_nodes = 0
                S_nodes = 0

                #Broadcast cost
                for key2, row2 in TableTrs_cpy1.iterrows():

                    if row['ID'] == row2['ID'] and row2['nodeID'][0] == 'R':
                        R_all += row2['count']*self.length_R
                        if row['nodeID'] != row2['nodeID']:
                            R_nodes += 1
                        for key3, row3 in TableTrs_cpy2.iterrows():
                            if row2['ID'] == row3['ID'] and row3['nodeID'][0] == 'S' and row2['nodeID'][1] == row3['nodeID'][1] :
                                R_local += row2['count']*self.length_R
                    if row['ID'] == row2['ID'] and row2['nodeID'][0] == 'S':
                        S_nodes += 1
                id = row['ID']

                if self.debugging: print(f'ID: {id}\n R_all: {R_all} \n R_local: {R_local} \n R_nodes: {R_nodes} \n S_nodes: {S_nodes}')
                cost_RS = R_all*S_nodes - R_local + R_nodes*S_nodes*self.messageSize
                dict_costs[row['ID']][0] = cost_RS

            '''
            Cost broadcast S to R:
            '''
            if row['ID'] not in distinct_ids_S and row['nodeID'][0] == 'S':
                distinct_ids_S.append(row['ID'])

                # Paramether initialization:
                S_all = 0
                S_local = 0
                S_nodes = 0
                R_nodes = 0

                # Broadcast cost
                for key2, row2 in TableTrs_cpy1.iterrows():
                    if row['ID'] == row2['ID'] and row2['nodeID'][0] == 'S':
                        S_all += row2['count']*self.length_S
                        if row['nodeID'] != row2['nodeID']:
                            S_nodes += 1
                        for key3, row3 in TableTrs_cpy2.iterrows():
                            if row2['ID'] == row3['ID'] and row3['nodeID'][0] == 'R' and row2['nodeID'][1] == row3['nodeID'][1]:
                                S_local += row2['count']*self.length_S
                    if row['ID'] == row2['ID'] and row2['nodeID'][0] == 'R':
                        R_nodes += 1
                id = row['ID']

                if self.debugging: print(f'ID: {id}\n S_all: {S_all} \n S_local: {S_local} \n S_nodes: {S_nodes} \n R_nodes: {R_nodes}')
                cost_SR = S_all * R_nodes - S_local + R_nodes * S_nodes * self.messageSize
                dict_costs[row['ID']][1] = cost_SR

        total_cost_RS = 0
        total_cost_SR = 0

        optimized_cost = 0

        for key in dict_costs:
            id = key

            total_cost_RS += dict_costs[key][0]
            total_cost_SR += dict_costs[key][1]

            cost_RS = dict_costs[key][0]
            cost_SR = dict_costs[key][1]

            if cost_SR >= cost_RS:
                optimized_cost += cost_RS
                print(f'{id} should be sent from R to S {cost_SR}>={cost_RS}')
            else:
                optimized_cost += cost_SR
                print(f'{id} should be sent from S to R {cost_RS}>{cost_SR}')

        print(f'3-Phase Track join total cost for node {self.nodeID}:\n   - Send R to S: {total_cost_RS}\n   - Send S to R: {total_cost_SR}\n   - Mixed send:  {optimized_cost}')

        self.analyticsWrite('total_cost_RS', total_cost_RS)
        self.analyticsWrite('total_cost_SR', total_cost_SR)
        self.analyticsWrite('optimized_cost', optimized_cost)

        '''
        Third part of process T: Iterate through all distinct key,R_node pairs and for all processes that have the same 
        key but are on S sent the key and the corresponding node S to right node R
        '''
        TableTrs_cpy = self.TableTrs
        for key, row in self.TableTrs.iterrows():
            cost_RS = dict_costs[row['ID']][0]
            cost_SR = dict_costs[row['ID']][1]

            if cost_RS < cost_SR:
                for key2, row2 in TableTrs_cpy.iterrows():
                    if row['nodeID'][0] == 'R' and row2['nodeID'][0] == 'S' and row['ID'] == row2['ID']:
                        self.sendTo_2(shouldSend=row['nodeID'], key=row2['ID'], sendTo=row2['nodeID'])
                        if self.debugging: print(row['nodeID'], 'should send', payload['ID'], 'to', row2['nodeID'])
            else:
                for key2, row2 in TableTrs_cpy.iterrows():
                    if row['nodeID'][0] == 'S' and row2['nodeID'][0] == 'R' and row['ID'] == row2['ID']:
                        self.sendTo_2(shouldSend=row['nodeID'], key=row2['ID'], sendTo=row2['nodeID'])
                        if self.debugging: print(row['nodeID'], 'should send', payload['ID'], 'to', row2['nodeID'])

    def run(self):

        readbuffer = ""
        if self.nodeID == 0: print('    Starting Process R1/S1 - Reading')
        self.processR1()
        self.processS1()
        self.send_1()
        self.queue.put(False)

        time.sleep(1)
        phaseID = 0
        while True:
            if self.debugging: print(self.nodeID, 'waiting for message, completed:', phaseID)
            phaseID = self.queue.get()  # This will block until a message is sent to the queue.
            if isinstance(phaseID, bool):
                self.queue.put(phaseID)
                continue
            print(self.nodeID,'got message', phaseID)

            if phaseID == 1:
                if self.nodeID == 0: print('    Starting Process T')
                self.processT()
                self.send_2()
                self.queue.put(False)
            elif phaseID == 2:
                if self.nodeID == 0: print('    Starting Process R2/S2 - Sending')
                self.processR2S2_send()
                self.queue.put(False)

            elif phaseID == 3:
                with open("nodes/{}/JoinedTable.csv".format(self.nodeID), 'w+', newline='') as csvfile:
                    csvwriter = csv.writer(csvfile)
                    csvwriter.writerow(['ID', 'Family Name', 'First Name'])
                csvfile.closed
                print(self.nodeID, 'Reading inbound messages for Process R and S')
                if self.nodeID == 0: print('    Starting Process R3/S3 - Recieving')
                self.processR3S3_recieve()

                if self.nodeID == 0: print('    Joined all values!')
                self.queue.put(False)
                break

            time.sleep(5)

        print(f'Node {self.nodeID} finished')

