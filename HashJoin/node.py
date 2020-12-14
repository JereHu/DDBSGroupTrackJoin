import os
import time
from multiprocessing import Queue, Process
import pandas as pd
import csv
from logging import debug


class Node(Process):

    def __init__(self, q, nodeID, hashing_fun):
        super().__init__()

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
        
        MessageSentOUT_S = Amount of messages sent from S out of the node during S1
        MessageSentIN_S = Amount of messages sent from S to R in the same node
        
        MessageSentOUT_S = Amount of messages sent from R out of the node during R1
        MessageSentIN_S = Amount of messages sent from R to S in the same node
        
        messageOut = list containing all messages that will be sent at the end of the respective phases
        '''
        self.length_R = 1
        self.length_S = 1
        self.messageSize = 0
        self.debugging = False

        self.queue = q

        self.nodeID = nodeID
        self.hashing_fun = hashing_fun

        self.TableR = pd.read_csv('nodes/{}/r.csv'.format(nodeID))
        self.TableS = pd.read_csv('nodes/{}/s.csv'.format(nodeID))
        self.TableT = {}

        self.messageSentOUT_S = 0
        self.messageSentIN_S = 0

        self.messageSentOUT_R = 0
        self.messageSentIN_R = 0

        # messages sent out during first phase
        self.messageOUT = []

        print('Successfully started node', self.nodeID, 'on', self)

    def analyticsWrite(self, name, value):
        '''
        Adds value to the analytics file of the node
        :param name: name of the value
        :param value: value
        '''
        with open("nodes/{}/analytics.csv".format(self.nodeID), 'a+', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow([name, value])
        csvfile.closed

    def processR1(self):
        for key, payload in self.TableR.iterrows():
            hashed_key = self.hashing_fun(payload['ID'])
            if hashed_key == self.nodeID:
                if payload['ID'] not in self.TableT:
                    self.TableT[payload['ID']] = {'R': [], 'S': []}
                self.TableT[payload['ID']]['R'].extend(payload.values.flatten().tolist()[1:])
                self.messageSentIN_R += 1
            else:
                self.messageOUT.append(['R', hashed_key, '‖'.join( [str(x) for x in payload.values.flatten().tolist()])])
                self.messageSentOUT_R += 1

    def processS1(self):
        for key, payload in self.TableS.iterrows():
            hashed_key = self.hashing_fun(payload['ID'])
            if hashed_key == self.nodeID:
                if payload['ID'] not in self.TableT:
                    self.TableT[payload['ID']] = {'R': [], 'S': []}
                self.TableT[payload['ID']]['S'].extend(payload.values.flatten().tolist()[1:])
                self.messageSentIN_S += 1
            else:
                self.messageOUT.append(['S', hashed_key, '‖'.join( [str(x) for x in payload.values.flatten().tolist()])])
                self.messageSentOUT_S += 1

    '''
    Writes all messages contained in the self.messageOUT to csv file
    and then adds the amount of messages sent to the analyticsFile
    '''
    def send(self):
        if self.debugging: print('nodeID, message OUT:', self.nodeID, self.messageOUT)
        with open("nodes/{}/OUT.csv".format(self.nodeID), 'w+', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['table', 'sendTo', 'payload'])
            for message in self.messageOUT:
                csvwriter.writerow(message)

        csvfile.closed
        self.analyticsWrite('message_sent_out_R', self.messageSentOUT_R)
        self.analyticsWrite('message_sent_in_R', self.messageSentIN_R)

        self.analyticsWrite('message_sent_out_S', self.messageSentOUT_S)
        self.analyticsWrite('message_sent_in_S', self.messageSentIN_S)

        self.messageOUT = []

    '''
    Reads all inbound messages and then joins all the values into the JoinedTable.csv
    '''
    def processT(self):
        inbound_mes = pd.read_csv('nodes/{}/IN.csv'.format(self.nodeID))
        for key, message in inbound_mes.iterrows():
            table = message['table']
            payload = message['payload'].split('‖')
            ID = int(payload[0])
            if ID not in self.TableT:
                self.TableT[ID] = {'R': [], 'S': []}
            if table == 'S':
                self.TableT[ID]['S'].extend(payload[1:])

            else:
                self.TableT[ID]['R'].extend(payload[1:])

        with open("nodes/{}/JoinedTable.csv".format(self.nodeID), 'w+', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['ID', 'Name', 'Surname'])
            for key in self.TableT:
                for name in self.TableT[key]['R']:
                    for surname in self.TableT[key]['S']:
                        csvwriter.writerow([key, name, surname])
        csvfile.closed

    def run(self):

        # send all payloads
        self.processR1()
        self.processS1()
        self.send()
        self.queue.put(False)

        time.sleep(1)

        while True:
            phaseID = self.queue.get()  # This will block until a message is sent to the queue.
            if isinstance(phaseID, bool):
                self.queue.put(phaseID)
                continue
            if phaseID == 1:
                print(self.nodeID, 'Joining')
                self.processT()
                self.queue.put(False)

                break

            time.sleep(1)

        print('done')
