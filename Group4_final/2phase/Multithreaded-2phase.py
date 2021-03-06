import names
import pandas as pd
import os
import time
import csv
import random
from multiprocessing import Process, Queue

from node import Node
from AnalysisTool import analysis
num = 3000
max_sameKey = 4
n_nodes = 6
n_phases = 2
keepData = True
#type = 'uniform'
type = 'random'


barrier = []
'''
Data Initialization:
step 1:
 Generate n_nodes amount of r,s,t tables
 Generate num amount of firstname-lastname combinations

step 2:
 Randomly assign to each firstname a table r
 Randomly assign to each lastname a table s

2-Track-Join:
step 1:
 Initialize n_nodes threads (each node is represented by one thread)
 Start 1st phase of algorithm

step 2:
 Gather all outbound messages
 Send to respective thread the message
 
step 3:
 Send start 2nd phase message
 
step 4: 
 Run analytics tool 
'''


def hash(key):
    return (int)((n_nodes / num ) * key)


def initialize_nodes():
    current_directory = os.getcwd()

    for c in range(0, n_nodes):

        try:
            os.mkdir(current_directory + '/nodes/{0}'.format(c))
            print('Created folder for node {}'.format(c))
        except:
            print('Overwritten folder for node {}'.format(c))

    print("{} nodes initialized successfully".format(n_nodes))

    distribution_names = {}
    distribution_surnames = {}
    amount_of_names = 0
    if not keepData:
        for i in range(0, num):
            for c in range(0, random.randrange(max_sameKey)+1):
                amount_of_names += 1
                if type == 'random':
                    '''
                    Random distribution
                    '''
                    name = names.get_first_name()
                    r_loc = random.randrange(n_nodes)
                    if r_loc not in distribution_names:
                        distribution_names[r_loc] = []
                    distribution_names[r_loc].append([i, name])

                    surname = names.get_last_name()
                    s_loc = random.randrange(n_nodes)
                    if s_loc not in distribution_surnames:
                        distribution_surnames[s_loc] = []
                    distribution_surnames[s_loc].append([i, surname])
                else:
                    '''
                    uniform distribution every node gets same amout +/- of data
                    '''
                    while True:
                        name = names.get_first_name()
                        r_loc = random.randrange(n_nodes)
                        if r_loc not in distribution_names:
                            distribution_names[r_loc] = []
                        if len(distribution_names[r_loc]) <= int( max(amount_of_names, num) / n_nodes)+1:
                            distribution_names[r_loc].append([i, name])
                            break
                    while True:
                        surname = names.get_last_name()
                        s_loc = random.randrange(n_nodes)
                        if s_loc not in distribution_surnames:
                            distribution_surnames[s_loc] = []
                        if len(distribution_surnames[s_loc]) <= int( max(amount_of_names, num) / n_nodes)+1:
                            distribution_surnames[s_loc].append([i, surname])
                            break
        print('Amount of Names = ',amount_of_names)

        # write R nodes
        for node in range(0, n_nodes):
            with open("{}/nodes/{}/r.csv".format(current_directory, node), 'w+', newline='') as csvfile:
                time.sleep(0.1)
                csvwriter = csv.writer(csvfile)
                csvwriter.writerow(['ID', 'FirstName'])
                for entry in distribution_names[node]:
                    csvwriter.writerow(entry)

        # write S nodes
        for node in range(0, n_nodes):
            with open("{}/nodes/{}/s.csv".format(current_directory, node), 'w+', newline='') as csvfile:
                time.sleep(0.1)
                csvwriter = csv.writer(csvfile)
                csvwriter.writerow(['ID', 'Surname'])
                for entry in distribution_surnames[node]:
                    csvwriter.writerow(entry)

    # initialize barrier
    for node in range(0, n_nodes):
        with open("{}/nodes/{}/analytics.csv".format(current_directory, node), 'w+', newline='') as csvfile:
            time.sleep(0.1)
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['variable', 'value'])

        with open("{}/nodes/{}/t.csv".format(current_directory, node), 'w+', newline='') as csvfile:
            time.sleep(0.1)
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['ID', 'nodeID'])
        barrier.append(True)
    print('Wrote %d name and surname combinations on %d nodes' % (num, n_nodes))

def dispatchMessages():
    print('Reading all Outbound messages')
    outboud_mes = {}
    for nodeID in range(0, n_nodes):
        messages = pd.read_csv('nodes/{}/OUT.csv'.format(nodeID))
        for key, payload in messages.iterrows():
            if payload['sendTo'] not in outboud_mes:
                outboud_mes[payload['sendTo']] = []
            outboud_mes[payload['sendTo']].append(payload.values.flatten().tolist())
        print('Read all messages from', nodeID)
    print('Dispatching the messages')

    for nodeID in range(0,n_nodes):
        with open("nodes/{}/IN.csv".format(nodeID), 'w+', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['ID', 'sendTo', 'sentBy'])
            for message in outboud_mes[nodeID]:
                csvwriter.writerow(message)
    print('All messages dispatched')

def dispatchMessages_2nd():
    print('Reading all Outbound messages')
    outboud_mes = {}
    for nodeID in range(0, n_nodes):
        messages = pd.read_csv('nodes/{}/OUT.csv'.format(nodeID))

        for key, payload in messages.iterrows():
            if payload['sentBy'] not in outboud_mes:
                outboud_mes[payload['sentBy']] = []
            outboud_mes[payload['sentBy']].append(payload.values.flatten().tolist())
        print('Read all messages from', nodeID)

    print('Dispatching the messages')

    for nodeID in range(0,n_nodes):
        with open("nodes/{}/IN.csv".format(nodeID), 'w+', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['ID', 'sendTo', 'sentBy'])
            for message in outboud_mes['R{}'.format(nodeID)]:
                csvwriter.writerow(message)
        csvfile.closed
    print('All messages dispatched')

def dispatchMessages_3rd():
    print('Reading all outbound messages')
    outboud_mes = {}
    for nodeID in range(0, n_nodes):
        messages = pd.read_csv('nodes/{}/payloadOUT.csv'.format(nodeID))
        for key, payload in messages.iterrows():
            if payload['sendTo'] not in outboud_mes:
                outboud_mes[payload['sendTo']] = []
            outboud_mes[payload['sendTo']].append([payload['ID'], payload['payload']])
        print('Read all messages from', nodeID)

    print('Dispatching the messages')

    for nodeID in range(0,n_nodes):

        with open("nodes/{}/payloadIN.csv".format(nodeID), 'w+', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['ID', 'payload'])
            for message in outboud_mes['S{}'.format(nodeID)]:
                csvwriter.writerow(message)
    print('All messages dispatched')

if __name__ == "__main__":
    print("Initialization phase:")

    initialize_nodes()

    print('Initialization concluded \nStarting %d nodes' % (n_nodes))
    print('\nStarting step 1')
    processes = {}
    for item in range(0, n_nodes):
        try:
            q = Queue()
            process = Node(q, item, hash)
            process.start()
            processes[str(item)] = (q, process)
        except Exception as e:
            print("An error occurred while creating threads.")
            print(str(e))

    time.sleep(0.1)

    phase = 0
    # Wait for all nodes to reach barrier
    while any(barrier):
        for node in range(0, n_nodes):
            if barrier[node]:
                in_msg = processes[str(node)][0].get()
                if not in_msg:
                    barrier[node] = False

    # Dispatch all messages
    dispatchMessages()

    phase = 1
    # Send message to all nodes to start next phase
    print('\nStarting step 1_b')
    for node in range(0, n_nodes):
        processes[str(node)][0].put(phase)  # Sends a message using the queue object
        barrier[node] = True
        time.sleep(0.1)
    time.sleep(1)

    while any(barrier):
        for node in range(0, n_nodes):
            if barrier[node]:
                in_msg = processes[str(node)][0].get()
                if not in_msg:
                    barrier[node] = False

    # Dispatch all messages
    dispatchMessages_2nd()

    phase = 2
    print('\nStarting step 2')
    for node in range(0, n_nodes):
        barrier[node] = True
        processes[str(node)][0].put(phase)  # Sends a message using the queue object
        time.sleep(0.5)

    time.sleep(5)

    while any(barrier):
        for node in range(0, n_nodes):
            if barrier[node]:
                in_msg = processes[str(node)][0].get()
                if not in_msg:
                    barrier[node] = False
    time.sleep(5)

    dispatchMessages_3rd()
    print('\n \n Starting step 3')
    phase = 3
    for node in range(0, n_nodes):
        barrier[node] = True
        processes[str(node)][0].put(phase)  # Sends a message using the queue object
        time.sleep(0.5)

    while any(barrier):
        for node in range(0, n_nodes):
            if barrier[node]:
                in_msg = processes[str(node)][0].get()
                if not in_msg:
                    barrier[node] = False

    print('\n \n Join completed, running analysis Tool (step 4)')
    analysis()