import names
import pandas as pd
import os
import time
import csv
import random
from multiprocessing import Process, Queue


from AnalysisTool import analysis
from node import Node

'''
Num = Number of unique keys
max_sameKey = maximum amount of tuples with the same key, it will generate a randoma amount between 1 and max_sameKey
n_nodes = number of nodes

keepData = if True it won't generate new tuples and just use the one already present
type = uniform -> there are no duplicates, wrong naming 
'''

num = 3000
max_sameKey = 2
n_nodes = 6

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
 initialize table t and analytics.csv

3-Track-Join:
step 1:
 Initialize n_nodes threads (each node is represented by one thread)
 Start 1st phase of algorithm

step 2:
 Gather all outbound messages
 calculate cost
 Send the command to send the payload to respective R or S nodes

step 3:
 Send start 2nd phase message + Join tables

step 4: 
 Gather all joined Tables 
'''


def hash(key):
    return (int)((n_nodes / num) * key)


def initialize_nodes():
    current_directory = os.getcwd()
    print("step 1")
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
    print("step 2")
    # initialize barrier
    for node in range(0, n_nodes):
        with open("{}/nodes/{}/analytics.csv".format(current_directory, node), 'w+', newline='') as csvfile:
            time.sleep(0.1)
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['variable', 'value'])

        with open("{}/nodes/{}/t.csv".format(current_directory, node), 'w+', newline='') as csvfile:
            time.sleep(0.1)
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['ID', 'count', 'nodeID'])
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

    for nodeID in range(0, n_nodes):
        with open("nodes/{}/IN.csv".format(nodeID), 'w+', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['table', 'sendTo', 'payload'])
            for message in outboud_mes[nodeID]:
                csvwriter.writerow(message)
    print('All messages dispatched')


if __name__ == "__main__":
    print("Initialization phase:")

    initialize_nodes()

    print('Initialization concluded \nStarting %d nodes' % (n_nodes))
    '''
    Starting the Join algorithm
    '''
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

    # All nodes should send the data to the right node
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
    print('Trying to start phase', phase)
    for node in range(0, n_nodes):
        processes[str(node)][0].put(phase)  # Sends a message using the queue object
        barrier[node] = True
        time.sleep(0.1)
    time.sleep(5)

    while any(barrier):
        for node in range(0, n_nodes):
            if barrier[node]:
                in_msg = processes[str(node)][0].get()
                if not in_msg:
                    barrier[node] = False
    
    print('Join completed, running analysis Tool \n')
    analysis(n_nodes)


