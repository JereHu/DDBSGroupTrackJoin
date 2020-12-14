import names
import pandas as pd
import os
import time
import csv
import random
from multiprocessing import Process, Queue

from node import Node

def analysis():

    cost_2_phaseRS = 0
    cost_2_phaseSR = 0
    cost_3_phase = 0
    message_sent_out_1 = 0
    message_sent_in_1 = 0
    message_sent_out_2 = 0
    message_sent_in_2 = 0
    payloads_IN = 0
    payloads_OUT = 0

    for i in range(6):
        values = pd.read_csv('nodes/{}/analytics.csv'.format(i))

        message_sent_out_1 += int(values.loc[values.variable == 'message_sent_out_1', 'value'])
        message_sent_in_1 += int(values.loc[values.variable == 'message_sent_in_1', 'value'])


        payloads_IN += int(values.loc[values.variable == 'payloads_IN', 'value'])
        payloads_OUT += int(values.loc[values.variable == 'payloads_OUT', 'value'])

    print(f'Phase 1:\nMessage out 1: {message_sent_out_1}\nMessage in 1: {message_sent_in_1}\n')
    print(f'Phase 2:\nPayload out 1: {payloads_OUT}\nPayload in 1: {payloads_IN}\n')
