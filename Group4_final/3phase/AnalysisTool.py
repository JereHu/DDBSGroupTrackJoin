import names
import pandas as pd
import os
import time
import csv
import random
from multiprocessing import Process, Queue


from node import Node

def analysis(n_nodes):
    cost_2_phaseRS = 0
    cost_2_phaseSR = 0
    cost_3_phase = 0
    message_sent_out_1 = 0
    message_sent_in_1 = 0
    message_sent_out_2 = 0
    message_sent_in_2 = 0
    payloads_IN_R = 0
    payloads_OUT_R = 0
    payloads_IN_S = 0
    payloads_OUT_S = 0

    for i in range(n_nodes):
        values = pd.read_csv('nodes/{}/analytics.csv'.format(i))
        cost_2_phaseRS += int(values.loc[values.variable == 'total_cost_RS', 'value'])
        cost_2_phaseSR += int(values.loc[values.variable == 'total_cost_SR', 'value'])
        cost_3_phase += int(values.loc[values.variable == 'optimized_cost', 'value'])

        message_sent_out_1 += int(values.loc[values.variable == 'message_sent_out_1', 'value'])
        message_sent_in_1 += int(values.loc[values.variable == 'message_sent_in_1', 'value'])

        message_sent_out_2 += int(values.loc[values.variable == 'message_sent_out_2', 'value'])
        message_sent_in_2 += int(values.loc[values.variable == 'message_sent_in_2', 'value'])

        payloads_IN_S += int(values.loc[values.variable == 'payloads_IN_S', 'value'])
        payloads_OUT_S += int(values.loc[values.variable == 'payloads_OUT_S', 'value'])

        payloads_IN_R += int(values.loc[values.variable == 'payloads_IN_R', 'value'])
        payloads_OUT_R += int(values.loc[values.variable == 'payloads_OUT_R', 'value'])

    print(f'Cost 2-Phase RS: {cost_2_phaseRS}\nCost 2-Phase SR: {cost_2_phaseSR}\nCost 3-Phase RS: {cost_3_phase}\n')
    print(f'Phase 1:\nMessage out 1: {message_sent_out_1}\nMessage in 1: {message_sent_in_1}\n')
    print(f'Phase 2:\nMessage out 1: {message_sent_out_2}\nMessage in 1: {message_sent_in_2}\n')
    print(f'Phase 3:\nPayload out S: {payloads_OUT_S}\nPayload in S: {payloads_IN_S}\n')
    print(f'Phase 3:\nPayload out R: {payloads_OUT_R}\nPayload in R: {payloads_IN_R}\n')


if __name__ == "__main__":
    analysis(6)