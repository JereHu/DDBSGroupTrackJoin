
import pandas as pd


def analysis(n_nodes):

    payloads_IN_S = 0
    payloads_OUT_S = 0
    payloads_IN_R= 0
    payloads_OUT_R = 0

    for i in range(n_nodes):
        values = pd.read_csv('nodes/{}/analytics.csv'.format(i))

        payloads_OUT_S += int(values.loc[values.variable == 'message_sent_out_S', 'value'])
        payloads_IN_S += int(values.loc[values.variable == 'message_sent_in_S', 'value'])

        payloads_OUT_R += int(values.loc[values.variable == 'message_sent_out_R', 'value'])
        payloads_IN_R += int(values.loc[values.variable == 'message_sent_in_R', 'value'])


    print(f'Phase 1:\nMessage out R: {payloads_OUT_R}\nMessage in R: {payloads_IN_R}\nMessage out S: {payloads_OUT_S}\nMessage in S: {payloads_IN_S}\n')

