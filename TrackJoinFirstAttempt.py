import pandas as pd
import numpy as np
import csv
from collections import defaultdict

TableR = pd.read_csv("TableR.csv")
TableS = pd.read_csv("TableS.csv")
TableT = {}
TableOut = open('TableOut.csv', 'w', newline='')

for person in TableR["Name"]:
    TableT[person] = TableR[TableR["Name"] == person]




def hashJoin(table1, index1, table2, index2):
    h = defaultdict(list)
    # hash phase
    for s in table1:
        h[s[index1]].append(s)
    # join phase
    return [(s, r) for r in table2 for s in h[r[index2]]]
 

 
table1 = [(27, "Jonah"),
          (18, "Alan"),
          (28, "Glory"),
          (18, "Popeye"),
          (28, "Alan")]
table2 = [("Jonah", "Whales"),
          ("Jonah", "Spiders"),
          ("Alan", "Ghosts"),
          ("Alan", "Zombies"),
          ("Glory", "Buffy")]


 
'''for row in hashJoin(table1, 1, table2, 0):
    print(row)
    '''


'''
let A = the first input table (or ideally, the larger one)
let B = the second input table (or ideally, the smaller one)
let jA = the join column ID of table A
let jB = the join column ID of table B
let MB = a multimap for mapping from single values to multiple rows of table B (starts out empty)
let C = the output table (starts out empty)

for each row b in table B:
   place b in multimap MB under key b(jB)

for each row a in table A:
   for each row b in multimap MB under key a(jA):
      let c = the concatenation of row a and row b
      place row c in table C
'''