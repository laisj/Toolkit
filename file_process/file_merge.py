#encode=utf8
import itertools
import datetime
import time
import operator
import collections

iter1 = (line for line in open(r"sort0310queryParam.tsv", "r"))
iter2 = (line for line in open(r"sort0310display.tsv", "r"))
#iter3 = (line for line in open(r"sort0311click.tsv", "r"))

a = next(iter1, None)
b = next(iter2, None)
#c = next(iter3, None)
while a != None and b != None:
    
    if a.split("|")[3] == b.split("|")[2]:
        print a.strip() + "|"+ b.strip()
        a = next(iter1, None)
        b = next(iter2, None)
    elif a.split("|")[3] > b.split("|")[2]:
        b = next(iter2, None)
    else:
        a = next(iter1, None)
