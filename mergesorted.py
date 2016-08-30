#encode=utf8
import itertools
import datetime
import time
import operator
import collections

iter1 = (line.split("|") for line in open(r"1.tsv", "r"))
iter2 = (line.split("|") for line in open(r"2.tsv", "r"))

a = next(iter1, None)
b = next(iter2, None)
while a!= None and b != None:
    if a[0] == b[0]:
        print a,b
        a = next(iter1, None)
        b = next(iter2, None)
    elif a[0] > b[0]:
        print b
        b = next(iter2, None)
    else:
        print a
        a = next(iter1, None)


