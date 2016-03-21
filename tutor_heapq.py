#encoding=utf8
import sys
reload(sys)
sys.setdefaultencoding("utf8")
import os
import collections
import json
import heapq

datadir = "./data/"

feeddict = collections.defaultdict(str)
groupdict = collections.defaultdict(str)
topicdict = collections.defaultdict(str)

for line in open(datadir + "feedsel.tsv", "r"):
    linearr = line.strip().split("\t")
    feeddict[linearr[0][1:-1]] += linearr[1]

for line in open(datadir + "groupsel.tsv", "r"):
    linearr = line.strip().split("\t")
    mmidmap = json.loads(linearr[1][1:-1])

    for k,v in mmidmap.items():
        groupdict[k] += linearr[0]

for line in open(datadir + "topictst2.tsv", "r"):
    linearr = line.strip().split("\t")
    mmids = json.loads(linearr[1][1:-1])

    for k in mmids:
        topicdict[k] += linearr[0]

dictlist = [feeddict, groupdict, topicdict]
batchx = [(k+v for k,v in sorted(d.items(), key=lambda x:x[0])) for d in dictlist]
merged = heapq.merge(*batchx)
for m in merged:
    print m
