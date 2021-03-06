import pylab
import numpy as np
import matplotlib.pyplot as plt
from pandas.tools.plotting import scatter_matrix
import pandas as pd
'''
df = pd.DataFrame(np.random.randn(1000, 4), columns=['a', 'b', 'c', 'd'])
print np.random.randn(10, 4)
scatter_matrix(df, alpha=0.5, figsize=(8, 8), diagonal='kde')
plt.show()
'''
import sys
import redis

modelNameOut = sys.argv[0]
r = redis.Redis(host="ads-test-001.dx", port=6303)
# input comes from STDIN (standard input)
for line in sys.stdin:
    line_pair = line.strip().split("|")
    k = line_pair[0]
    v = line_pair[1]
    r.hset("model:" + modelNameOut + ":info", k, v)
    r.publish("reloadLrModel", "1")
print r.hgetall('c')
r.hmset('a', {'123':'\"[1,2]\"'})
r.hmset('aaaa', {'123':'\"[1,2]\"'})
#print r.dump('a')
print r.keys(pattern='a*')
#print r.append('a', {'b':'c'})
print r.hgetall('a')
print r.hget('a','123')
print r.hget('aaaa','123')

r.hmset('a', {'123':'\"[1,2]\"'})
r.hset('a', '123', '456')
print r.hgetall('a')
