import random
from math import sqrt

means = [0, 6]
data = [random.random() for i in range(10000)]
data.extend([random.random() + 5 for i in range(10000)])
random.shuffle(data)
print len(data)
#print data
print means
print reduce(lambda x,y: x+y, data) / len(data)
param = 0.1
g = [0.5, 0.5]

for i,x in enumerate(data):
    closest_k = 0
    smallest_error = 9999
    for k in enumerate(means):
        error = abs(x-k[1])
        if error < smallest_error:
            smallest_error = error
            closest_k = k[0]
    means[closest_k] = means[closest_k] + (x-means[closest_k]) * param / sqrt(g[closest_k])
    g[closest_k] += (x-means[closest_k]) ** 2
#    print x, means

print means
