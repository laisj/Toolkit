import random

means = [0, 6]
data = [random.random() for i in range(1000)]
data.extend([random.random() + 5 for i in range(1000)])
random.shuffle(data)
print len(data)
print data
print means
print reduce(lambda x,y: x+y, data) / len(data)
param = 0.1

for i,x in enumerate(data):
    closest_k = 0
    smallest_error = 9999
    for k in enumerate(means):
        error = abs(x-k[1])
        if error < smallest_error:
            smallest_error = error
            closest_k = k[0]
    means[closest_k] = means[closest_k] * (1-param) + x*(param)
    print x, means

print means
