import time
#import datetime

print time.time()
print time.localtime()
a = time.strptime('2013-10-22', '%Y-%m-%d')
print time.strftime('%m/%d/%Y', a)
c = time.mktime(a)
print c
c += 86400
d = time.localtime(c)
print time.strftime('%d//%m//%Y', d)
