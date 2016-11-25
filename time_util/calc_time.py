import sys
import os
import time
 
srcdir = r'd:\\si\\'
desdir = r'd:\\temp3\\'
 
dayStr = time.strftime("%Y-%m-%d", time.localtime())
dayTime = time.mktime(time.strptime(dayStr,"%Y-%m-%d"))
seedTimeStr = time.strftime("%Y-%m-%dT%H-%M", time.localtime(dayTime))
 
loopTime = dayTime
for i in xrange(0, 24*4*90):
    seedTimeStr = time.strftime("%Y-%m-%dT%H-%M", time.localtime(loopTime))
    for file in os.listdir(srcdir):
        os.system("copy %s %s" % (file, desdir + seedTimeStr+file))
    loopTime = loopTime - 15*60
 
print seedTimeStr
