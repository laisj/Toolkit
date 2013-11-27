import os
import sys
import time
import datetime
import subprocess
import threading

#os.system('QueryIndexDataDumper.exe')
#os.popen('QueryIndexDataDumper.exe')

configStr = '''
RegexMaxTimeout=1000
'''

def worker(configId):
	global g_mutex
	print "Str ", configId, datetime.datetime.now()
	print "config: data\\newsqueryindexdatadumper" + str(configId)
	g_mutex.acquire()
	# edit config file
	fp = open(r'D:\sbox\app\NewsQueryIndexDataDumper\QueryIndexDataDumper.ini', "w")
	fp.write(configStr.replace("{id}", str(configId + 180)))
	fp.close()
	threading.Thread(target=worker2, args=()).start()
	time.sleep(2)
	g_mutex.release()
	print "End ", configId, datetime.datetime.now()

def worker2():
	os.system('QueryIndexDataDumper.exe')

if __name__ == "__main__":
	global g_mutex
	g_mutex = threading.Lock()
	thread_pool = []
	for i in xrange(82):
		th = threading.Thread(target=worker, args=(i,))
		thread_pool.append(th)
	for i in xrange(82):
		thread_pool[i].start()

