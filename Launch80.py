import os
import sys
import time
import datetime
import subprocess
import threading

#os.system('QueryIndexDataDumper.exe')
#os.popen('QueryIndexDataDumper.exe')

configStr = '''
[LogRules]
Rule1=QueryIndexDataDumper,*,*,stdout
Rule2=QueryIndexDataDumper,*,*,LogRule_QueryIndexDataDumper

[LogRule_QueryIndexDataDumper]
FileNameBase=local\QueryIndexDataDumper_Log
MaxFiles=20
MaxFileSize=1000000
BufferSize=10000

[QueryIndexDataDumper]
;build interval count by seconds
BuildInterval=120
;max chunk count for filter
ChunkMaxCount=30
;news classifier query index log pattern
NCFilePathPattern=NewsClassifier_QueryIndex_
;NC query index log dir
NCDir=D:\Data\Logs\Local\\
;wait interval after log is ready
WaitInterval=20
; FilterQueryLength is equal with max chunk count

; BlackFormcodeList
BlackFormcodeList=monitr,monitr2
;WhiteFormcodeList
WhiteFormcodeList=qbnt
;Formcode filter switch, 0 for black form code
FormcodeFilterSwitch=0
;Traffic sample rate
SampleRate=0
;how many appearence time can avoid being trunked
ChunkFilterThreshold=1
;cosmos folder of the upload files
devmachine$CosmosRootFolder=http://cosmos09.osdinfra.net:88/cosmos/Realtime/local/Realtime/QueryIndexDataDumper/Dev/
Answers-Int-Bn1$CosmosRootFolder=http://cosmos09.osdinfra.net:88/cosmos/Realtime/local/Realtime/QueryIndexDataDumper/Int/
CosmosRootFolder=http://cosmos09.osdinfra.net:88/cosmos/Realtime/local/Realtime/QueryIndexDataDumper/Prod/
;local temp file folder
LocalRootFolder=d:\data\NewsQueryIndexDataDumper{id}\\
;regex pattern for query index log
RegexPattern=d,(?<time>.*?),News,(.*?)Market=(?<market>.*?)NormalizedQuery=(?<query>.*?)FormCode=(?<formcode>.*?)
;time out threshold for regex
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

