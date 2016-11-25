import os
import sys
import time
j=0
i=0
while (True):
	fp = open("NewsClassifier_QueryIndex_" + str(j) + ".log", "w")
	i=0
	while (i < 159188):
		fp.write( r'd,' + time.strftime('%m/%d/%Y %H:%M:%S', time.localtime()) + ',News,QueryIndex,Pid="2240" Tid="7800" TS="0x01CEE10E763D0474" String1="Market=en-us	NormalizedQuery=' + time.strftime('%m/%d/%Y %H:%M:%S', time.localtime()) + '	FormCode="\n')
		time.sleep (0.02)
		i+=1
		fp.flush()
	fp.close()
	j += 1
	time.sleep(1)

#d,11/13/2013 23:52:26,News,QueryIndex,Pid="2240" Tid="7800" TS="0x01CEE10E763D0474" String1="Market=en-us	NormalizedQuery=cerita panas memek nenekku	FormCode="
