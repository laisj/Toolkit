import datetime
import os

print datetime.datetime.now()
m_list = ['x.vm']

for i,m in enumerate(m_list):
    print str(i) + ":" + m

m_index = int(raw_input("machine name:\n"))
print "ssh " + m_list[m_index]
os.system("ssh " + m_list[m_index])
