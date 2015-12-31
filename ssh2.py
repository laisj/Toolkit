import datetime
import os

print datetime.datetime.now()
m_list = ['bastion001.m6', 'bastion002.m6', 'ads-001.m6', 'ads-002.m6', 'ads-0033.m6', 'ads-004.m6', 'vm-mts-001.vm', 'vm-mts-003.vm']

for i,m in enumerate(m_list):
    print str(i) + ":" + m

m_index = int(raw_input("machine name:\n"))
print "ssh " + m_list[m_index]
os.system("ssh " + m_list[m_index])
