#encode=utf8
import sys
import os
import time

machinedict = {
   'a1' : 'ads-001.m6',
   'a2' : 'ads-002.m6',
   'a3' : 'ads-003.m6',
   'a4' : 'ads-004.m6',
   'a5' : 'ads-005.m6',
   'v1' : 'vm-mts-001.vm',
   'v3' : 'vm-mts-003.vm'
}

def glory_file_down(f, m):
   prefix = str(time.time())
   dest = machinedict[m]
   print "ssh -p 16020 lai.sijia@xxx.m6.xxx.com 'scp -r lai.sijia@"+ dest +":/home/lai.sijia/mirror/" + f + " /home/lai.sijia/mirror/" + prefix+f + " & '"
   os.system("ssh -p 16020 lai.sijia@xxx.m6.xxx.com 'scp -r lai.sijia@"+ dest +":/home/lai.sijia/mirror/" + f + " /home/lai.sijia/mirror/" + prefix+f + " & '")
   print "scp -r -P 16020 lai.sijia@xxx.m6.xxx.com:/home/lai.sijia/mirror/" + prefix+f + " ./" + f
   os.system("scp -r -P 16020 lai.sijia@xxx.m6.xxx.com:/home/lai.sijia/mirror/" + prefix+f + " ./" + f)
   print "ssh -p 16020 lai.sijia@xxx.m6.xxx.com 'rm -rf /home/lai.sijia/mirror/* & '"
   os.system("ssh -p 16020 lai.sijia@xxx.m6.xxx.com 'rm -rf /home/lai.sijia/mirror/* & '")

if len(sys.argv) == 3:
   glory_file_down(sys.argv[1], sys.argv[2])
