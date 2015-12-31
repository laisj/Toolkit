import datetime
import pexpect
import os

print datetime.datetime.now()

ssh = pexpect.spawn("ssh lai.sijia@gw1.wemomo.com -p 16020")
ssh.expect("000*")
ssh.sendline("python ssh.py")
ssh.interact()
