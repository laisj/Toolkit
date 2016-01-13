import datetime
import pexpect
import os

print datetime.datetime.now()

ssh = pexpect.spawn("ssh lai.sijia@gxxxxxx -p xxx")
ssh.setwinsize(40, 160)
ssh.expect("000*")
ssh.sendline("python ssh.py")
ssh.interact()
