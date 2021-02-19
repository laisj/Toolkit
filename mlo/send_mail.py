#encoding=utf8
import smtplib
from configparser import ConfigParser
from email.mime.text import MIMEText
from email.header import Header

cfg = ConfigParser()
cfg.read("/Users/lai/git/Toolkit/mlo/mailconf.ini")

# use qq
mailsection = "qq"
print cfg.sections()
sender = cfg.get(mailsection, "addr")
receivers = [cfg.get(mailsection, "addr")]  # 接收邮件，可设置为你的QQ邮箱或者其他邮箱

# 三个参数：第一个为文本内容，第二个 plain 设置文本格式，第三个 utf-8 设置编码
message = MIMEText(cfg.get(mailsection, "passphrase"), 'plain', 'utf-8')
#message['From'] = Header("CPM", 'utf-8')
#message['To'] =  Header("CPC", 'utf-8')

subject = 'conf test'
message['Subject'] = Header(subject, 'utf-8')

server = smtplib.SMTP(cfg.get(mailsection, "smtp"))
server.starttls()
server.login(cfg.get(mailsection, "name"), cfg.get(mailsection, "passphrase"))
server.sendmail(sender, receivers, message.as_string())
