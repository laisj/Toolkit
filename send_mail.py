#encoding=utf8
import smtplib
from email.mime.text import MIMEText
from email.header import Header

sender = 'laisijia163@163.com'
receivers = ['laisijia163@163.com']  # 接收邮件，可设置为你的QQ邮箱或者其他邮箱

# 三个参数：第一个为文本内容，第二个 plain 设置文本格式，第三个 utf-8 设置编码
message = MIMEText('头部广告主', 'plain', 'utf-8')
message['From'] = Header("CPM", 'utf-8')
message['To'] =  Header("CPC", 'utf-8')

subject = 'header ads'
message['Subject'] = Header(subject, 'utf-8')

server = smtplib.SMTP('smtp.163.com')
server.starttls()
server.login('laisijia163','pswd')
server.sendmail(sender, receivers, message.as_string())
