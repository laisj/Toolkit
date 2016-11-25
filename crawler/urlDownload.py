import urllib2

#url = 'http://www.guoyi360.com/zp/sgzp/1316.html' 
headers1 = {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6)Gecko/20091201 Firefox/3.5.6'}
req = urllib2.Request(url = 'http://www.guoyi360.com/zp/sgzp/1316.html',headers = headers1)
page=urllib2.urlopen(req).read()
print page