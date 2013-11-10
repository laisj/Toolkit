#encoding=utf8
import sys
import re
import urllib2
import webbrowser
import time
urlPattern = '{destUrl}'
html_src = urllib2.urlopen(urlPattern + '/00/'+ sys.argv[1] +'.html').read()
html_src_gb = html_src.decode('gb2312',"ignore")
print html_src_gb
p = re.compile("/p2p/"+sys.argv[1]+"/\d\d-\d\d-\d\d-\d\d-\d\d-\d\d\.html")
usefulList = p.findall(html_src)
finalList = []
for urliter in usefulList:
    print urliter
    time.sleep(1)
    try:
        final_html_src = urllib2.urlopen(urlPattern +urliter, timeout=20).read()
    except:
        continue
    else:
        final_html_src = final_html_src.decode('gb2312','ignore')
        #print final_html_src
        pp = re.compile("{destQuery}".decode('utf8','ignore'))
        boduolist = pp.findall(final_html_src)
        if(len(boduolist) > 0):
            print final_html_src
            time.sleep(1)
            webbrowser.open_new_tab(urlPattern+urliter)
            finalList.append(urlPattern+urliter)

for urliter in finalList:
    print urliter
