# -*- coding utf-8 -*-
import urllib
import sys
 
if __name__ == '__main__':
    str = "??"
    print str
    params = {}
    params['name'] = str.encode("UTF-8")
    print urllib.urlencode(params)
