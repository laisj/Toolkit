#encode=utf8
__author__ = 'sijia.lai'

import gzip
import StringIO
import json
import logging
c = open('1.txt.gz', 'rb').read()
print c
compresseddata = c
compressedstream = StringIO.StringIO(compresseddata)
gzipper = gzip.GzipFile(fileobj=compressedstream)
data = gzipper.read()
#print data
col_map = json.JSONDecoder().decode(data.decode("gbk"))
print col_map["depDate"]
for cur_line in col_map["data"]:
    # print cur_line
    print cur_line["flightNo"]
    print cur_line["price"]
    print cur_line["depTime"]
    print cur_line["arrTime"]

def load_compressed_data(compressed_data):
    try:
        compressedstream = StringIO.StringIO(compresseddata)
        gzipper = gzip.GzipFile(fileobj=compressedstream)
        data = gzipper.read()
        #col_map = json.JSONDecoder().decode(data.decode("gbk"))
        twell_map = json.JSONDecoder().decode(data)
        return twell_map
    except Exception:
        logging.error("load twell compressed data error.")
        return None

