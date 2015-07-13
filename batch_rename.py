#encoding=utf8
"""
Created on Fri Mar 28 00:59:04 2014

@author: sijla
"""

import os
fpath = r'd:/temp/V2withNC/'
for sfile in os.listdir(fpath):
    if os.path.isfile(os.path.join(fpath, sfile)) == True:
        if sfile.find("V2withNC") >= 0 :
            newname = sfile.replace("V2withNC", "nnc")
            os.rename(os.path.join(fpath, sfile), os.path.join(fpath, newname))
            print sfile, newname, "ok"
            
