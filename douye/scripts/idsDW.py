#!/usr/bin/python
import sys,os

def fea2id(infile, outfile, startIdx=1):
    infile = open(infile, 'r')
    idx = startIdx
    for line in infile:
        fea = line.strip()
        outfile.write(fea + "\t" + str(idx) + "\n")
        idx += 1
    return idx

if __name__=="__main__":
    flight = sys.argv[1]
    dfea = "./"+ flight + "_dw/deep"
    wfea = "./"+ flight + "_dw/wide"
    doutPath = "fea2id_" + flight + "_d"
    woutPath = "fea2id_" + flight + "_w"
    conf="conf_" + flight + "_dw"
    doutfile=open(doutPath, 'w')
    woutfile=open(woutPath, 'w')
    confile=open(conf, 'w')
    # 0 indicates null
    idx = 1
    list = os.listdir(dfea)
    for i in range(0, len(list)):
        filePath = os.path.join(dfea, list[i])
        idx = fea2id(filePath, doutfile,idx)
    confile.write("dfea\t" + str(idx) + "\n ")

    idx = 1
    list = os.listdir(wfea)
    for i in range(0, len(list)):
        filePath = os.path.join(wfea, list[i])
        idx = fea2id(filePath, woutfile, idx)
    confile.write("wfea\t" + str(idx) + "\n ")

    doutfile.close()
    woutfile.close()
    confile.close()
