#!/usr/bin/python
import sys,os

def fea2id(infile, outfile, startIdx=1):
    infile = open(infile, 'r')
    idx = startIdx
    for line in infile:
        if len(line.strip().split()) == 0 :
            continue
        fea = line.strip().split()[0]
        outfile.write(fea + "\t" + str(idx) + "\n")
        idx += 1
    return idx

def fea2idWithWeight(infile, outfile, startIdx=1):
    infile = open(infile, 'r')
    idx = startIdx
    for line in infile:
        fea = line.strip().split()[0]
        weight = line.strip().split()[1]
        outfile.write(fea + "\t" + str(idx) + "\t" + weight + "\n")
        idx += 1
    return idx    

if __name__=="__main__":
    fromModle = sys.argv[1]
    flight = sys.argv[2]
    idx = int(sys.argv[3])
    outPath = "fea2id_" + flight + "_" + fromModle
    outfile=open(outPath, 'w')
    conf="conf_" + flight + "_" + fromModle
    if fromModle=="0":
        dirPath = "./"+ flight
        list = os.listdir(dirPath)
        for i in range(0, len(list)):
            filePath = os.path.join(dirPath, list[i])
            idx = fea2id(filePath, outfile, idx)
       
    else:
        filePath = "model." + flight + "." + fromModle
        idx = fea2idWithWeight(filePath, outfile, idx)

    confile=open(conf, 'w')    
    confile.write("feaNum\t" + str(idx) + "\n ")

    outfile.close()
    confile.close()
