import sys
import os

def my_read_all(dir):
        for files in os.listdir(dir):
                filepath = os.path.join(dir, files)
                if(os.path.isdir(filepath)):
                        my_read_all(filepath)
                        return
                fp = open(filepath, "r")
                print "### in file", filepath, " ###"
                for line in fp:
                        sys.stdout.write( "%s" % line)

my_read_all("/home/lai/git/Toolkit")
