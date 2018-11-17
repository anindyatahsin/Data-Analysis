#import hashlib
import sys

f = open('sinfo.txt', 'r')
g = open('standard.csv', 'w')

line = f.readline()

while line:
    vars = line.split()
    if vars[0] == "standard":
        resList = vars[5].split(",");
        #print resList
        for ls in resList:
            g.write(ls + ",standard\n" )
    line = f.readline()

f.close()
g.close()