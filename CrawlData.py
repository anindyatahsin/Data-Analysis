import glob
import sys
import time
import datetime as dt

schools =["IU", "VT", "UVA"]
algo = [5,25]
perc = [10,20,30,40]
#perc = [30,40]
directories = [None] * 24
util = [] #8
value = [] #20
wait = [] #11
hp = [] #22
mp = [] #23
lp = [] #24

ind = 0
for i in schools:
    for j in perc:
        for k in algo:
             directories[ind] = "../v3/" + i+"-"+str(j)+"-"+str(k)
             g = open(directories[ind] + "/Results(ResultBasic-Estim)-" + str(j) + ".csv", 'r')
             line = g.readline()
             line = g.readline()
             vars = [x.strip() for x in line.split(',')]
             util.append(float(vars[8]))
             wait.append(float(vars[11]))
             value.append(float(vars[20]))
             hp.append(float(vars[22]))
             mp.append(float(vars[23]))
             lp.append(float(vars[24]))
             #print i + "\t" + str(j) + "\t" + str(k) + "\t" + str(util[ind]) + "\t" + str(wait[ind]) + "\t" + str(value[ind]) + "\t" + str(hp[ind]) + "\t" +  str(mp[ind]) + "\t" + str(lp[ind])
             ind = ind + 1
             g.close()

#print(directories)

for j in perc:
    for k in algo:
        #print str(k) + "\t" + str(j)
        filepath = "../v3/CCC-" + str(j) + "-" + str(k) + "/ccc-*/*/jobs_" + str(k) + "_ccc-data.swf_estim_bez.csv"
        #print filepath
        txt = glob.glob(filepath)
        #print txt
        for textfile in txt:
            valDic = {}
            costDic = {"iu":0, "vt":0, "uva":0}
            priDic = {}
            wait = {}
            g = open(textfile, 'r')
            line = g.readline()
            lnno = 0

            while line:
                line = g.readline()
                if len(line.strip()) == 0:
                    break;
                vars = [x.strip() for x in line.split(',')]
                #print str(lnno) + " " + line

                if vars[7] in valDic:
                    valDic[vars[7]] = valDic[vars[7]] + float(vars[8]) * 0.024
                else:
                    valDic[vars[7]] = float(vars[8]) * 0.024
                lnno = lnno + 1

                if vars[7] != vars[9]:
                    costDic[vars[7]] = costDic[vars[7]] -  float(vars[8]) * .024
                    costDic[vars[9]] = costDic[vars[9]] + float(vars[8]) * .024
            print str(k) + "\t" + str(j)
            print "---------------------------------------------------"
            print valDic
            print costDic
            print "---------------------------------------------------"
            g.close()

'''
for k in algo:
    for j in perc:
        filepath = "utilz/util-" + str(j) + "_5_25-" + str(k)
        g = open(filepath, 'r')
        line = ""
        util = {}
        iu = {"bigred2":11008, "bigred2-gpu":10816}
        uva = {"parallel0":4800, "largemem":80, "gpu":392, "knl":512}
        vt = {"Blueridge0":2080,"Blueridge1":64, "Blueridge2":4096, "Blueridge3":288, "Cascades0":144, "Cascades1":128,"Cascades2":6080,
              "DragonsTooth":1152, "NewRiver0":120, "NewRiver1":1092, "NewRiver2":192, "NewRiver3":384, "NewRiver4":2400}
        iuval = 0
        vtval = 0
        uvaval = 0
        while True:
            line1 = g.readline()
            #vars = [x.strip() for x in line.split(',')]
            line2 = g.readline()
            #vars2 = [x.strip() for x in line.split(',')]
            if len(line1.strip()) == 0:
                break;
            util[line1[:-1]] = float(line2[:-3])
            if line1[:-1] in iu :
                iuval = iuval + iu[line1[:-1]] * util[line1[:-1]] / 100
            elif line1[:-1] in uva:
                uvaval = uvaval + uva[line1[:-1]] * util[line1[:-1]] / 100
            elif line1[:-1] in vt:
                vtval = vtval + vt[line1[:-1]] * util[line1[:-1]] / 100
            else:
                print line1[:-1]
        g.close()
        print str(k) + "\t\t" + str(j)
        print "--------------------------------------------------"
        print 'uva\t' + str(uvaval / (4800 + 80 + 392 + 512) * 100 )
        print 'iu\t' + str(iuval / (11008 + 10816) * 100)
        print 'vt\t' + str(vtval / 18220 * 100)

        g.close()
'''