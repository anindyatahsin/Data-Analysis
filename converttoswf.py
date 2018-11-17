import sys
import time
import random
import datetime as dt



def convert( astr ):
    #print astr
    newStr = astr.split(':')
    day = newStr[0].split('-')
    if(len(day) > 1):
        sec = int(day[0]) * 24 * 3600 + int(day[1]) * 3600 + int(newStr[1]) * 60 + int(newStr[2])
    else:
        sec = int(newStr[0]) * 3600 + int(newStr[1]) * 60 + int(newStr[2])
    #print astr + " " + str(sec)
    return sec

g = open("D:/Data/Final/IU_2017-10-12_data-10.csv", 'r')

line = g.readline()
#print line
vars = [x.strip() for x in line.split(',')]
#vars = line.split(",")
header = {}
i = 0
for var in vars:
    header[var] = i
    i = i + 1
print header
g.close()


f = open("convertswf.csv", 'r')
line = f.readline()
#print line
#vars = line.split(",")
swfheader = {}
i = 0
while line:
    #print line
    vars = [x.strip() for x in line.split(',')]
    #print len(vars)
    if(header.has_key(vars[1])):
        swfheader[vars[0]] = header[vars[1]]
    else:
        swfheader[vars[0]] = ''
    i = i + 1
    line = f.readline()
print swfheader

f.close()
# CHANGE
''' 
set the value of REPEAT to add additional loads to the system. A value of 0 denotes no additional load. A value of x means 1 in every
x job will be repeated, so an additional load of (100/x)% is added 
'''
REPEAT = 0
f = open("D:/PHD/alea-master/uva-data.swf-40", 'w')
g = open("D:/Data/Final/V3/UVA_2017-10-12_data-40-v3.csv", 'r')
line = g.readline()
    
vars = [x.strip() for x in line.split(',')]
#vars = line.split(",")
nheader = {}
i = 0
for var in vars:
    nheader[i] = var
    i = i + 1
print nheader

lno = 1
lc = REPEAT
line = g.readline()
rep = 0
#start_dt = dt.datetime.strptime("8/4/2017  7:46:00 PM", '%m/%d/%Y %I:%M:%S %p')
#print "returned tuple: %s " % start_dt
while line:
    if lc == REPEAT & REPEAT != 0:
        lc = 0
        rep = random.randint(1,REPEAT-1)
        #print rep
    vars = [x.strip() for x in line.split(',')]

    # UVA ram is given a K or M, for VTech and IU it is given as kb, mb
    if(vars[5] != ''):
        #CHANGE
        vars[5] = vars[5][:-1] # 2 for kb / mb
    else:
        vars[5] = -1

    # CHANGE
    #VTech qtime format 10/01/2017 00:00
    #end_dt = dt.datetime.strptime(vars[8], '%m/%d/%Y %H:%M').timetuple()
    # UVA qtime format 2017-12-01T00:00:01
    arrival = vars[8].split('T')
    end_dt = dt.datetime.strptime('{0} {1}'.format(arrival[0], arrival[1]), '%Y-%m-%d %H:%M:%S').timetuple()

    epoch_time = time.mktime(end_dt)
    # IU qtime is in epoch time, so no need to convert
    #epoch_time = long(vars[8])

    begin_date = dt.datetime.strptime('2017-10-01 00:00:01', '%Y-%m-%d %H:%M:%S').timetuple()

    #print epoch_time
    #diff = (end_dt - start_dt)
    # Oct 1506830401
    # Nov 1509508801
    # Dec 1512104401
    # 1502833902
    # CHANGE
    if (epoch_time <= 1506830401):
        epoch_time = time.mktime(begin_date)
        #epoch_time = 1506830401
    # CHANGE
    vars[8] = str(epoch_time)[:-2] # for IU don't truncate the last two digits
    #vars[8] = str(epoch_time)
    prop = '-1'
    if(vars[6] != ''):
        node = vars[6].split(':')
        ppn = ['ppn', 1]
        if(len(node) == 2):
            ppn = node[1].split('=')
            try:
                vars[6] = int(node[0]) * int(ppn[1])
            except ValueError:
                vars[6] = 1
        elif(len(node) > 2):
            ppn = node[1].split('=')
            try:
                vars[6] = int(node[0]) * int(ppn[1])
            except ValueError:
                vars[6] = 1
            prop = node[2]
    else:
        #print lno
        nodelist = vars[15].split('+')
        vars[6] = len(nodelist)
        nd = {}
        for i in range(0, len(nodelist)):
            procs = nodelist[i].split('/')
            if(procs[0] in nd):
                nd[procs[0]] = nd[procs[0]] + 1
            else:
                nd[procs[0]] = 1

        node[0] = len(nd)
        ppn[1] = int(vars[6]) / len(nd)
        #print str(node[0]) + ' ' + str(ppn[1]) + ' ' + str(vars[5])
        #print nd
        #break
    try:
        vars[13] = convert(vars[13])
        vars[14] = convert(vars[14])
    except ValueError:
        vars[13] = 0
        vars[14] = 0
    array_size = int(vars[16])
    for j in range(0,array_size):
        towrite = str(lno) + '\t'
        for i in range (1,16):
            #print swfheader[0]
            if(swfheader[str(i)] != ''):
                towrite = towrite + str(vars[swfheader[str(i)]]) + '\t'
            else:
                towrite = towrite + '-1\t'
        # CHANGE
        towrite = towrite + str(node[0]) + '\t' + str(ppn[1]) + '\t' + 'uva\t' + prop
        towrite = towrite + '\n'
        f.write(towrite)
        lno = lno + 1
    #print str(lno)  + ' ' + str(vars[4])
    lc = lc + 1
    if(lc == rep):
        continue;
    line = g.readline()
    #print towrite
f.close()
g.close()

