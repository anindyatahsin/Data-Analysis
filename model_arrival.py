import sys
import re

g = open('../VT/VT_2017-11_data.csv', 'r')
f = open('VT_2017-11_data_arrival.csv', 'w')

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

line = g.readline()
lineno = 1
currentTime = ''
count = 1

while line:
    vars = line.split(",")	
    if vars[header['queue']] == '"normal_q"':
        lineno = lineno + 1
        line = g.readline()
        continue
    else:
        print vars[header['queue']]
    #res_list = re.split(":=",vars[header['"Resource_List_nodes"']])
    vars5 = vars[5].replace('"','')

    resources = vars5.split('+')
    total_cores = 0
    total_gpus = 0
    if len(resources) > 1:
        print lineno
    for res_lists in resources:
        res_list = res_lists.split(':')
        gpus = 0;
        #print res_list
        try:
            cores = int(res_list[0])
        except ValueError:
            cores = 1
        if len(res_list) > 1:
            ppn = res_list[1].split('=')[1]
            cores = cores * int(ppn)
        if len(res_list) > 2:
            #print str(lineno) + ': ' + str(res_list)
            gpu = res_list[2].split('=')
            if(len(gpu) == 2):
                gpus = int(gpu[1])
            else:
                gpus = 1
        total_cores = total_cores + cores
        total_gpus = total_gpus + gpus

    val = vars[header['"resources_used_walltime"']].replace('"','').split(':')
    if len(val)> 2:
        walltime = int(val[0]) * 60 * 60 + int(val[1]) * 60 + int(val[2])
    else:
        #print 'no walltime'
        print val
        walltime = 0

    if vars[header['qtime']] == currentTime:
        count = count + 1;
    else:
        toWrite = ''
        toWrite = vars[header['queue']] + ',' + str(total_cores) + ',' + str(total_gpus) + ',' + str(walltime) + ',' + vars[header['qtime']] \
                  + ',' + str(count) + ',\n'

        f.write(toWrite)
        count = 1
        currentTime = vars[header['qtime']]
    #print vars[header['"Resource_List_nodes"']]
    #print vars[res_dic]
    lineno = lineno + 1
    line = g.readline()
    #count = 0
    #break

g.close()
f.close()
