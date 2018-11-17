import sys
import re

g = open('../IUDataFormatting/IU-DATA-Oct-2017.csv', 'r')
f = open('IU_2017-10_data_summary.csv', 'w')

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
while line:
    vars = line.split(",")
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

    val = vars[header['resources_used.walltime']].replace('"','').split(':')
    if len(val)> 2:
        walltime = int(val[0]) * 60 * 60 + int(val[1]) * 60 + int(val[2])
    else:
        print 'no walltime'
        print val
        walltime = 0
    toWrite = ''
    toWrite = vars[header['queue']] + ',' + str(total_cores) + ',' + str(total_gpus) + ',' + str(walltime) + ',\n'

    f.write(toWrite)
    #print vars[header['"Resource_List_nodes"']]
    #print vars[res_dic]
    lineno = lineno + 1
    line = g.readline()
    #break

g.close()
f.close()