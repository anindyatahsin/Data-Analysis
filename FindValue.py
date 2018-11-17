import sys
import re
import math

f = open('../Resource_Cost.csv', 'r')
line = f.readline()
line = f.readline()
vars = line.split(',')
res_val = {}
'''
while line:
    vars = [x.strip() for x in line.split(',')]
    res_val[vars[0]] = vars[2]
    #i = i + 1
    line = f.readline()
print res_val
'''
f.close()

g = open('D:/Data/Collection/UVA_2017-10_data.csv', 'r')
wf = open('UVA_2017-12_val.csv', 'w')

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
toWrite = 'queue' + ',' + 'nodes' + ',' + 'cores' + ',' + 'gpus' + ',' \
              + 'mic' + ',' + 'highmem' + ',' \
              + 'other' + ',' + 'walltime' \
              + ',' + 'total val' + ',' + 'Job Value' + ',' + 'Resource val' +  ',\n'
wf.write(toWrite)

while line:
    vars = [x.strip() for x in line.split(',')]
    #res_list = re.split(":=",vars[header['"Resource_List_nodes"']])
    hosts = vars[header['exec_host']].replace('"','')

    resources = hosts.split('+')
    tot_val = 0
    tot_node = 0
    curNode = ''
    #print resources
    for res in resources:
        node = res.split('/')[0]
        #print 'lineno: ' + str(lineno)
        #print res_val[node]
        try:
            tot_val = tot_val + 1
        except KeyError:
            #print 'Unknown node ' + node + ' in: ' + str(lineno)
            if node != ' ':
                tot_val = tot_val + 1;
            else:
                continue
        if (node != curNode):
            curNode = node
            tot_node =  tot_node + 1
    #break
    res_req = vars[5].replace('"', '')
    resources = res_req.split('+')
    total_cores = 0
    total_gpus = 0
    total_mic = 0
    total_highmem = 0
    total_other = 0

    #if len(resources) > 1:
    #print lineno
    #print resources
    for res_lists in resources:
        res_list = res_lists.split(':')
        gpus = 0
        mic = 0
        highmem = 0
        other = 0
        #print res_list
        try:
            cores = int(res_list[0])
        except ValueError:
            cores = 1
        node = cores
        #print res_list
        if len(res_list) > 1:
            ppn = res_list[1].split('=')[1]

            cores = cores * int(ppn)
        elif len(res_list) > 2:
            print str(lineno) + ': ' + str(res_list)
            #print str(lineno) + ': ' + str(res_list)
            prop = res_list[2].split('=')
            #print prop
            if prop[0] == 'mic':
                if(len(prop) == 2):
                    mic = int(prop[1])
                else:
                    mic = 1
            elif prop[0] == 'gpus':
                if(len(prop) == 2):
                    gpus = int(prop[1])
                else:
                    gpus = 1
            elif prop[0] == 'highmem':
                if(len(prop) == 2):
                    highmem = int(prop[1])
                else:
                    highmem = 1
            else:
                if (len(prop) == 2):
                    other = int(prop[1])
                else:
                    other = 1
        else:
            cores = tot_node
            node = cores
        total_cores = total_cores + cores
        total_gpus = total_gpus + gpus
        total_mic = total_mic + mic
        total_highmem = total_highmem + highmem
        total_other = total_other + other

    val = vars[header['resources_used.walltime']].replace('"','').split(':')

    if len(val)> 2:
        days = val[0].split('-')
        if(len(days) > 1):
            walltime = (int(days[0]) * 24 + int(days[1])) * 60 * 60 + int(val[1]) * 60 + int(val[2])
        else:
            walltime = int(val[0]) * 60 * 60 + int(val[1]) * 60 + int(val[2])
    else:
        print 'no walltime'
        print val
        walltime = 0
    toWrite = ''
    resValue = math.ceil((tot_val * walltime) / 3600.0)

    jobValue = math.ceil ((total_cores * walltime) / 3600.0)
    #print jobValue
    if total_mic > 0:
        jobValue = jobValue + math.ceil ((walltime * total_mic)/3600.0) * 2
    if total_gpus > 0:
        jobValue = jobValue + math.ceil((walltime * total_gpus) / 3600.0) * 5
    if total_highmem > 0:
        jobValue = jobValue + math.ceil((walltime * total_highmem) / 3600.0) * 1.5
    if total_other > 0:
        jobValue = jobValue + math.ceil((walltime * total_other) / 3600.0) * 1.5
    if node > 1:
        jobValue = min(jobValue * 2, resValue)
    elif jobValue > resValue:
        jobValue = resValue

    #if vars[header['queue']] == '"normal_q"':
    toWrite = vars[header['queue']] + ',' + str(node) + ',' + str(total_cores) + ',' + str(total_gpus) + ',' \
          + str(total_mic) + ',' + str(total_highmem) + ',' \
          + str(total_other) + ',' + str(walltime) \
          + ',' + str(tot_val) + ',' + str(jobValue) + ',' + str(resValue) + ',' +  str(resValue - jobValue) +  ',\n'

    wf.write(toWrite)
    #print vars[header['"Resource_List_nodes"']]
    #print vars[res_dic]

    lineno = lineno + 1
    #if lineno == 2:
    #    break
    #break
    line = g.readline()
    #break

g.close()
wf.close()