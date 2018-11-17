import sys
import re


g = open('../VT/VT_2017-10_data.csv', 'r')
#f = open('VT_2017-12_data_summary.csv', 'w')

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
print len(header)

g.close()
g = open('MASON-DATA-Dec-2017.csv','w');
i = 0
writeline = ''
for x in range(0, len(header)):
    writeline = writeline + header.keys()[header.values().index(x)] + ','
writeline = writeline + '\n'
g.write(writeline)
for date in range(01,32):
    f = open('../Mason/201712' + format(date,'02d') + '.anon', 'r')
    line = f.readline()

    while line:
        vars = line.split()
        jobs = []
        jobVals = {}
        # process var 0 and 1
        jobType = vars[1].split(';')
        if len(jobType) == 4:
            if jobType[1] != 'E':
                line = f.readline()
                continue;
                # pass
                # not the final job entry
            user = jobType[3].split('=')
            if (user[0] in header):
                jobVals[header[user[0]]] = user[1]
        for x in range(2, len(vars)):
            valPair = vars[x].split('=')
            if len(valPair) == 2:
                if(valPair[0] in header):
                    jobVals[header[valPair[0]]] = valPair[1]
            if len(valPair) >= 3:
                if (valPair[0] in header):
                    jobVals[header[valPair[0]]] = valPair[1] + '=' + valPair[2]
        writeline = ''
        for x in range(0, len(header)):
            if(x in jobVals):
                writeline = writeline + jobVals[x] + ','
            else:
                writeline = writeline + ','
                print 'line no :' + str(i) + ' ' + str(x) + ' not found';
        writeline = writeline + '\n'
        g.write(writeline)
        jobs.append(jobVals)
        line = f.readline()
        i = i + 1
        #print 'lineno ' + str(i)
    f.close()
g.close()
    #print jobVals




