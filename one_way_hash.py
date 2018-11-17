import hashlib
import sys

f = open(sys.argv[1]+'.log', 'r')
g = open(sys.argv[1]+ '-hashed.log', 'w')

line = f.readline()
g.write(line)

while line:
	line = f.readline()
	vars = line.split("|")
	i = 0


	toWrite = ''
	while (i < len(vars) - 1):
		if(i < 1 or i > 5):
			toWrite = toWrite + (vars[i] + '|')
		else:
			toWrite = toWrite + hashlib.md5(vars[i]).hexdigest() + '|'
		i+= 1

	toWrite += vars[i]

	g.write(toWrite)

print 'done'

f.close()
g.close() 	
