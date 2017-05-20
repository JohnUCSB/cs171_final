import argparse
import socket
import sys

# get arguments
if len(sys.argv) != 2:
	print ("ERROR: Please check your arguments")
	print ("USAGE: ./prm [IP]")
	sys.exit(0)

TCP_IP = sys.argv[2]
TCP_PORT = 5001

# start client
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((TCP_IP, TCP_PORT))

while 1:
	try:
		line = sys.stdin.readline()
	except KeyboardInterrupt:
		break
	if not line:
		break
	tokens=line.split()
	if len(tokens)>0:
		#3.1 Data processing calls
		#map filename #milestone 2
		if tokens[0]=="map":
			print("milestone 2")
		#reduce filename1 filename2 ..... #milestone 2
		elif tokens[0]=="reduce":
			print("milestone 2")
		#replicate filename
		elif tokens[0]=="replicate":
			print("replicating")
			query = tokens[0]+" "+tokens[1]
			s.send(query)
		#stop
		elif tokens[0]=="stop":
			print("stopping")
		#resume
		elif tokens[0]=="resume":
			print("resumming")
			s.send(tokens[0])

		#3.2 Data query calls
		#total pos1 pos2 .....
		elif tokens[0]=="total":
			print("totaling")
			s.send(line)
		#print
		elif tokens[0]=="print":
			print("printing")
			s.send(tokens[0])
		#merge pos1 pos2
		elif tokens[0]=="merge":
			print("merging")
			query = tokens[0]+" "+tokens[1]+" "+tokens[2]
			s.send(query)

		#exit
		elif tokens[0]=="exit":
			print("exiting")
			sys.exit(0)
		else:
			print("Usage: map filename | reduce filename1 filename2 ..... | replicate filename | stop | resume | total pos1 pos2 ..... | print | merge pos1 pos2")
