import sys
import threading
import argparse
import socket
import time

# TODO:
# coverage edge cases like "merge" instead of "merge [1] [2]"
# Node: sending to PRM must have 2 parts [command] [msg]
def listen(ip, port):
	# receive message - TCP
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	sock.bind((ip, port))
	sock.listen(16)

	# wait 5 seconds for other servers
	time.sleep(5)

	while True:
		stream, addr = sock.accept() # rcv stream
		data = stream.recv(1024) # buffer size of 1024 bytes
		if not data:
			continue

def process(ip):
	# wait 5 seconds for other servers
	time.sleep(5)
	print ("<Welcome to CLI>")
	while 1:
		try:
			line = sys.stdin.readline()
		except KeyboardInterrupt:
			break
		if not line:
			break
		tokens=line.split()
		if len(tokens)>0:
			# setup socket
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

			#3.1 Data processing calls
			if tokens[0]=="map":
				filename = tokens[1]
				total_count = len(open(filename, "r").read().split())
				half = total_count/2
				s.connect((ip, 5002))
				s.sendall("map " + filename + " 0 " + str(half))
				s.close()
				s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				s.connect((ip, 5003))
				s.sendall("map " + filename + " " + str(half) + " " + str(total_count-half))
				s.close()
			#reduce filename1 filename2 ..... #milestone 2
			elif tokens[0]=="reduce":
				filenamesString
				for index token in tokens:
					if index!=0: filenamesString = filenamesString + token + ";"
				s.connect((ip, 5004))
				s.sendall("reduce "+ filenamesString)
				s.close()
			#replicate filename
			elif tokens[0]=="replicate":
				print("replicating")
				query = tokens[0]+" "+tokens[1]
				s.connect((ip, 5005))
				s.sendall(query)
				s.close()
			#stop
			elif tokens[0]=="stop":
				print("stopping")
				s.connect((ip, 5005))
				s.sendall(tokens[0])
				s.close()
			#resume
			elif tokens[0]=="resume":
				print("resumming")
				s.connect((ip, 5005))
				s.sendall(tokens[0])
				s.close()

			#3.2 Data query calls
			#total pos1 pos2 .....
			elif tokens[0]=="total":
				print("totaling")
				s.connect((ip, 5005))
				s.sendall(line)
				s.close()
			#print
			elif tokens[0]=="print":
				print("printing")
				s.connect((ip, 5005))
				s.sendall(tokens[0])
				s.close()
			#merge pos1 pos2
			elif tokens[0]=="merge":
				print("merging")
				if len(tokens)>2:
					query = tokens[0]+" "+tokens[1]+" "+tokens[2]
					s.connect((ip, 5005))
					s.sendall(query)
					s.close()
				else:
					print("Usage merge: pos1 pos2")
			#exit
			elif tokens[0]=="exit":
				if len(tokens) > 1:
					if tokens[1] == "cli":
						print("exiting")
						break
					elif tokens[1] == "prm":
						s.connect((ip, 5005))
						s.sendall("exit")
						s.close()
				#if no argument given, exit both cli and prm
				else:
					s.connect((ip, 5005))
					s.sendall("exit")
					s.close()
					print("exiting")
					break

			else:
				print("Usage: map filename | reduce filename1 filename2 ..... | replicate filename | stop | resume | total pos1 pos2 ..... | print | merge pos1 pos2")


def main():
	# get arguments
	if len(sys.argv) != 2:
		print ("ERROR: Please check your arguments")
		print ("USAGE: ./prm [IP]")
		sys.exit(0)

	TCP_IP = sys.argv[1]
	TCP_PORT = 5001

	thread1 = threading.Thread(target=process, args=[TCP_IP])
	thread2 = threading.Thread(target=listen, args=[TCP_IP, TCP_PORT])
	thread2.setDaemon(True) # daemon thread
	thread1.start()
	thread2.start()
	thread1.join()
	sys.exit(0)

if __name__ == "__main__":
	main()
