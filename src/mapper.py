import re
import sys
import socket
import Queue
import threading
import time
from os.path import basename

QUERY_Q = Queue.Queue()
QUERY_LOCK = threading.Lock()
sys_id = 0
sys_dict = {}

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
		QUERY_LOCK.acquire()
		QUERY_Q.put(data)
		QUERY_LOCK.release()
		# Exit
		if data == "exit":
			break

def process():
	# wait 5 seconds for other servers
	time.sleep(5)

	while True:
		if QUERY_Q.empty():
			continue
		# Lock/unlock QUERY_LOCK
		QUERY_LOCK.acquire()
		query = QUERY_Q.get()
		QUERY_LOCK.release()
		# parse query data
		if query == "exit":
			break
		elif query.count(" ") == 3:
			cmd, filename, offset, read_size = query.split()
		else:
			print "ERROR: invalid command"
			print "ERROR: " + query
			continue
		if cmd == "map":
			# process infile
			i = int(offset)
			read_size = int(read_size)
			words = open(filename, "r").read().split()
			while(read_size > 0 and i < len(words)):
				w = re.sub(r'\W+', '', words[i])
				if w in sys_dict:
					sys_dict[w] += 1
				else:
					sys_dict[w] = 1
				#incre/decrement
				i += 1
				read_size -= 1
			# write to outfile
			filename = basename(filename)
			outfilename =  filename.split(".",1)[0] + "_I_" + str(sys_id)
			f = open(outfilename, "w")
			for key in sys_dict:
				f.write(key + " " + str(sys_dict[key]) + "\n")
			f.close()
			print "Completed mapping " + filename + " (id " + str(sys_id) + ")"
		else:
			print ("ERROR: unknown query command: ")
			print (cmd)


def main():
	global QUERY_Q, QUERY_LOCK, sys_id, sys_dict

	# NOTE: 
	# sys_id = 1 -> port 5002
	# sys_id = 2 -> port 5003

	# get arguments
	if len(sys.argv) != 3:
		print ("ERROR: Please check your arguments")
		print ("USAGE: ./mapper [IP] [ID]")
		sys.exit(0)
	sys_ip_address = sys.argv[1]
	sys_id = int(sys.argv[2])
	if sys_id == 1:
		sys_port = 5002
	else:
		sys_port = 5003

	# run threads
	thread1 = threading.Thread(target=process)
	thread2 = threading.Thread(target=listen, args=[sys_ip_address, sys_port])
	thread1.start()
	thread2.start()
	thread1.join()
	thread2.join()

if __name__ == "__main__":
	main()
