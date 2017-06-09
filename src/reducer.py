import sys
import socket
import time
import Queue
import threading

QUERY_Q = Queue.Queue()
QUERY_LOCK = threading.Lock()
FILEPATH = "./"

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
		if data.split()[0] == "exit":
			break

def process():

	while True:
		if QUERY_Q.empty():
			continue
		# Lock/unlock QUERY_LOCK
		QUERY_LOCK.acquire()
		query = QUERY_Q.get()
		QUERY_LOCK.release()
		filenamesString = ""
		print ("Query--" + query)
		# parse query data
		if query.count(" ") == 1:
			cmd, filenamesString = query.split()
			print (filenamesString)
		else:
			cmd = query
		# process commands
		if cmd == "exit":
			break
		elif cmd == "reduce":
			reduce(filenamesString)		
		else:
			print ("ERROR: unknown query command: ")
			print (cmd)

#reduce files into single file
def reduce(filenamesString):
	partNumCount = -1
	reduced = {}
	origFileName = ""
	filesnames = filenamesString.split(";")
	for filename in filesnames:

		#check if filesnames are intermediate
		print filename
		origFileName, I, partNumString = filename.split("_")
		partNum = int(partNumString)
		if partNumCount == -1:
			partNumCount = partNum
		else:
			if partNumCount == partNum-1:
				partNumCount += 1
			else:
				print ("ERROR: filename isn't intermediate: ")
				print (filename)

		#update dictionary with each file
		try:
			filestream = open((FILEPATH + filename), "r")
			for line in filestream:
				word, count = line.split()
				count = int(count)
				if word not in reduced:
					reduced[word] = 1
				else:
					reduced[word] += 1
		except:
			print ("ERROR: cannot open file: ")
			print (filename)
	
	#For debugging
	print(reduced)

	#write reduced result to file
	# write to outfile
	outfilename =  filename + "_reduced"
	f = open((FILEPATH + outfilename), "w")
	for key in reduced:
		f.write(key + " " + str(reduced[key]) + "\n")
	f.close()
	print("Completed reducing: ")
	print(filenamesString.replace(";"," "))


def main():
	global QUERY_Q, QUERY_LOCK

	# get arguments
	if len(sys.argv) != 2:
		print ("ERROR: Please check your arguments")
		print ("USAGE: ./reducer [IP]")
		sys.exit(0)
	sys_ip_address = sys.argv[1]
	sys_port = 5004
	# run threads
	thread1 = threading.Thread(target=process)
	thread2 = threading.Thread(target=listen, args=[sys_ip_address, sys_port])
	thread1.start()
	thread2.start()
	thread1.join()
	thread2.join()

if __name__ == "__main__":
	main()

if __name__ == "__main__":
	main()