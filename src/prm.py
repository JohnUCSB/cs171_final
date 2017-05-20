import sys
import Queue
import threading
import socket
import time
import pickle

QUERY_Q = Queue.Queue()
QUERY_LOCK = threading.Lock()
PRM_SEND_LIST = []
SYS_PRM = None


def setup(filename, ip):
	# setup CLI IP/PORT (same IP & port 5001)
	# get a list of other IP/Port of PRM (different IP & port 5005)
	# recv/send is from asg 1
	filestream = open(filename, "r").read().splitlines()
	for line in filestream:
		tmp_ip, tmp_port = line.split()
		tmp_port = int(tmp_port)
		if ip != tmp_ip: 
			PRM_SEND_LIST.append((tmp_ip, tmp_port))

def listen(ip, port):
	# receive message - TCP
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
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
	# wait 5 seconds for other servers
	time.sleep(5)

	while True:
		if QUERY_Q.empty():
			continue
		# Lock/unlock QUERY_LOCK
		QUERY_LOCK.acquire()
		query = QUERY_Q.get()
		QUERY_LOCK.release()

		print query

		# Exit
		if query[0] == "exit":
			break
		# PRM commands
		elif query[0] == "prm":
			continue
		elif query[0] == "propose":
			continue
		elif query[0] == "ack":
			continue
		elif query[0] == "accept":
			continue
		# CLI commands
		elif query[0] == "replicate":
			SYS_PRM.replicate(query[1]) # replicate [filename]
		elif query[0] == "stop":
			SYS_PRM.stop() # stop
		elif query[0] == "resume":
			SYS_PRM.resume() # resume
		elif query[0] == "merge":
			SYS_PRM.merge() # merge [pos1] [pos2]
		elif query[0] == "total":
			SYS_PRM.total() # total [pos1 pos2 ..]
		elif query[0] == "print":
			SYS_PRM.print_filenames() # print
		else:
			print ("ERROR: unknown query command: ")
			print (query)

class PRM(object):
	def __init__(self):
		self.logs = []
		self.wait_queue = Queue.Queue()

	# PRM calls from CLI
	def replicate(self, filename):
		# parse new log to wait_queue
		self.wait_queue.put(Log(filename))
		# TODO -- propose & update
	def stop(self):
		return
	def resume(self):
		return

	# Data query calls from CLI
	def merge(self):
		return
	def total(self):
		return
	def print_filenames(self):
		return

class Log(object):
	def __init__(self, filename):
		self.filename = filename
		self.word_dict = {}
		self.parse_file()

	def parse_file(self):
		filestream = open(self.filename, "r").read().splitlines()
		for line in filestream:
			word, count = line.split()
			count = int(count)
			if not word in self.word_dict:
				self.word_dict[word] = count
			else:
				self.word_dict[word] += count

def main():
	global QUERY_Q, QUERY_LOCK, PRM_SEND_LIST, SYS_PRM
	# Notes
	#	add sleeps

	# get arguments
	if len(sys.argv) != 3:
		print ("ERROR: Please check your arguments")
		print ("USAGE: ./prm [setup_prm] [IP]")
		sys.exit(0)
	sys_setup = sys.argv[1]
	sys_ip_address = sys.argv[2]
	sys_port = 5005

	# initialize
	setup(sys_setup, sys_ip_address)
	SYS_PRM = PRM()

	# run threads
	thread1 = threading.Thread(target=process)
	thread2 = threading.Thread(target=listen, args=[sys_ip_address, sys_port]);
	thread1.start()
	thread2.start()
	thread1.join()
	thread2.join()

if __name__ == "__main__":
	main()