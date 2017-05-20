import sys
import Queue
import threading
import socket
import time
import pickle

QUERY_Q = Queue.Queue()
QUERY_LOCK = threading.Lock()

def setup():
	# setup CLI IP/PORT (same IP & port 5001)
	# get a list/dict of other IP/Port of PRM (different IP & port 5005)
	# recv/send is from asg 1
	return

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


class PRM(object):
	def __init__(self, start_log):
		self.logs = [start_log]

	def replicate(self):
		return
	def stop(self):
		return
	def resume(self):
		return
	def merge(self):
		return
	def total(self):
		return
	def print_logs(self):
		return
	def print_result(self):
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
	global QUERY_Q, QUERY_LOCK
	# Notes
	#	add sleeps

	# get arguments
	if len(sys.argv) != 4:
		print ("ERROR: Please check your arguments")
		print ("USAGE: ./prm [setup_prm] [reduced_file] [IP] [port]")
		sys.exit(0)
	sys_filename = sys.argv[1]
	sys_ip_address = sys.argv[2]
	sys_port = int(sys.argv[3])

	# PRM
	myPRM = PRM(Log(sys_filename))

	thread1 = threading.Thread(target=process)
	thread2 = threading.Thread(target=listen, args=[sys_ip_address, sys_port]);
	thread1.start()
	thread2.start()

	# when does program end?
	# t2.setDaemon(True) # daemon thread 














if __name__ == "__main__":
	main()