import sys
import Queue
import collections
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
	prm_id = None
	id_counter = 0
	filestream = open(filename, "r").read().splitlines()
	for line in filestream:
		tmp_ip, tmp_port = line.split()
		tmp_port = int(tmp_port)
		if ip != tmp_ip: 
			PRM_SEND_LIST.append((tmp_ip, tmp_port))
			id_counter += 1
		else:
			prm_id = id_counter
	return prm_id

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
		data = str(addr[0]) + " " + data
		QUERY_LOCK.acquire()
		QUERY_Q.put(data)
		QUERY_LOCK.release()
		# Exit
		if data.split()[1] == "exit":
			break

def process():
	# wait 5 seconds for other servers
	time.sleep(5)

	while True:
		if QUERY_Q.empty():
			#if SYS_PRM.accept_num == None and len(SYS_PRM.wait_queue) > 0:
				# not sending/receiving anything
				# and SYS_PRM.wait_queue is not empty
				#SYS_PRM.prepare()
			continue

		# Lock/unlock QUERY_LOCK
		QUERY_LOCK.acquire()
		query = QUERY_Q.get()
		QUERY_LOCK.release()
		if query.count(" ") >= 2:
			src_ip, command, msg = query.split(" ", 2)
		else:
			src_ip, command = query.split()
			msg = ""

		print "------"
		print "source ip is: ",
		print src_ip
		print "command is: ",
		print command
		print "msg is: ",
		print msg
		print "---eom---"

		# Exit
		if command == "exit":
			break
		# PRM commands
		elif command == "prepare":
			# ip prepare pickle_stream(ballot_num)
			b_new = pickle.loads(msg)
			b_old = SYS_PRM.ballot_num
			# compare ballot
			if b_new[0] > b_old[0] or (b_new[0] == b_old[0] and b_new[1] > b_old[1]):
				# lost ballot
				SYS_PRM.ballot_num = b_new
				# reply with "ack" pickle([ballot_num, accept_num, accept_val])
				SYS_PRM.first_accept_majority = True
				pickle_array = [SYS_PRM.ballot_num, SYS_PRM.accept_num, SYS_PRM.accept_val]
				textstream = "ack " + pickle.dumps(pickle_array, protocol=pickle.HIGHEST_PROTOCOL)
				SYS_PRM.send(src_ip, 5004, textstream) #DEBUG 5005
				print "<sending ack to 5004!>"
		elif command == "ack":
			# ip ack pickle_stream([ballot_num, accept_num, accept_val])
			# Note: there are only 3 nodes, so recving any "ack" + yourself is majority
			# Note: if you recv "ack", you already won the ballot once
			ack_array = pickle.loads(msg)
			if SYS_PRM.ballot_num == ack_array[0] and SYS_PRM.accept_num != ack_array[0]:
				# still same ballot AND
				# first majority after recving "ack"
				# update Paxos info
				SYS_PRM.first_accept_majority = True
				SYS_PRM.accept_num = ack_array[0] 
				SYS_PRM.accept_val = [len(SYS_PRM.logs), SYS_PRM.wait_queue[0]] #[index, log object]
				# send out accepts
				pickle_array = [SYS_PRM.ballot_num, SYS_PRM.accept_val]
				textstream = "accept " + pickle.dumps(pickle_array, protocol=pickle.HIGHEST_PROTOCOL)
				SYS_PRM.send_prm(textstream)
				print "<sending accept via ack to all!>"
		elif command == "accept":
			print "hi1"
			# ip accept pickle_stream([ballot_num, accept_val([index, log object])])
			accept_array = pickle.loads(msg)
			b_new = accept_array[0]
			b_old = SYS_PRM.ballot_num
			print accept_array
			print b_new
			print SYS_PRM.accept_num
			# compare ballot
			if SYS_PRM.ballot_num == b_new:
				print "hi2"
				# got majority for your accept
				if SYS_PRM.first_accept_majority:
					print "hi3"
					# FIRST confirmation of majority
					# TODO: check if you're missing any!
					SYS_PRM.accept_num = None
					SYS_PRM.accept_val = None
					SYS_PRM.first_accept_majority = False
					print accept_array
					print accept_array[1]
					SYS_PRM.logs.append(accept_array[1][1])
					if SYS_PRM.id == b_new[1]:
						# sender with first majority
						SYS_PRM.wait_queue.popleft()
					else:
						# pass on accepts (not original sender)
						pickle_array = [b_new, accept_array[1]]
						textstream = "accept " + pickle.dumps(pickle_array, protocol=pickle.HIGHEST_PROTOCOL)
						SYS_PRM.send_prm(textstream)
						print "<sending accept via accept to all!>"
			elif b_new[0] > b_old[0] or (b_new[0] == b_old[0] and b_new[1] > b_old[1]):
				print "hi2.1"
				# lost ballet
				# update Paxos info
				SYS_PRM.ballot_num = b_new
				SYS_PRM.accept_num = None
				SYS_PRM.accept_val = None
				# send out accepts
				pickle_array = [b_new, accept_array[1]]
				textstream = "accept " + pickle.dumps(pickle_array, protocol=pickle.HIGHEST_PROTOCOL)
				SYS_PRM.send_prm(textstream)
				SYS_PRM.logs.append(accept_array[1][1])
				print "<sending accept via accept to all!>"
			else:
				print "hi2.2"
				# won ballet -- do nothing
				continue
		# CLI commands
		elif command == "replicate":
			SYS_PRM.replicate(msg) # ip replicate [filename]
		elif command == "stop":
			SYS_PRM.stop() # ip stop
		elif command == "resume":
			SYS_PRM.resume() # ip resume
		elif command == "merge":
			SYS_PRM.merge() # ip merge [pos1] [pos2]
		elif command == "total":
			SYS_PRM.total() # ip total [pos1 pos2 ..]
		elif command == "print":
			SYS_PRM.print_filenames() # ip print
			# DEBUG
			textstream = SYS_PRM.print_logs()
			print "sending: ",
			print textstream
			SYS_PRM.send_cli(textstream)
			# --DEBUG--
		else:
			print ("ERROR: unknown query command: ")
			print (command)

class PRM(object):
	def __init__(self, prm_id, prm_ip):
		# Fixed
		self.id = prm_id
		self.ip = prm_ip
		# Paxos
		self.ballot_num = [0, 0]
		self.accept_num = None
		self.accept_val = None
		self.first_accept_majority = False
		# Logs
		self.logs = []
		self.wait_queue = collections.deque()

	# PRM calls from CLI
	def replicate(self, filename):
		# parse new log to wait_queue
		self.wait_queue.append(Log(filename))
		self.prepare()
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
	def print_logs(self):
		ret = ""
		index = 0
		for log in self.logs:
			ret += "0: " + str(log.word_dict) + " from " + str(log.filename) + "\n"
		return ret

	# Helpers
	def prepare(self):
		# prepare
		self.ballot_num = [self.ballot_num[0]+1, self.id]
		self.accept_num = None
		self.accept_val = None
		# send
		textstream = pickle.dumps(self.ballot_num, protocol=pickle.HIGHEST_PROTOCOL)
		self.send_prm("prepare " + textstream)
	def send(self, send_ip, send_port, textstream):
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.connect((send_ip, send_port))
		sock.sendall(textstream)
		sock.close()
	def send_prm(self, textstream):
		for dst in PRM_SEND_LIST:
			sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			sock.connect(dst)
			sock.sendall(textstream)
			sock.close()
	def send_cli(self, textstream):
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.connect((self.ip, 5001))
		sock.sendall(textstream)
		sock.close()

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
	'''
	# get arguments
	if len(sys.argv) != 3:
		print ("ERROR: Please check your arguments")
		print ("USAGE: ./prm [setup_prm] [IP]")
		sys.exit(0)
	sys_setup = sys.argv[1]
	sys_ip_address = sys.argv[2]
	sys_port = 5005

	# initialize
	prm_id = setup(sys_setup, sys_ip_address)
	SYS_PRM = PRM(prm_id, sys_ip_address)
	'''
	''''''
	# debug mode
	if int(sys.argv[1]) == 4:
		sys_ip_address = "127.0.0.1"
		sys_port = 5004
		prm_id = 4
		SYS_PRM = PRM(prm_id, sys_ip_address)
		PRM_SEND_LIST = [("127.0.0.1", 5002), ("127.0.0.1", 5003)]
	elif int(sys.argv[1]) == 2:
		sys_ip_address = "127.0.0.1"
		sys_port = 5002
		prm_id = 2
		SYS_PRM = PRM(prm_id, sys_ip_address)
		PRM_SEND_LIST = [("127.0.0.1", 5004), ("127.0.0.1", 5003)]
	elif int(sys.argv[1]) == 3:
		sys_ip_address = "127.0.0.1"
		sys_port = 5003
		prm_id = 3
		SYS_PRM = PRM(prm_id, sys_ip_address)
		PRM_SEND_LIST = [("127.0.0.1", 5002), ("127.0.0.1", 5004)]
	''''''

	# run threads
	thread1 = threading.Thread(target=process)
	thread2 = threading.Thread(target=listen, args=[sys_ip_address, sys_port])
	thread1.start()
	thread2.start()
	thread1.join()
	thread2.join()

if __name__ == "__main__":
	main()