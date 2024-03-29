import sys
import Queue
import collections
import threading
import socket
import time
import pickle
import os.path
from collections import OrderedDict

QUERY_Q = Queue.Queue()
QUERY_LOCK = threading.Lock()
PRM_SEND_LIST = []
SYS_PRM = None


def setup(filename, ip):
	# setup CLI IP
	# get a list of other IP/Port of PRM (different IP & port 5005)
	# recv/send is from asg 1
	prm_id = None
	id_counter = 0
	filestream = open(filename, "r").read().splitlines()
	for line in filestream:
		tmp_ip = line
		tmp_port = 5005
		if ip != tmp_ip: 
			PRM_SEND_LIST.append((tmp_ip, tmp_port))
			id_counter += 1
		else:
			prm_id = id_counter
	return prm_id

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

	print ("<Welcome to PRM>")

	#check for lost logs when starting
	SYS_PRM.recovery_req()

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

		# Exit
		if command == "exit":
			break
		# PRM commands May want to wrap these three functions into the class
		elif command == "prepare":
			if not SYS_PRM.stopped:
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
					SYS_PRM.send(src_ip, 5005, textstream)
		elif command == "ack":
			if not SYS_PRM.stopped:
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
		elif command == "accept":
			if not SYS_PRM.stopped:
				# ip accept pickle_stream([ballot_num, accept_val([index, log object])])
				accept_array = pickle.loads(msg)
				b_new = accept_array[0]
				b_old = SYS_PRM.ballot_num
				# compare ballot
				if SYS_PRM.ballot_num == b_new:
					# got majority for your accept
					if SYS_PRM.first_accept_majority:
						# FIRST confirmation of majority
						# TODO: check if you're missing any!
						SYS_PRM.accept_num = None
						SYS_PRM.accept_val = None
						SYS_PRM.first_accept_majority = False
						#check if the reciving log has index greater than current index+1, which means missing logs
						if accept_array[1][0] > len(SYS_PRM.logs):
							SYS_PRM.recovery_req();
						#put received log on appropriate indexed slot
						SYS_PRM.logs[accept_array[1][0]] = accept_array[1][1]
						if SYS_PRM.id == b_new[1]:
							# sender with first majority
							SYS_PRM.wait_queue.popleft()
						else:
							# pass on accepts (not original sender)
							pickle_array = [b_new, accept_array[1]]
							textstream = "accept " + pickle.dumps(pickle_array, protocol=pickle.HIGHEST_PROTOCOL)
							SYS_PRM.send_prm(textstream)
				elif b_new[0] > b_old[0] or (b_new[0] == b_old[0] and b_new[1] > b_old[1]):
					# lost ballet
					# update Paxos info
					SYS_PRM.ballot_num = b_new
					SYS_PRM.accept_num = None
					SYS_PRM.accept_val = None
					# send out accepts
					pickle_array = [b_new, accept_array[1]]
					textstream = "accept " + pickle.dumps(pickle_array, protocol=pickle.HIGHEST_PROTOCOL)
					SYS_PRM.send_prm(textstream)
					SYS_PRM.logs[accept_array[1][0]] = accept_array[1][1]
				else:
					# won ballet -- do nothing
					continue

		#Recovery commands
		elif command == "recovery_req":
			SYS_PRM.recovery_ans(msg)
		elif command == "recovery_res":
			SYS_PRM.recovery_rec(msg)

		# CLI commands
		elif command == "replicate":
			if os.path.isfile(msg): 
				SYS_PRM.replicate(msg) # ip replicate [filename]
			else:
				print ("ERROR: cannot find file, given file name: ")
				print (msg)
		elif command == "stop":
			print "stopping.."
			SYS_PRM.stop() # ip stop
		elif command == "resume":
			print "resuming.."
			SYS_PRM.resume() # ip resume
		elif command == "merge":
			poss = msg.split();
			if len(poss)==2:
				pos1 = int(poss[0]);
				pos2 = int(poss[1]);
				SYS_PRM.merge(pos1, pos2) # ip merge [pos1] [pos2]
			else:
				print ("ERROR: merge has to have two position arguments, but given: ")
				print (msg)
		elif command == "total":
			SYS_PRM.total() # ip total [pos1 pos2 ..]
		elif command == "print":
			SYS_PRM.print_filenames() # ip print
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
		self.logs = {}
		self.wait_queue = collections.deque()
		#status
		self.stopped = False

	# PRM calls from CLI
	def replicate(self, filename):
		if not self.stopped:
			# parse new log to wait_queue
			self.wait_queue.append(Log(filename))
			self.prepare()
	def stop(self):
		self.stopped = True
		return
	def resume(self):
		self.stopped = False
		#check for missing logs
		self.recovery_req()
		return

	# Data query calls from CLI
	def merge(self, pos1, pos2):
		if not self.stopped:
			to_be_merged = [self.logs[pos1],self.logs[pos2]]
			merge_result = {}
			for log in to_be_merged:
				for word in log.word_dict:
					if not word in merge_result:
						merge_result[word] = log.word_dict[word]
					else:
						merge_result[word] += log.word_dict[word]
			print merge_result

	def total(self):
		if not self.stopped:
			total_result = {}
			for index in self.logs:
				log = self.logs[index]
				for word in log.word_dict:
					if not word in total_result:
						total_result[word] = log.word_dict[word]
					else:
						total_result[word] += log.word_dict[word]
			print total_result

	def print_filenames(self):
		ordered_logs = OrderedDict(sorted(self.logs.items(), key=lambda t: t[0]))
		ret = ""
		for index in ordered_logs:
			log = ordered_logs[index]
			ret += str(log.filename) + "\n"
		print ret

	def print_logs(self):
		ordered_logs = OrderedDict(sorted(self.logs.items(), key=lambda t: t[0]))
		ret = ""
		for index in ordered_logs:
			log = ordered_logs[index]
			ret += str(index) + ": " + str(log.word_dict) + " from " + str(log.filename) + "\n"
		print ret

	# Helpers
	def prepare(self):
		if not self.stopped:
			# prepare
			self.ballot_num = [self.ballot_num[0]+1, self.id]
			self.accept_num = None
			self.accept_val = None
			# send
			textstream = pickle.dumps(self.ballot_num, protocol=pickle.HIGHEST_PROTOCOL)
			self.send_prm("prepare " + textstream)
	def send(self, send_ip, send_port, textstream):
		if not self.stopped:
			sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			sock.connect((send_ip, send_port))
			sock.sendall(textstream)
			sock.close()
	def send_prm(self, textstream):
		if not self.stopped:
			for dst in PRM_SEND_LIST:
				sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				sock.connect(dst)
				sock.sendall(textstream)
				sock.close()
	def send_cli(self, textstream):
		if not self.stopped:
			sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			sock.connect((self.ip, 5001))
			sock.sendall(textstream)
			sock.close()

	#helper functions for recoverying
	def recovery_req(self):
		if not self.stopped:
			maxIndex = len(self.logs)-1
			# send
			textstream = pickle.dumps(maxIndex, protocol=pickle.HIGHEST_PROTOCOL)
			self.send_prm("recovery_req " + textstream)
	def recovery_ans(self, msg):
		if not self.stopped:
			maxIndex_local = len(self.logs)-1
			index_str = pickle.loads(msg)
			maxIndex_foregin = int(index_str)
			debug_msg = "DEBUG: recovery_ans has maxIndex_local: " + str(maxIndex_local) + " index_str: " + str(index_str) + " maxIndex_foregin: " + str(maxIndex_foregin)
			while maxIndex_foregin < maxIndex_local:
				maxIndex_foregin += 1
				indexed_log= [maxIndex_foregin, self.logs[maxIndex_foregin]] #[index, log object]
				textstream = pickle.dumps(indexed_log, protocol=pickle.HIGHEST_PROTOCOL)
				self.send_prm("recovery_res " + textstream)
	def recovery_rec(self, msg):
		if not self.stopped:
			indexed_log = pickle.loads(msg)
			self.logs[indexed_log[0]]=indexed_log[1]

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

	# run threads
	thread1 = threading.Thread(target=process)
	thread2 = threading.Thread(target=listen, args=[sys_ip_address, sys_port])
	thread1.start()
	thread2.start()
	thread1.join()
	thread2.join()

if __name__ == "__main__":
	main()
