import sys

class Connection(object):
	def __init__(self):
		self.out_sockets = {}
		self.in_channels = {}

	def start_listen(self):
		

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

	# add sleeps!

	if len(sys.argv) != 4:
		print ("ERROR: Please check your arguments")
		print ("USAGE: ./prm [filename] [IP] [port]")
		sys.exit(0)

	sys_filename = sys.argv[1]
	sys_ip_address = sys.argv[2]
	sys_port = int(sys.argv[3])

	myPRM = PRM(Log(sys_filename))

if __name__ == "__main__":
	main()