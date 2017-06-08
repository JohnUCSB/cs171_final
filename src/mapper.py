import sys
import threading

def main():
	# get arguments
	if len(sys.argv) != 2:
		print ("ERROR: Please check your arguments")
		print ("USAGE: ./mapper [ID]")
		sys.exit(0)
	unique_id = int(sys.argv[1])



if __name__ == "__main__":
	main()