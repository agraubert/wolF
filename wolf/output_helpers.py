#
# some syntatic sugar functions for reading outputs into values
#

def read_file(infile):
	with open(infile, "r") as f:
		return f.read().rstrip()

def read_file_raw(infile):
	with open(infile, "rb") as f:
		return f.read()

def read_lines(infile):
	with open(infile, "r") as f:
		return f.read().split("\n")

