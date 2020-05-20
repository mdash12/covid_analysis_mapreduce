from __future__ import print_function

import sys
from datetime import datetime
from pyspark import SparkContext, SparkConf
from csv import reader
import time

def valid_row(word):

	try:
		return (word[1],int(word[2]))
	except Exception as e:
		print("Exception in map: " + str(e))
	return None

def pop_perm(row):
	
	try:
		pop_pm = 1000000*row[1] * 1.0/broadcasted_pop_dict.value[row[0]]
		return (row[0],str(pop_pm))
	except Exception as e:
		print("Exception in pop dict: " + str(e))
	return None

if __name__ == "__main__":
	
	execution_start = time.time()
	sc = SparkContext("local","PySpark TASK 3")
	
	dictionary_pop = {}
	with open(sys.argv[2]) as fp:
		csv_reader = reader(fp,delimiter = ',')
		for line in csv_reader:
			try:
				pop = int(line[4])
				if pop > 0:
					dictionary_pop.update({line[1] : pop})
			except Exception as e:
				print("exception in reading: " + str(e))

	broadcasted_pop_dict = sc.broadcast(dictionary_pop)

	# read data from text file and split each line into words
	words = sc.textFile(sys.argv[1]).map(lambda line: line.split(","))

	
	# count the occurrence of each word
	wordCounts = words.map(valid_row).filter(lambda x: x is not None).reduceByKey(lambda a,b: a+b)

	# save the counts to output
	wordCounts.map(pop_perm).filter(lambda x: x is not None).saveAsTextFile(sys.argv[3])
	execution_end = time.time()
	print("execution time: " + str(execution_end - execution_start))
	



