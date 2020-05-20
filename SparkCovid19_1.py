import sys
from datetime import datetime
from pyspark import SparkContext, SparkConf
import time


def valid_date(word):
	
	try:
		curdate = datetime.strptime(word[0],"%Y-%m-%d")
		if date_begin.value <= curdate and curdate <= date_end.value:
			return (word[1],int(word[3]))
	except:
		print("Exception invalid entry in map function")
	return None


if __name__ == "__main__":
	
	execution_start = time.time()
	validStart = datetime(2019, 12, 31)
	validEnd = datetime(2020, 4, 8)

	sc = SparkContext("local","PySpark TASK 2")
	try:
		date_begin = sc.broadcast(datetime.strptime(sys.argv[2],"%Y-%m-%d")) 
		date_end = sc.broadcast(datetime.strptime(sys.argv[3],"%Y-%m-%d")) 
		# If start date is not less than end date it is invalid
		if date_begin.value > date_end.value: 
			raise Exception()
		# If the given date range doesnot fall in 2019-12-31 to 2020-04-08 range it is invalid
		if date_begin.value < validStart or date_end.value > validEnd:
			raise Exception()
	except Exception as e:
		print("Invalid dates entered, exiting")
		exit(1)


	# read data from text file and split each line into words
	words = sc.textFile(sys.argv[1]).map(lambda line: line.split(","))
	wordCounts = words.map(valid_date).filter(lambda x: x is not None).reduceByKey(lambda a,b: a+b)
	wordCounts.saveAsTextFile(sys.argv[4])
	execution_end = time.time()
	print("execution time: " + str(execution_end - execution_start))
	

