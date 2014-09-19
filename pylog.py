# Apache Spark Log Processor

from pyspark import SparkContext, SparkConf
import sys
import math

def writeToFile(fname, string):
	f = open(fname,'a')
	f.write(string)
	f.close()


def Main():

	sc = SparkContext(appName='pylog')

	if len(sys.argv) == 3:
		log = sc.textFile(sys.argv[1])
		log_ne = log.filter(lambda x: len(x) > 0 )
		line = log_ne.map(lambda x: x.split())

		ip = line.map(lambda x: (x[0],1))\
			 .reduceByKey(lambda x,y: x+y)\
			 .sortByKey(True)

		req_10 = line.map(lambda x: (x[3].replace('[',''),1))\
				 .map(lambda x: (x[0][0:-4]+'0:00',1))\
				 .reduceByKey(lambda x,y: x+y)\
				 .sortByKey(True)


		tp99 = line.map(lambda x: (int(x[8]),1))\
			   .sortByKey(lambda x: x)

		index = int(math.ceil(tp99.count()*.99))
		tp = tp99.take(index)

		outlog = 'IP : Count\n'
		# requests from an IP
		for (i, icount) in ip.collect():
			#print(i + ' => ' + str(icount))
			outlog += i + ' ' + str(icount) + '\n'
		writeToFile(sys.argv[2]+'/ip_addr.log',outlog)

		outlog = 'Request interval: Count\n'
		# requests every 10 minute
		for (ts, tscount) in req_10.collect():
			#print(ts + ' => ' + str(tscount))
			outlog += ts + ' ' + str(tscount) + '\n'
		writeToFile(sys.argv[2]+'/req_int.log',outlog)

		# TP99
		#print ('TP99 = ' + str(tp[-1][0]))
		outlog = 'TP99 ' + str(tp[-1][0]) + '\n'
		writeToFile(sys.argv[2]+'/tp_99.log',outlog)

	else:
		print 'Usage: pylog <input file> <output path>'
	

	sc.stop()

if __name__ == '__main__':
	Main()
