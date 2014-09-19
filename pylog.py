from pyspark import SparkContext, SparkConf

def Main():

	sc = SparkContext(appName='pylog')

	log = sc.textFile('access.log')
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

	index = int(tp99.count()*.99)
	tp = tp99.take(index)

	# requests from an IP
	for (i, icount) in ip.collect():
		print(i + ' => ' + str(icount))

	# requests every 10 minute
	for (ts, tscount) in req_10.collect():
		print(ts + ' => ' + str(tscount))

	# TP99
	print ('TP99 = ' + str(tp[-1][0]))
	

	sc.stop()

if __name__ == '__main__':
	Main()
