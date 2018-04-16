"""scriptIdWord.py"""
import findspark
import commands
findspark.init()
import pyspark
import operator 
from pyspark import SparkContext

if __name__ == "__main__":

	print "Creating Key-Value (Id-Word)..."
	sc = SparkContext(appName="Creating Key-Value (Id-Word)")

	diccionarioGral = sc.textFile("hdfs://Master:9000/PROYECTO/DataSetCore/DiccionarioGralAZ/part-00000").filter(lambda x : len(x) > 0)
	rta = commands.getstatusoutput('hdfs dfs -rm -R /PROYECTO/DataSetCore/IdWordAZ')
	if rta[0] ==  0 or rta[0] ==  256:
		IdWordAZ = sc.parallelize(xrange(diccionarioGral.count()))

		#print IdWordAZ.collect()[:100]
		#print diccionarioGral.collect()[:100]

		IdWordAZ.coalesce(1).saveAsTextFile("hdfs://Master:9000/PROYECTO/DataSetCore/IdWordAZ")
	else:
		print ("Error: "+str(rta[0])+"\nDescripcion: "+rta[1]+" .")

	print " FIN "
	print "***************************"

	sc.stop()