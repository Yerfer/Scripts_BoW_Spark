"""scriptIdDoc.py"""
import findspark
import commands
findspark.init()
import pyspark
import operator 
from pyspark import SparkContext

if __name__ == "__main__":

	print "Creating Key-Value (Id-Doc)..."
	sc = SparkContext(appName="Creating Key-Value (Id-Doc)")#.getOrCreate()

	#PARA MAS ADELANTE...
	#oldKeysDocs = sc.textFile("hdfs://Master:9000/PROYECTO/DataSetCore/OldKeysDocs/part-00000")
	#PARA MAS ADELANTE...

	#SE USARIA oldKeysDocs EN LUGAR DE KeysDocs.
	KeysDocs = sc.textFile("hdfs://Master:9000/PROYECTO/DataSetCore/KeysDocs/part-00000")

	rta = commands.getstatusoutput('hdfs dfs -rm -R /PROYECTO/DataSetCore/IdDoc')
	if rta[0] ==  0 or rta[0] ==  256:
		docsAZ = KeysDocs.sortBy(lambda x:x)
		IdDocsAZ = sc.parallelize(xrange(docsAZ.count()))

		#print docsAZ.collect()
		#print IdDocsAZ.collect()

		docsAZ.coalesce(1).saveAsTextFile("hdfs://Master:9000/PROYECTO/DataSetCore/IdDoc/DocsAZ")
		IdDocsAZ.coalesce(1).saveAsTextFile("hdfs://Master:9000/PROYECTO/DataSetCore/IdDoc/IdDocsAZ")
	else:
		print ("Error: "+str(rta[0])+"\nDescripcion: "+rta[1]+" .")

	print " FIN "
	print "***************************"

	sc.stop()