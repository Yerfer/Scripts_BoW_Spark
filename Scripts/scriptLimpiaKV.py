"""scriptLimpiaKV.py"""
import findspark
import commands
findspark.init()
import pyspark
import operator 
import re
#from time import time
from pyspark import SparkContext

if __name__ == "__main__":

	print "Leer y limpiar Clave-Valor..."

	sc = SparkContext(appName="Lee y limpia Key-Value")

	"""

	OBSOLETO, NO SE ESTA USANDO

	"""

	idDoc = sc.textFile("hdfs://Master:9000/YERFER/temporal/prueba/reuterspruebaCORE/IdDoc/part-00000").filter(lambda x : len(x) > 0)

	patronId = re.compile('(\()|(,.*)')
	patronDoc = re.compile('(^\(.*u\')|(\'\))')
	#print "-------------------------------------------------------------------\n"

	idDoc3 = idDoc.map(lambda x: ( re.sub(patronId, "", x) , re.sub(patronDoc, "", x) )) 
	print "-------------------------------------------------------------------\n"
	for (id, word) in idDoc3.collect():
		print("(%s,%s)" % (id, word))
	print "-------------------------------------------------------------------\n"



	print " FIN "
	print "***************************"
	

	sc.stop()