"""scriptDiccionarioGral.py"""
import findspark
import commands
findspark.init()
import pyspark
import operator 
from pyspark import SparkContext

if __name__ == "__main__":

	print "***************************"
	sc = SparkContext(appName="Creating Dictionary")
	#oldKeysDocs = sc.textFile("hdfs://Master:9000/PROYECTO/DataSetCore/OldKeysDocs/part-00000")

	rta = commands.getstatusoutput('hdfs dfs -rm -R /PROYECTO/DataSetCore/DiccionarioGralAZ/')
	#El error 256 representa que esta tratando de eliminar un directorio que no existe, si no existe pues lo crea y ya.
	if rta[0] ==  0 or rta[0] ==  256:

		bow = sc.textFile("hdfs://Master:9000/PROYECTO/DataSetCore/BoWGral/part-00000")
		diccionarioGral = bow.flatMap(lambda palabra : palabra.split(" ")).filter(lambda x : len(x) > 0).distinct().sortBy(lambda x:x)
		#diccionarioGral= temp.map(lambda x : x+" ").reduce(operator.concat)
		print "Creando Diccionario General..."
		#sc.parallelize([diccionarioGral]).coalesce(1).saveAsTextFile("hdfs://Master:9000/PROYECTO/DataSetCore/DiccionarioGral")
		diccionarioGral.coalesce(1).saveAsTextFile("hdfs://Master:9000/PROYECTO/DataSetCore/DiccionarioGralAZ")
		print "Cantidad de palabras en el Diccionario GRAL: "+str(diccionarioGral.count())
		print ("Creacion del diccionario general terminado. ")
	else:
		print ("Error: "+str(rta[0])+"\nDescripcion: "+rta[1]+" .")
	print "******* FIN **********"

	sc.stop()