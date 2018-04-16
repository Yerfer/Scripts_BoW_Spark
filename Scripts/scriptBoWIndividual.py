"""scriptBoWIndividual.py"""
import findspark
import commands
findspark.init()
import pyspark
import operator 
from pyspark import SparkContext

if __name__ == "__main__":

	#El corpus de cada documento es el mismo CORE que se creo en un inicio.
	print "***************************"
	print "Creando Bag of Words Individuales para cada documento a partir del Corpus de cada documento..."
	sc = SparkContext(appName="Creating BoWIndividual...")

	#PARA MAS ADELANTE...
	#oldKeysDocs = sc.textFile("hdfs://Master:9000/PROYECTO/DataSetCore/OldKeysDocs/part-00000")
	#PARA MAS ADELANTE...

	#SE USARIA oldKeysDocs EN LUGAR DE KeysDocs.
	KeysDocs = sc.textFile("hdfs://Master:9000/PROYECTO/DataSetCore/KeysDocs/part-00000")

	#Cargando el Stopword desde el HDFS.
	stopword = sc.textFile('hdfs://Master:9000/PROYECTO/DataSetCore/StopWordsEnglish/english.txt')

	rta = commands.getstatusoutput('hdfs dfs -rm -R /PROYECTO/DataSetCore/BoWIndividual/')
	#El error 256 representa que esta tratando de eliminar un directorio que no existe, si no existe pues lo crea y ya.
	if rta[0] ==  0 or rta[0] ==  256:
		#Obtener el nombre del archivo por split
		for pathDocumento in KeysDocs.collect():
			nombreVector = pathDocumento.split("/")
			nombre = nombreVector[len(nombreVector)-1]

			bowInd = sc.textFile("hdfs://Master:9000/PROYECTO/DataSetCore/Core/"+nombre+"/part-00000").filter(lambda x : len(x) > 0).flatMap(lambda palabra : palabra.split(" ")).filter(lambda x : len(x) > 0).subtract(stopword)
			#bowIndSave = bowInd.map(lambda x : x+" ").reduce(operator.concat)
			bowInd.coalesce(1).saveAsTextFile("hdfs://Master:9000/PROYECTO/DataSetCore/BoWIndividual/"+nombre)
			#print ("BoW del documento \""+nombre+"\" creado exitosamente.")
	else:
		print ("Error: "+str(rta[0])+"\nDescripcion: "+rta[1]+" .")

	print " FIN "
	print "***************************"

	sc.stop()