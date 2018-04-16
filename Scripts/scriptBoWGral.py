"""scriptBoWGral.py"""
import findspark
import commands
findspark.init()
import pyspark
import operator 
from pyspark import SparkContext

if __name__ == "__main__":

	print "*******************************************"
	print "Creando Bag of Words General del Corpus..."
	print "-------------------------------------------"
	sc = SparkContext(appName="Creating BoWGral...")

	#oldKeysDocs = sc.textFile("hdfs://Master:9000/PROYECTO/DataSetCore/OldKeysDocs/part-00000")
	commands.getstatusoutput('hdfs dfs -mkdir /PROYECTO/DataSetCore/StopWordsEnglish')
	commands.getstatusoutput('hdfs dfs -put -p Stopwords/english.txt /PROYECTO/DataSetCore/StopWordsEnglish/')

	rta = commands.getstatusoutput('hdfs dfs -rm -R /PROYECTO/DataSetCore/BoWGral/')
	#El error 256 representa que esta tratando de eliminar un directorio que no existe, si no existe pues lo crea y ya.
	if rta[0] ==  0 or rta[0] ==  256:

		palabrasSplit = sc.textFile("hdfs://Master:9000/PROYECTO/DataSetCore/CorpusGral/part*").filter(lambda x : len(x) > 0)
		#palabrasSplit = palabras.flatMap(lambda palabra : palabra.split(" ")).filter(lambda x : len(x) > 0)

		#Cargando el Stopword desde el HDFS.
		stopword = sc.textFile('hdfs://Master:9000/PROYECTO/DataSetCore/StopWordsEnglish/english.txt')

		#print "cantidad de palabras limpias "+str(len(palabraslimpias.collect()))
		#print "Cantidad de Stopword "+str(stopword.count())

		#interseccion = bow.intersection(stopword)
		#print "Cantidad de palabras que se van a quitar por el stop words: "+str(interseccion.count())

		#print "Palabras que se van a quitar:"
		#print interseccion.collect()

		print "Quitando  Palabras..."
		bowGral = palabrasSplit.subtract(stopword)
		print "Cantidad de palabras Validas: "+str(palabrasSplit.count())

		print "Guardando en el HDFS..."
		#bowGralfinal = bowGral.map(lambda x : x+" ").reduce(operator.concat)
		bowGral.coalesce(1).saveAsTextFile("hdfs://Master:9000/PROYECTO/DataSetCore/BoWGral")
		print "----    *************      ------"
		print ("Creacion del BoW General terminado. ")
		print "----    *************      ------"
		#print docfinal[:200]

		#print "Palabras Finales del documento SIN REPETIDAS (Muestra)"
		#docfinalsinRepetir = palabrasfiltradas.distinct().map(lambda x : x+" ").reduce(operator.concat)
		#print docfinalsinRepetir[:200]

	else:
		print ("Error: "+str(rta[0])+"\nDescripcion: "+rta[1]+" .")

	print "***************************"

	sc.stop()