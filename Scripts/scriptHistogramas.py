"""scriptHistogramas.py"""
import findspark
import commands
findspark.init()
import pyspark
import operator 
from pyspark import SparkContext
import matplotlib.pyplot as plt

if __name__ == "__main__":

	sc = SparkContext(appName="Creating histograms")
	print "Creando Histogramas..."
	temp = 0
	#PARA MAS ADELANTE...
	#oldKeysDocs = sc.textFile("hdfs://Master:9000/PROYECTO/DataSetCore/OldKeysDocs/part-00000")
	#PARA MAS ADELANTE...

	#SE USARIA oldKeysDocs EN LUGAR DE KeysDocs.
	KeysDocs = sc.textFile("hdfs://Master:9000/PROYECTO/DataSetCore/KeysDocs/part-00000")

	rta = commands.getstatusoutput('hdfs dfs -rm -R /PROYECTO/DataSetCore/HistogramasBoW/')
	if rta[0] ==  0 or rta[0] ==  256:

		diccionarioGral = sc.textFile("hdfs://Master:9000/PROYECTO/DataSetCore/DiccionarioGralAZ/part-00000").flatMap(lambda palabra : palabra.split(" ")).filter(lambda x : len(x) > 0)
		for pathDocumento in KeysDocs.collect():
			#print "----------------------------------------"
			nombreVector = pathDocumento.split("/")
			nombre = nombreVector[len(nombreVector)-1]
			documentoBoW = sc.textFile("hdfs://Master:9000/PROYECTO/DataSetCore/BoWIndividual/"+nombre+"/part-00000").filter(lambda x : len(x) > 0).flatMap(lambda palabra : palabra.split(" ")).filter(lambda x : len(x) > 0)
			docParTemp = diccionarioGral.union(documentoBoW).map(lambda palabra: (palabra, 1)).reduceByKey(lambda x, y: x+y)
			docPar = docParTemp.mapValues(lambda x: x-1).sortByKey()
			#Se visualizan una determinada cantidad de histogramas. En este caso 2.
			"""
			ESTO SI LA PERSONA QUIERE VER ALGUNOS HISTOGRAMAS
			if temp<2:
				temp=temp+1
				output = docPar.collect()
				for (word, count) in output[:10]:
					print("%s: %i" % (word, count))
				words = docPar.keys().collect()
				counts = docPar.values().collect()
				x = range(len(words))

				plt.figure()
				plt.plot(x, counts,label = nombre)
				plt.ylabel("frecuencia")
				plt.xlabel("palabras")
				plt.xticks(x, "", size='small', color='b', rotation = 0)
				plt.legend()
				plt.title("Histograma "+str(temp))
				plt.suptitle(u"BAG OF WORDS")
				plt.axis([0,None,0,None])
				plt.show()
			"""

			#print "Guardando BoW..."
			#1ra forma de guardar en una sola linea "(key1,value1) (key2,value2)"
			#docParSave = docPar.map(lambda x: "("+str(x[0])+","+str(x[1])+") ").reduce(operator.concat)

			#2da forma de guardar en una sola linea "(u'ability', 3, u'account', 1)"
			#docParSave = docPar.reduce(operator.concat)

			#sc.parallelize([docParSave]).coalesce(1).saveAsTextFile("hdfs://Master:9000/PROYECTO/DataSetCore/HistogramasBoW/"+nombre)

			#3ra forma por defeault "(u'ability', 3)\n(u'account', 1)"
			docPar.coalesce(1).saveAsTextFile("hdfs://Master:9000/PROYECTO/DataSetCore/HistogramasBoW/"+nombre)

			#print "----------------------------------------"

	else:
		print ("Error: "+str(rta[0])+"\nDescripcion: "+rta[1]+" .")

	print " FIN "
	print "***************************"

	sc.stop()