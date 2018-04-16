"""scriptSparseMatrix.py"""
import findspark
import commands
findspark.init()
import pyspark
import operator 
import re
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
import numpy as np

"""
#Este codigo hace que las filas sean las palabras y las columnas los documentos, es lento cuando son varias palabras y muchos docs
#pero son los docs que aumentaran con el tiempo por lo que a largo plazo puede ser mejor que la segunda parte.
if __name__ == "__main__":

	print "Creating Sparse Matrix (Termino/Documento)..."
	sc = SparkContext(appName="Creating Sparse Matrix (Termino/Documento)")
	spark = SparkSession.builder.getOrCreate()
	docsAZ = sc.textFile("hdfs://Master:9000/PROYECTO/DataSetCore/IdDoc/DocsAZ/part-00000")
	idDocsAZ = sc.textFile("hdfs://Master:9000/PROYECTO/DataSetCore/IdDoc/IdDocsAZ/part-00000")
	diccionarioGralAZ = sc.textFile("hdfs://Master:9000/PROYECTO/DataSetCore/DiccionarioGralAZ/part-00000")

	#Crear el dataframe con sus correspondientes columnas y con una fila temporal para rellenar que luego se elimina.
	idRDD = sc.parallelize(["ID"])
	columnas = idRDD.union(idDocsAZ).collect()
	temporal = [-1]*(idDocsAZ.count()+1)
	temporalDF = spark.createDataFrame([temporal], columnas)
	patronPalabra = re.compile('(\(u\')|(\',.*)')
	patronFrec = re.compile('([^\d])')
	diccionarioGralLista = diccionarioGralAZ.collect()
	nPalabras = len(diccionarioGralLista)
	docsAZList = docsAZ.collect()
	docsAZCount = docsAZ.count()+1
	division = True
	cubo = []
	rta = commands.getstatusoutput('hdfs dfs -rm -R /PROYECTO/DataSetCore/SparseMatrixDF')

	for doc in docsAZList:
		nombreVector = doc.split("/")
		nombre = nombreVector[len(nombreVector)-1]
		cubo.append(sc.textFile("hdfs://Master:9000/PROYECTO/DataSetCore/HistogramasBoW/"+nombre+"/part-00000").map(lambda x: ( re.sub(patronPalabra, "", x) , re.sub(patronFrec, "", x) )).collect())

	for idPalabra, palabra in enumerate(diccionarioGralLista):
		vector = np.zeros(docsAZCount)
		vector[0]=idPalabra
		for idDoc, doc in enumerate(docsAZList):
			valor = cubo[idDoc][idPalabra][1]
			if int(valor) > 0:
				vector[idDoc+1]=1
		if division==True:
			dataframe = spark.createDataFrame([vector.astype(int).tolist()], columnas)
			division=False
		else:
			df = spark.createDataFrame([vector.astype(int).tolist()], columnas)
			dataframe = dataframe.union(df)

		if (idPalabra>0 and idPalabra%1000==0) or idPalabra==(nPalabras-1):
			print "Guardando nuevo fragmento de 1000 palabras del dataframe en el HDFS como csv..."
			dataframe.coalesce(1).write.csv(path="hdfs://Master:9000/PROYECTO/DataSetCore/SparseMatrixDF",mode="append",header=True)
			print "Formateando Dataframe..."
			dataframe = spark.createDataFrame([temporal], columnas)
			division=True

	print " FIN "
	print "***************************"

	sc.stop()
"""

#Guarda con estructura: filas como documentos y columnas como palabras.
if __name__ == "__main__":

	print "Creating Sparse Matrix (Termino/Documento)..."
	sc = SparkContext(appName="Creating Sparse Matrix (Termino/Documento)")
	spark = SparkSession.builder.getOrCreate()
	print "Cargando Rdd..."
	docsAZ = sc.textFile("hdfs://Master:9000/PROYECTO/DataSetCore/IdDoc/DocsAZ/part-00000")
	#docsAZ.cache()
	#idDocsAZ = sc.textFile("hdfs://Master:9000/PROYECTO/DataSetCore/IdDoc/IdDocsAZ/part-00000")
	diccionarioGralAZ = sc.textFile("hdfs://Master:9000/PROYECTO/DataSetCore/DiccionarioGralAZ/part-00000")
	#diccionarioGralAZ.cache()
	#diccionarioGralAZID = sc.textFile("hdfs://Master:9000/PROYECTO/DataSetCore/IdWordAZ/part-00000")
	#diccionarioGralAZID.cache()
	print "Cargando datos..."
	#Crear el dataframe con sus correspondientes columnas y con una fila temporal para rellenar que luego se elimina.
	#idRDD = sc.parallelize(["ID"])
	patronPalabra = re.compile('(\(u\')|(\',.*)')
	patronFrec = re.compile('([^\d])')
	#diccionarioGralLista = diccionarioGralAZ.collect()
	#nPalabras = len(diccionarioGralLista)
	#columnasWords = idRDD.union(diccionarioGralAZID).collect()
	#docsAZList = docsAZ.collect()
	#docsAZCount = int(docsAZ.count())
	#temporalWords = [-1]*(diccionarioGralAZ.count()+1)
	#temporalDF = spark.createDataFrame([[-1]*(diccionarioGralAZ.count()+1)], columnasWords)
	#division = True
	cantidadPalabras = diccionarioGralAZ.count()
	#cubo = []
	patronNombre = re.compile('(.*)(\/)')
	rta = commands.getstatusoutput('hdfs dfs -rm -R /PROYECTO/DataSetCore/SparseMatrixDF')
	print "Primer for..."
	#for doc in docsAZ.collect():
	#	nombreVector = doc.split("/")
	#	nombre = nombreVector[len(nombreVector)-1]
	#	cubo.append(sc.textFile("hdfs://Master:9000/PROYECTO/DataSetCore/HistogramasBoW/"+nombre+"/part-00000").map(lambda x: ( re.sub(patronPalabra, "", x) , re.sub(patronFrec, "", x) )).collect())
	rdds = [sc.textFile("hdfs://Master:9000/PROYECTO/DataSetCore/HistogramasBoW/"+re.sub(patronNombre, "",doc)+"/part-00000").map(lambda x: ( re.sub(patronPalabra, "", x) , re.sub(patronFrec, "", x) )) for doc in docsAZ.collect()]
	
	#print "len(rdds)"
	#print len(rdds)
	#print "rdds[0].count()"
	#print rdds[0].count()

	#rdd = sc.union(rdds).cache()
	#rdd.cache()
	#print rdd.count()#40431000
	#print rdd.first()

	otro = sc.parallelize([])
	otro.cache()

	print "Segundo for..."
	for idDoc, doc in enumerate(docsAZ.collect()):
		#nombreVector = doc.split("/")
		nombre = re.sub(patronNombre, "",doc)#nombreVector[len(nombreVector)-1]
		#vector2 = np.zeros(cantidadPalabras+1)
		vector = [0]*(cantidadPalabras+1)
		vector[0]=idDoc
		if idDoc%10==0:
			print "Van "+str(idDoc+1)+" documentos."
			print "Documento: "+nombre
		
		palabrasVector = rdds[idDoc].collect()

		#print "Tamano del diccionario: "+str(len(palabrasVector))
		#print "Primera palabra: ", palabrasVector[0]
		#print "Ultima palabra: ", palabrasVector[len(palabrasVector)-1]
		
		for idPalabra, palabra in enumerate(diccionarioGralAZ.collect()):
			#[idDoc][idPalabra]=>[palabra,frecuencia]
			#print rdds[idDoc].collect()[idPalabra][1]

			palab, frecuencia = palabrasVector[idPalabra]
			#palab2, frecuencia2 = rdd.collect()[idPalabra+(cantidadPalabras*idDoc)]
			#frecuencia = cubo[idDoc][idPalabra][1]

			#print "PALABRAS: "+palab+"."
			#if idPalabra%50==0:
			#print str(idPalabra+600)+", PALABRA: "+palab+" = "+palabra+", frecuencia "+frecuencia
			if int(frecuencia)>0:
				vector[idPalabra+1]=1

		#Unimos la nueva tupla (convertido en un dataframe) a nuestro dataframe general.

		"""

		HACIENDO UN RDD Y GUARDANDO NO UNA MAATRIX SINO UN VECTOR EXTENSO?

		"""
		otro = otro.union(sc.parallelize([vector]))
		#otro = otro.union(sc.parallelize(vector))

		"""
		if division==True:
			print "agregando tupla inicial"
			print "tamano vector: "+str(len(vector))			

			#dataframe = spark.createDataFrame([vector.astype(int).tolist()], columnasWords)
			dataframe = spark.createDataFrame([vector], columnasWords)


			#sc.parallelize([vector]).coalesce(1).saveAsTextFile("hdfs://Master:9000/PROYECTO/DataSetCore/SparseMatrixDF")
			division=False
		else:
			print "agregando tuplas..."

			#df = spark.createDataFrame([vector.astype(int).tolist()], columnasWords)
			df = spark.createDataFrame([vector], columnasWords)

			dataframe = dataframe.union(df)
		#Cada 100 documentos genera una nueva parte para guardarlo, este valor es variable segun la memoria del cluster.
		if (idDoc>-1 and idDoc%50==0) or idDoc==(docsAZCount-1):
			print "Guardando nuevo documento como tupla en el dataframe en el HDFS como csv..."
			#print "mostrando el dataframe..."
			#dataframe.show()
			# **************************
			#dataframe.coalesce(1).write.csv(path="hdfs://Master:9000/PROYECTO/DataSetCore/SparseMatrixDF",mode="append",header=True)
			# **********************			
			#dataframe.write.csv(path="hdfs://Master:9000/PROYECTO/DataSetCore/SparseMatrixDF",mode="append",header=True)
			print "Formateando Dataframe..."
			dataframe = spark.createDataFrame([temporalWords], columnasWords)
			division=True
		"""

	#print "inicial"
	#print otro2.first()

	print "guardando..."
	otro.coalesce(1).saveAsTextFile("hdfs://Master:9000/PROYECTO/DataSetCore/SparseMatrixDF")

	print " FIN "
	print "***************************"

	sc.stop()