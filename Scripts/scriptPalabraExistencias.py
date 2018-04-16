"""scriptPalabraExistencias.py"""
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
import math

if __name__ == "__main__":

	print "Creating PalabraExistencias..."
	sc = SparkContext(appName="Creating PalabraExistencias")
	spark = SparkSession.builder.getOrCreate()

	docsAZ = sc.textFile("hdfs://Master:9000/PROYECTO/DataSetCore/IdDoc/DocsAZ/part-00000")
	idDocsAZ = sc.textFile("hdfs://Master:9000/PROYECTO/DataSetCore/IdDoc/IdDocsAZ/part-00000")
	diccionarioGralAZ = sc.textFile("hdfs://Master:9000/PROYECTO/DataSetCore/DiccionarioGralAZ/part-00000")
	#diccionarioGralAZID = sc.textFile("hdfs://Master:9000/PROYECTO/DataSetCore/IdWordAZ/part-00000")

	#Crear el dataframe con sus correspondientes columnas y con una fila temporal para rellenar que luego se elimina.
	#idRDD = sc.parallelize(["ID"])
	patronPalabra = re.compile('(\(u\')|(\',.*)')
	patronFrec = re.compile('([^\d])')
	#diccionarioGralLista = diccionarioGralAZ.collect()
	#nPalabras = len(diccionarioGralLista)
	#columnasWords = idRDD.union(diccionarioGralAZID).collect()
	#docsAZList = docsAZ.collect()
	docsAZCount = int(docsAZ.count())
	#temporalWords = [-1]*(diccionarioGralAZ.count()+1)
	#temporalDF = spark.createDataFrame([temporalWords], columnasWords)
	#division = True
	cantidadPalabras = diccionarioGralAZ.count()
	#cubo = []
	patronNombre = re.compile('(.*)(\/)')
	#ESTO ES FUNCIONAL
	#
	#
	#
	#rta = commands.getstatusoutput('hdfs dfs -rm -R /PROYECTO/DataSetCore/TFIDFMatrixDF')
	#
	#
	#
	#
	#FUNCIONAL


	print "Primer for..."
	"""
		for doc in docsAZList:
			nombreVector = doc.split("/")
			nombre = nombreVector[len(nombreVector)-1]
			cubo.append(sc.textFile("hdfs://Master:9000/PROYECTO/DataSetCore/HistogramasBoW/"+nombre+"/part-00000").map(lambda x: ( re.sub(patronPalabra, "", x) , re.sub(patronFrec, "", x) )).collect())
	"""

	rdds = [sc.textFile("hdfs://Master:9000/PROYECTO/DataSetCore/HistogramasBoW/"+re.sub(patronNombre, "",doc)+"/part-00000").map(lambda x: ( re.sub(patronPalabra, "", x) , re.sub(patronFrec, "", x) )).cache() for doc in docsAZ.collect()]

	palabraExistencias = []
	entro = True

	otro = sc.parallelize([])
	otro.cache()

	#FALLO: print "prueba dde rdds -> broadcast"
	#FALLO: rddsBC = sc.broadcast(rdds)

	lista = []
	print "Cargando documentos..."
	for x in xrange(docsAZCount):
		lista.append(rdds[x].collect())

	#NO SE PUDO COMPROBAR SI ESTA OPCION SERVIA YA QUE LA MEMORIA FUE INSUFICIENTE
	"""
	print "Convirtiendo a broadcast..."
	listaBC = sc.broadcast(lista)

	print "Eliminando la lista y liberando memoria RAM..."
	lista.clear()

	print "Creando otros BC..."
	cantidadPalabrasBC = sc.broadcast([cantidadPalabras])
	diccionarioGralAZBC = sc.broadcast(diccionarioGralAZ.collect())
	docsAZCountBC = sc.broadcast([docsAZCount])

	def funcion(idDocumento):
		print "idDocumento: "+str(idDocumento)
		vector = [0]*(cantidadPalabrasBC.value+1)
		vector[0]=idDocumento
		#FALLO: palabrasVector = rddsBC.value[idDocumento].collect()
		#SEGUNDA FORMA POR SI LA PRIMERA FALLA.
		palabrasVector = listaBC.value[idDocumento].collect()
		for idPalabra, palabra in enumerate(diccionarioGralAZBC.value):
			#[idDoc][idPalabra]=>[palabra,frecuencia]
			#frecuencia = cubo[idDoc][idPalabra][1]
			palab, frecuencia = palabrasVector[idPalabra]
			existencias = 0
			for x in xrange(0,docsAZCountBC.value):
				#FALLO: if int(rddsBC.value[x][idPalabra][1])>0:
				#SEGUNDA OPCION...
				if int(listaBC.value[x][idPalabra][1])>0:
					existencias+=1
			tf = float(frecuencia)
			delta = float(math.log10(docsAZCountBC.value))
			idf = math.log10((float(docsAZCountBC.value)+delta)/float(existencias))
			vector[idPalabra+1]=tf*idf

			#Creamos una key-value de palabra-existencias en el core, para guardarla en el HDFS y usarla en la similitud coseno en lugar de operar de nuevo.
			#if(entro):
			#	palabraExistencias.append((palabra,existencias))

		return [idDocumento, vector]

	print "Creando RDD final..."
	final = idDocsAZ.map(funcion)

	print "Guardando..."
	final.coalesce(1).saveAsTextFile("hdfs://Master:9000/PROYECTO/DataSetCore/TFIDFMatrixDF")
	"""

	#ESTE FORMA USA CARGANDO TODOS LOS DATOS EN UN ARRAYLIST, MUY PESADO PARA LA MEMORIA PERO MUCHO MAS RAPIDO A LA HORA DE PROCESAR.
	print "Procesando documentos..."
	"""for idDoc, doc in enumerate(docsAZ.collect()[0]): ####ACA HICE CAMBIO [0]...
		nombre = re.sub(patronNombre, "",doc)
		vector = [0]*(cantidadPalabras+1)
		vector[0]=idDoc
		if idDoc%10==0:
			print "Van "+str(idDoc+1)+" documentos."
			print "Documento: "+nombre

		#palabrasVector = rdds[idDoc].collect()
		#ESTO PRODRIA CAMBIAR ALGO...
		palabrasVector = lista[idDoc]
	"""
	palabrasVector = lista[0]
	for idPalabra, palabra in enumerate(diccionarioGralAZ.collect()):
		#[idDoc][idPalabra]=>[palabra,frecuencia]
		#frecuencia = cubo[idDoc][idPalabra][1]
		palab, frecuencia = palabrasVector[idPalabra]
		existencias = 0
		for x in xrange(0,docsAZCount):
			#if int(rdds[x].collect()[idPalabra][1])>0:  #ESTO SERA DEMORADO...
			if int(lista[x][idPalabra][1])>0:
				existencias+=1
			#Calculamos cada valor Wtd
			###################tf = float(frecuencia)
			########################delta = float(math.log10(docsAZCount))
			#######################idf = math.log10((float(docsAZCount)+delta)/float(existencias))
			############################vector[idPalabra+1]=tf*idf

			#Creamos una key-value de palabra-existencias en el core, para guardarla en el HDFS y usarla en la similitud coseno en lugar de operar de nuevo.
		"""if(entro):"""
		palabraExistencias.append((palabra,existencias))		
		############################otro = otro.union(sc.parallelize([vector]))
		entro = False

	#FORMA NO PROBADA para liberar espacio de memoria.
	#del lista[:]


	#print "Guardando Matrix-TFIDF..."
	########################otro.coalesce(1).saveAsTextFile("hdfs://Master:9000/PROYECTO/DataSetCore/TFIDFMatrixDF")
	print "Guardando key-value de existencias..."
	commands.getstatusoutput('hdfs dfs -rm -R /PROYECTO/DataSetCore/PalabraExistencias')
	sc.parallelize(palabraExistencias).coalesce(1).saveAsTextFile("hdfs://Master:9000/PROYECTO/DataSetCore/PalabraExistencias")


	### ESTA FORMA NO SIRVIO YA QUE EL DATAFRAME ERA MUY LIMITADO CON LA CANTIDAD DE COLUMNAS.
	"""
		for idDoc, doc in enumerate(docsAZList):
			vector = np.zeros(nPalabras+1)
			vector[0]=idDoc
			#print "******************************"
			#print "DOCUMENTO: "+doc
			#print "******************************"
			for idPalabra, palabra in enumerate(diccionarioGralLista):
				#[idDoc][idPalabra]=>[palabra,frecuencia]
				frecuencia = cubo[idDoc][idPalabra][1]
				existencias = 0
				for x in xrange(0,docsAZCount):
					if int(cubo[x][idPalabra][1])>0:
						existencias+=1
				#Calculamos cada valor Wtd
				if int(docsAZCount+1)==int(existencias):
					print "ALERTA: "+palabra
				tf = float(frecuencia)
				delta = float(math.log10(docsAZCount))
				idf = math.log10((float(docsAZCount)+delta)/float(existencias))
				vector[idPalabra+1]=tf*idf

				#Creamos una key-value de palabra-existencias en el core, para guardarla en el HDFS y usarla en la similitud coseno en lugar de operar de nuevo.
				if(entro):
					palabraExistencias.append((palabra,existencias))
			entro = False
			#Unimos la nueva tupla (convertido en un dataframe) a nuestro dataframe general.
			if division==True:
				dataframe = spark.createDataFrame([vector.astype(float).tolist()], columnasWords)
				division=False
			else:
				df = spark.createDataFrame([vector.astype(float).tolist()], columnasWords)
				dataframe = dataframe.union(df)
			#Cada 100 documentos genera una nueva parte para guardarlo, este valor es variable segun la memoria del cluster.
			if (idDoc>0 and idDoc%100==0) or idDoc==(docsAZCount-1):
				print "Guardando nuevo documento como tupla en el dataframe en el HDFS como csv..."
				dataframe.coalesce(1).write.csv(path="hdfs://Master:9000/PROYECTO/DataSetCore/TFIDFMatrixDF",mode="append")#,header=True)
				print "Formateando Dataframe..."
				dataframe = spark.createDataFrame([temporalWords], columnasWords)
				division=True

		commands.getstatusoutput('hdfs dfs -rm -R /PROYECTO/DataSetCore/PalabraExistencias')
		sc.parallelize(palabraExistencias).coalesce(1).saveAsTextFile("hdfs://Master:9000/PROYECTO/DataSetCore/PalabraExistencias")

	"""
	print " FIN "
	print "***************************"

	sc.stop()