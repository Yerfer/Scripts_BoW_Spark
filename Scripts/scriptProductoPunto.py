"""scriptProductoPunto.py"""
# -*- coding: utf-8 -*-
import findspark
import commands
findspark.init()
import pyspark
import operator 
import re
from pyspark import SparkContext
from pyspark.sql import SparkSession
import sys
import math
import operator 
from IPython.display import display, HTML
import urllib2
from urllib2 import Request, urlopen, URLError, HTTPError
import httplib
import threading
import Queue
import time

if __name__ == "__main__":

	if len(sys.argv)<3:
		print 'Faltan Argumentos, son 2: -(p|k|u) "(path|words|url)".'
		sys.exit()
	if len(sys.argv)>3:
		print sys.argv #-p -k -u "/home/hduser/News/hola.txt"
		print "Se recibe solo 2 parametros, la consulta debe ir entre comillas (e.g. -k \"\xc2como usar el buscador\xc2\")."
		print "\t-p (Path de algun documento a comparar)\n\t-k (KeyWord o palabras)\n\t-u (Url's de algun pagina web a comparar)"
		sys.exit(0) #Finaliza normal.
		#sys.exit(1) #Finaliza a modo de error.		
	
	opcion = sys.argv[1]
	if opcion=="-p":
		try:
			file = open(sys.argv[2], 'r')
			consulta = file.read()
			file.close()
		except IOError:
			print("Error en la lectura del archivo: {0}".format(IOError))
			sys.exit(0)
	elif opcion=="-k":
		consulta = sys.argv[2]
	elif opcion=="-u":
		req = urllib2.Request(sys.argv[2])
		try: 
			consulta = urlopen(req).read()
		except HTTPError, e:
			print ('HTTPError = '+"\n"+str(e)+"\nCodigo: "+ str(e.code)+"\nRazon: "+ str(e.reason))
			sys.exit(0)
		except URLError, e:
			print('URLError = '+"\n"+str(e)+"\nCodigo: "+ str(e.code)+"\nRazon: "+ str(e.reason))
			print "ERROR AL CONECTARSE A INTERNET, REVISE SU CONEXION."
			sys.exit(0)
		except httplib.HTTPException, e:
			print('HTTPException = '+"\n"+str(e)+"\nCodigo: "+ str(e.code)+"\nRazon: "+ str(e.reason))
			sys.exit(0)
		except Exception,e :
			print('ERROR AL CARGAR LA PAGINA.'+"\n"+str(e))
			sys.exit(0)
	else :
		print "Opcion Erronea.\n"
		sys.exit(0)


	print "Computing Cosine similarity..."
	sc = SparkContext(appName="Computing Cosine similarity")
	spark = SparkSession.builder.getOrCreate()
	patronVector = re.compile("[^(\d|\.)]")
	docsAZ = sc.textFile("hdfs://Master:9000/PROYECTO/DataSetCore/IdDoc/DocsAZ/part-00000")
	docsAZList = docsAZ.collect()
	docsAZCount = len(docsAZList) 																		#ESTO SE DEBE HACER PARA CONVERTIR EL STRING -> Key-Value
	tfIDFMatrixKV = sc.textFile('hdfs://Master:9000/PROYECTO/DataSetCore/TFIDFMatrixDF/part*').map(lambda x: (x.split(",")[0][1:] , x.split("]")[0].split(",")[1:])).sortByKey()
	tfIDFMatrixKV.cache()

	#Cargamos, limpiamos y particionamos la consulta en filas de un RDD.
	patronQuitarCharInv = re.compile('([^a-zA-Z]+)|([^\w]+\w[^\w]+)')
	#consultaRDD = sc.parallelize([consulta]).map(lambda line: re.sub(patronQuitarCharInv, " ",line.lower())).flatMap(lambda palabra: palabra.split(" ")).filter(lambda x: len(x)>1).map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)

	#Cargamos el diccionario y lo pasamos a key-value.
	diccionarioGralRDD = sc.textFile("hdfs://Master:9000/PROYECTO/DataSetCore/DiccionarioGralAZ/part-00000").map(lambda x: (x,0))

	#Hacemos un rightOuterJoin del la consulta y del diccionario, luego operamos sus values haciendo una suma filtrando los None (serian las palabras que no tenian en comun).
	#diccConsulta = consultaRDD.rightOuterJoin(diccionarioGralRDD).mapValues(lambda palabra: sum(list(filter(bool, palabra)))).sortByKey()

	#Cargamos el par Palabra-Existencias como un RDD limpio
	patronPalabra = re.compile('(\(u\')|(\',.*)')
	patronExist = re.compile('([^\d])')
	palabraExistencias = sc.textFile("hdfs://Master:9000/PROYECTO/DataSetCore/PalabraExistencias/part-00000").map(lambda x:  (re.sub(patronPalabra, "", x), re.sub(patronExist, "", x) ) )

	#Creamos los Broadcast para que todos puedan operar con el par palabra-existencia
	palabraExistenciasBC = sc.broadcast(dict(palabraExistencias.collect()))
	docsAZCountBS = sc.broadcast(docsAZCount)
	deltaBS = sc.broadcast(math.log10(docsAZCount))
	#tfIDFConsulta = diccConsulta.map(lambda x: ( x[0] , x[1]*math.log10((docsAZCountBS.value+deltaBS.value)/float(palabraExistenciasBC.value[x[0]]) ) ) )#.sortByKey()
	#TIEMPO INICIAL
	punto_inicial = time.time()
	#Cargamos, limpiamos y particionamos la consulta en filas de un RDD.
	consultaRDD = sc.parallelize([consulta]).map(lambda line: re.sub(patronQuitarCharInv, " ",line.lower())).flatMap(lambda palabra: palabra.split(" ")).filter(lambda x: len(x)>1).map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)
	#Hacemos un rightOuterJoin del la consulta y del diccionario, luego operamos sus values haciendo una suma filtrando los None (serian las palabras que no tenian en comun).
	diccConsulta = consultaRDD.rightOuterJoin(diccionarioGralRDD).mapValues(lambda palabra: sum(list(filter(bool, palabra)))).sortByKey()
	#Creamos los Broadcast para que todos puedan operar con los datos
	tfIDFConsulta = diccConsulta.map(lambda x: ( x[0] , x[1]*math.log10((docsAZCountBS.value+deltaBS.value)/float(palabraExistenciasBC.value[x[0]]) ) ) )#.sortByKey()
	tfIDFConsultaBC = sc.broadcast(tfIDFConsulta.values().collect())

	def fpunto(doc):
		acu=0.0
		for id,x in enumerate(doc[1]):
			acu+=(float(x)*float(tfIDFConsultaBC.value[id]))
		return (doc[0], acu )

	similitudCoseno = tfIDFMatrixKV.map(fpunto).filter(lambda x: x[1]>0).sortBy(lambda x: x[1], ascending=False)
	punto_intermedio = time.time()

	patronNombre = re.compile('(.*/)')
	titulo = "********* RESULTADOS DE LA BUSQUEDA *********"
	display(HTML("<h1>"+titulo+"</h1>"))
	for doc in similitudCoseno.collect()[:10]:
		nombre = re.sub(patronNombre, "", docsAZList[int(float(doc[0]))])
		docOriginal = sc.textFile("hdfs://Master:9000/PROYECTO/DataSet/"+nombre).reduce(operator.concat)[0:500]
		url = "http://master:50070/webhdfs/v1/PROYECTO/DataSet/"+nombre+"?op=OPEN"
		display(HTML("<a href='"+url+"' target='_blank'><h2>"+nombre+"</h2></a>"))
		display(HTML("<h3>"+"\nRelevancia por SC: "+str(doc[1])+"</h3>"))
		display(HTML('<p><textarea readonly rows="5" cols="120" style="resize: vertical; max-height: 200px; min-height: 100px;">"'+docOriginal+'..."</textarea></p>'))
	punto_final = time.time()
	print " FIN "
	print "***************************"
	print "Tiempo de procesamiento (Busqueda): "+str(punto_intermedio - punto_inicial)+" s"
	print "Tiempo de Impresion de Resultados: "+str(punto_final - punto_intermedio)+" s"
	print "Tiempo TOTAL: "+str(punto_final - punto_inicial)+" s"
	print "***************************"

	sc.stop()

