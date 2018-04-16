"""scriptCorpus.py"""
# -*- coding: utf-8 -*- #AVERIGUAR PARA QUE FUNCIONE LAS TILDES Y ENYES XD
import commands
import re
import findspark
findspark.init()
import pyspark
from pyspark import SparkContext
import operator 

if __name__ == "__main__":
	def actualizarOld(keysDocsUPDATE):
		print "Actualizando el archivo keysDocs en OldKeysDocs: "
		rta = commands.getstatusoutput('hdfs dfs -rm -R /PROYECTO/DataSetCore/OldKeysDocs/')
		#El error 256 representa que esta tratando de eliminar un directorio que no existe, si no existe pues lo crea y ya.
		if rta[0] ==  0 or rta[0] ==  256:
			keysDocsUPDATE.coalesce(1).saveAsTextFile("hdfs://Master:9000/PROYECTO/DataSetCore/OldKeysDocs")
			print ("Actualizacion terminada.")
		else:
			print "Error: "+str(rta[0])+"\nDescripcion: "+rta[1]+" ."

	def unir(palabra):
		palabra=palabra+" ,"
		return palabra

	"""
	#La idea es que se detecte que existe previamente un Spark Context y la detenga para poder iniciar este script 
	#y crear el spark context correspondiente.
	if SparkContext().sparkUser()!=None:
		SparkContext().stop()
		pass"""
	
	sc = SparkContext(appName="Creating Corpus")

	keysDocs = sc.textFile("hdfs://Master:9000/PROYECTO/DataSetCore/KeysDocs/part-00000")
	#oldKeysDocs = sc.textFile("hdfs://Master:9000/PROYECTO/DataSetCore/OldKeysDocs/part-00000")
	#nuevosDocs = keysDocs.subtract(oldKeysDocs)
	palabrasAcu = sc.parallelize(["a"])

	#PARA MAS ADELANTE.
	#if nuevosDocs.count()==0:
	if keysDocs.count()==0:
		print "No hay nuevos documentos para cargar T_T"
	else:		
		print "Hay nuevos documentos para procesar y agregar sus palabras al Corpus General!!!"
		#print nuevosDocs.collect()
		print "Creando/Actualizando Corpus General..."

		patronNombre = re.compile('(.*)(\/)')
		patronSpace = re.compile('\s+')
		vectorNews = keysDocs.collect()

		print "Tamano: "+str(len(vectorNews))

		#for pathDocumento in vectorNews[:]:
			#Obtener el nombre del archivo por split
			#nombreVector = pathDocumento.split("/")
			#nombre = nombreVector[len(nombreVector)-1]
			#Obtener el nombre del archivo por RE			
		#	nombre = re.sub(patronNombre, "",pathDocumento) 
			#print "Docuemtno: "+str(nombre)+", path: "+pathDocumento
		#	palabrasRDD = sc.textFile("hdfs://Master:9000/PROYECTO/DataSetCore/Core/"+nombre+"/part-00000").filter(lambda x : len(x) > 1)	
			#palabrasRDD = documento.flatMap(lambda line : patronSpace.split(line)).filter(lambda x : len(x) > 0)
			#patron2=re.compile('([^a-zA-Z]+\.)|(^\.|\.$|(\s+\.\s+))|(([^a-zA-Z.]))')
			#patronURLSValidas = re.compile('(^(www.|(ht|f)tps?:\/\/)((\w*\.\w*)(.*)+)([^\.(\s*|\n*)+]))')
			#patronQuitarCharInv = re.compile('([^a-zA-Z]+)|((ht|f)tps?|www)|(\.+(com?|org|info|gov)\.*)|([^\w]+\w[^\w]+)')			
			#.map(lambda word : re.sub(patronQuitarCharInv, " ",word.lower())+" ").filter(lambda x : len(x) > 1).flatMap(lambda line : patronSpace.split(line)).filter(lambda x : len(x) > 0)
		#	palabrasAcu = palabrasAcu.union(palabrasRDD)	

		rdds = [sc.textFile("hdfs://Master:9000/PROYECTO/DataSetCore/Core/"+re.sub(patronNombre, "",pathDocumento)+"/part-00000").filter(lambda x : len(x) > 1) for pathDocumento in vectorNews]
		rdd = sc.union(rdds)

		"""
		ESTO ESTA PENSADO PARA FUTURO...
		#Cargamos las palabras que estuvieran antes. Importante que este directorio este creado, asi sea en blanco.
		rta1 = commands.getstatusoutput('hdfs dfs -mkdir /PROYECTO/DataSetCore/CorpusGral')
		rta1 = commands.getstatusoutput('hdfs dfs -touchz /PROYECTO/DataSetCore/CorpusGral/part-00000')
		#El error 256 representa que esta tratando de eliminar un directorio que no existe, si no existe pues lo crea y ya.
		print ("Corpus General inexistente por lo que se crea uno vacio...") if rta1[0] ==  0 else "Cargando Corpus General Existente..."
		if rta1[0] == 0:
			corpusOLD = sc.parallelize([])
		else:	
			corpusOLD = sc.textFile("hdfs://Master:9000/PROYECTO/DataSetCore/CorpusGral/part-00000").filter(lambda x : len(x) > 0)#.flatMap(lambda line : patronSpace.split(line)).filter(lambda x : len(x) > 0)
		corpus = palabrasAcu.union(corpusOLD)
		#corpus = palabrasAcu.map(lambda x : x+" ")
		#corpus = corpus2.reduce(operator.concat)
		#if rta1[0]!=0:
		ESTO ESTA PENSADO PARA FUTURO...
		"""
		#print "Tamano del Corpus: "+str(palabrasAcu.count())
		#corpus = palabrasAcu.filter(lambda x : len(x) > 2)	
		#corpus = corpus2.reduce(operator.concat)

		print "Cantidad de palabras: "+str(rdd.count())

		rta = commands.getstatusoutput('hdfs dfs -rm -R /PROYECTO/DataSetCore/CorpusGral')		
		#El error 256 representa que esta tratando de eliminar un directorio que no existe, si no existe pues lo crea y ya.
		#if rta[0] ==  0 or rta[0] ==  256:
			#sc.parallelize([corpus]).coalesce(1).saveAsTextFile("hdfs://Master:9000/PROYECTO/DataSetCore/CorpusGral")
		print "Guardando Core completo en el HDFS..."
			#corpus.coalesce(1).saveAsTextFile("hdfs://Master:9000/PROYECTO/DataSetCore/CorpusGral")
		rdd.saveAsTextFile("hdfs://Master:9000/PROYECTO/DataSetCore/CorpusGral")
		#palabrasAcu.coalesce(1).saveAsTextFile("hdfs://Master:9000/PROYECTO/DataSetCore/CorpusGral")
		print ("Creacion del Corpus General terminado. ")# if rta1[0] == 0 else  "Actualizacion del Corpus Gral con los nuevos documentos terminado."
		#else:
		#	print ("Error: "+str(rta[0])+"\nDescripcion: "+rta[1]+" .")


	"""
	PARA MAS ADELANTE...
	if nuevosDocs.count()==0:
		print "Archivo keysDocs en OldKeysDocs al dia."
	else:		
		actualizarOld(keysDocs)	
	PARA MAS ADELANTE...
	"""

	print "\t FIN "
	sc.stop()