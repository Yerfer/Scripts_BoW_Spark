"""scriptKeysDocs.py"""
# -*- coding: utf-8 -*- #AVERIGUAR PARA QUE FUNCIONE LAS TILDES Y ENYES XD
import findspark
import commands
findspark.init()
import pyspark
from pyspark import SparkContext

if __name__ == "__main__":

	sc = SparkContext(appName="Creating KeysDocs")

	"""Cargamos todos los documentos que se encuentren en el directorio como un RDD tipo clave-valor, donde la clave es el PATH
	del fichero cargado y el valor es todo su contenido textual
	por ejemplo: ["hdfs://.../hola.txt","hola mundo y toda la informacion que contenga..."]
	"""
	corpusRaw = sc.wholeTextFiles("hdfs://Master:9000/YERFER/temporal/prueba/reutersprueba")
	
	rta = commands.getstatusoutput('hdfs dfs -rm -R /YERFER/temporal/prueba/reuterspruebaCORE/KeysDocs/')
	#El error 256 representa que esta tratando de eliminar un directorio que no existe, si no existe pues lo crea y ya.
	if rta[0] ==  0 or rta[0] ==  256:
		corpusRaw.keys().coalesce(1).saveAsTextFile("hdfs://Master:9000/YERFER/temporal/prueba/reuterspruebaCORE/KeysDocs")
		print ("Creacion del fichero con las keys (nombres de todos los documentos) terminado. ")
	else:
		print "Error: "+str(rta[0])+"\nDescripcion: "+rta[1]+" ."

	sc.stop()