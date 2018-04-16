"""scriptOldKeysDocs.py"""
# -*- coding: utf-8 -*- #AVERIGUAR PARA QUE FUNCIONE LAS TILDES Y ENYES XD
import commands
import findspark
findspark.init()
import pyspark
from pyspark import SparkContext

if __name__ == "__main__":


	"""
	#La idea es que se detecte que existe previamente un Spark Context y la detenga para poder iniciar este script 
	#y crear el spark context correspondiente.
	if SparkContext().sparkUser()!=None:
		SparkContext().stop()
		pass"""
	
	sc = SparkContext(appName="Creating/Verifying OldKeysDocs")


	rta = commands.getstatusoutput('hdfs dfs -mkdir /YERFER/temporal/prueba/reuterspruebaCORE/OldKeysDocs/')
	#El error 256 representa que esta tratando de eliminar un directorio que no existe, si no existe pues lo crea y ya.
	if rta[0] ==  0:
		print ("OldKeysDocs inexistente por lo que se crea uno nuevo vacio junto con su archivo part-00000 tambien vacio. ")
		commands.getstatusoutput('hdfs dfs -touchz /YERFER/temporal/prueba/reuterspruebaCORE/OldKeysDocs/part-00000')
	else:
		if rta[0]==256:
			if commands.getstatusoutput('hdfs dfs -test -e /YERFER/temporal/prueba/reuterspruebaCORE/OldKeysDocs/part-00000')!=0:
				print ("Archivo part-00000 inexistente se crea uno vacio.")
				commands.getstatusoutput('hdfs dfs -touchz /YERFER/temporal/prueba/reuterspruebaCORE/OldKeysDocs/part-00000')	
		else:
			print "Error: "+str(rta[0])+"\nDescripcion: "+rta[1]+" ."


	sc.stop()