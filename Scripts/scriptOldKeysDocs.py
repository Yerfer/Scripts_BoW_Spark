"""scriptOldKeysDocs.py"""
# -*- coding: utf-8 -*- #AVERIGUAR PARA QUE FUNCIONE LAS TILDES Y ENYES XD
import commands
import findspark
findspark.init()
import pyspark
from pyspark import SparkContext

if __name__ == "__main__":

	sc = SparkContext(appName="Creating_Verifying OldKeysDocs")

	rta = commands.getstatusoutput('hdfs dfs -mkdir /PROYECTO/DataSetCore/OldKeysDocs/')
	if rta[0] ==  0:
		print ("OldKeysDocs inexistente por lo que se crea uno nuevo vacio junto con su archivo part-00000 tambien vacio. ")
		commands.getstatusoutput('hdfs dfs -touchz /PROYECTO/DataSetCore/OldKeysDocs/part-00000')
	else:
		if rta[0]==256:
			var = commands.getstatusoutput('hdfs dfs -touchz /PROYECTO/DataSetCore/OldKeysDocs/part-00000')
			#El error 256 representa para este caso, que el archivo vacio que se quiere crear existe y no es vacio.
			print ("Archivo part-00000 inexistente se crea uno vacio.") if var[0]==0 else "Archivo part-00000 Existente."
		else:
			print ("Error: "+str(rta[0])+"\nDescripcion: "+rta[1]+" .")
	

	sc.stop()