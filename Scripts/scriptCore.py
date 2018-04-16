"""scriptCore.py"""
import commands
import re
import findspark
findspark.init()
import pyspark
from pyspark import SparkContext
import operator 

if __name__ == "__main__":

	print "Creando el CORE de todos y cada uno de los documentos..."

	sc = SparkContext(appName="Creating Clean Core")

	keysDocs = sc.textFile("hdfs://Master:9000/PROYECTO/DataSetCore/KeysDocs/part-00000")
	oldKeysDocs = sc.textFile("hdfs://Master:9000/PROYECTO/DataSetCore/OldKeysDocs/part-00000")
	nuevosDocs = keysDocs.subtract(oldKeysDocs)
	palabraslimpias = sc.parallelize([])

	if nuevosDocs.count()==0:
		print "No hay nuevos documentos para procesar T_T"
	else:		
		print "Hay nuevos documentos para procesar y agregar al Core!!!"
		patronNombre = re.compile('(.*)(\/)')
		patron2=re.compile('([^a-zA-Z]+\.)|(^\.|\.$|(\s+\.\s+))|(([^a-zA-Z.]))')
		patronURLSValidas = re.compile('(^(www.|(ht|f)tps?:\/\/)((\w*\.\w*)(.*)+)([^\.(\s*|\n*)+]))')
		patronQuitarCharInv = re.compile('([^a-zA-Z]+)|((ht|f)tps?|www)|(\.+(com?|org|info|gov)\.*)|([^\w]+\w[^\w]+)')
		patronSpace = re.compile('\s+')
		patron1=re.compile('\s+')
		patronPath = re.compile('hdfs:\/\/Master:9000')
		nuevosDocsList = nuevosDocs.collect()
		tamano = len(nuevosDocsList)


		for count, pathDocumento in enumerate(nuevosDocsList):
			documento = sc.textFile(pathDocumento)
			#Obtener el nombre del archivo por split
			#nombreVector = pathDocumento.split("/")
			#nombre = nombreVector[len(nombreVector)-1]
			#Obtener el nombre del archivo por RE			
			nombre = re.sub(patronNombre, "",pathDocumento) 
			palabrasRDD = documento.flatMap(lambda line : patron1.split(line)).filter(lambda x : len(x) > 0)
			coreDoc = palabrasRDD.map(lambda word : re.sub(patronQuitarCharInv, " ",word.lower())+" ").filter(lambda x : len(x) > 1).flatMap(lambda line : patron1.split(line)).filter(lambda x : len(x) > 0)
				

			if(not coreDoc.isEmpty()):
				#coreDoc = palabraslimpias.map(lambda x : x+" ").reduce(operator.concat)
				rta = commands.getstatusoutput("hdfs dfs -rm -R /PROYECTO/DataSetCore/Core/"+nombre+"")
				#El error 256 representa que esta tratando de eliminar un directorio que no existe, si no existe pues lo crea y ya.
				if rta[0] ==  0 or rta[0] ==  256:
					#sc.parallelize([coreDoc]).coalesce(1).saveAsTextFile("hdfs://Master:9000/PROYECTO/DataSetCore/Core/"+nombre)
					coreDoc.coalesce(1).saveAsTextFile("hdfs://Master:9000/PROYECTO/DataSetCore/Core/"+nombre)
					if count%100==0:
						print "Van "+str(count+1)+" documentos..."
					if count==tamano-1:
						print "Son en total "+str(count+1)+" documentos."
										#print "----    *************      ------"
						#print ("Documento limpiado y agregado al CORE.")
						#print "----    *************      ------"
					#else:
					#	print "----    *************      ------"
					#	print ("Error: "+str(rta[0])+"\nDescripcion: "+rta[1]+" .")
					#	print "----    *************      ------"
			else:
				commands.getstatusoutput("hdfs dfs -rm "+re.sub(patronPath, "",pathDocumento))
				print "Documento: "+nombre+" eliminado del DataSet por no cumplir con las condiciones necesarias."


	if nuevosDocs.count()==0:
		print "Archivo keysDocs en OldKeysDocs al dia."

	print "\t FIN "
	sc.stop()