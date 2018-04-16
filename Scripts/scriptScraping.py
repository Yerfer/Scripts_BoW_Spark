from bs4 import BeautifulSoup
import requests
import commands

if __name__ == "__main__":
    URL = "http://textfiles.com/directory.html"
    req = requests.get(URL)
    status_code = req.status_code
    if status_code == 200:
        html = BeautifulSoup(req.text, "html.parser")
        entradas = html.find_all('b')
        titulos = []
        for i, entrada in enumerate(entradas):
            if entrada.find('a')!=None:
                titulos.append(entrada.find('a').get('href'))

        for titulo in titulos:
            print ("Descargando desde este lado "+titulo)
            req = requests.get("http://textfiles.com/"+titulo)
            status_code = req.status_code
            if status_code == 200:
                html = BeautifulSoup(req.text, "html.parser")
                entradas1 = html.find_all('b')
                for i, entrada in enumerate(entradas1):
                    if entrada.find('a')!=None:
                        doc = entrada.find('a').get('href')
                        commands.getoutput('wget http://textfiles.com/'+titulo+"/"+doc)
    else:
        print "Status Code %d" % status_code