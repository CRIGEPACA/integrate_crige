#!/usr/bin/python
# -*- coding: utf-8 -*-
# Gatchemmprise.py (http://parler.marseillais.overblog.com/g) a été conçu pour les crigiennes et crigiens et éventuellement leurs collègues. Il s'utilise de préférence à l'ombre des pinèdes et ne craint pas les cris d'oiseaux. 
__author__ = 'mmaignan'

###########################################
###### IMPORT DES DIFFERENTS MODULES ######
###########################################

import sys
import os
import shutil

import psycopg2
import psycopg2.extras

import time
from datetime import date

from osgeo import gdal
from osgeo import osr
from osgeo import ogr
import arcpy
import dbf

import Tkinter
import tkFileDialog

###########################################
####### DEFINITION DES VARIABLES ##########
###########################################

#######GRASS environment variables (currently useless)#########
GISBASE= "C:\GRASS-64"
GISRC= "C:\Documents and Settings\user\.grassrc6"
LD_LIBRARY_PATH= "C:\GRASS-64\lib"
PATH= "C:\GRASS-64\etc;C:\GRASS-64\etc\python;C:\GRASS-64\lib;C:\GRASS-64\bin;C:\GRASS-64\extralib;C:\GRASS-64\msys\bin;C:\Python26;"
PYTHONLIB= "C:\Python27"
PYTHONPATH= "C:\GRASS-64\etc\python"
GRASS_SH= "C:\GRASS-64\msys\bin\sh.exe"


####Pg connection

class BasePostgis():
	def __init__(self):
		#-------------modifier ici le nom de la base pg (crige/crige_dev) pour la connexion prod-dev------------------------------
		self.connexionpg=psycopg2.connect("host=91.206.199.84 dbname='crige' user='postgres' password='ata1846cR1' port='5432'")
		#----------------------------------------------------------------------------------------
		#Résultats sous forme de tuple indéxé
		# self.curspg=self.connexionpg.cursor()
		#Résultats sous forme de dictionnaire python
		self.curspg=self.connexionpg.cursor(cursor_factory=psycopg2.extras.DictCursor)
	def execute (self,requete,fetch) :
		self.curspg.execute(requete)
		if fetch == 1:
			return self.curspg.fetchall()
		
	def close(self):
		self.curspg.close()
		self.connexionpg.close()
	def enregistre(self):
		self.connexionpg.commit()




####### ENTREPOT DE DONNEES #######

# Option 1 : Choix des dossiers E/O par l'utilisateur dans une fenêtre de dialogue
root = Tkinter.Tk()
#srcDir = tkFileDialog.askdirectory(parent=root,initialdir="/",title='Please select a source directory')
#destDir = tkFileDialog.askdirectory(parent=root,initialdir="/",title='Please select an output directory')

#Option 2 : copier-coller des chemins aux dossiers concernés
srcDir = "V:/bdd_metier/audat_ocsolge_v0/mos_scot_tpm_audat_2003_2011/data/livraison/arcgis/"
destDir = "V:/bdd_metier/audat_ocsolge_v0/mos_scot_tpm_audat_2003_2011/data/import/shp/"

# Constantes et paramètres

projIn = osr.SpatialReference()
projIn.ImportFromEPSG(2154)
projOut = osr.SpatialReference()
projOut.ImportFromEPSG(4326)

driverOgr = 'ESRI Shapefile'

bufferDistance = 50

basePg = BasePostgis()
resultPg = basePg.execute("SELECT id, libelle FROM _emprise_donnee ORDER BY id DESC limit 1",1)

lastBufferId = resultPg[0]
#lastBufferId = 360
basePg.close()

i = 1

###########################################
############ FONCTIONS UTILES #############
###########################################

#Pour corriger les problèmes d'encodage des attributs
reload(sys)
sys.setdefaultencoding("utf-8")

def transform_attr(text):
    return text.encode('ISO-8859-1').decode('UTF-8')

###########################################
########## PROGRAMME PRINCIPAL ############
###########################################

#Detection de la version de GDAL
version_num = int(gdal.VersionInfo('VERSION_NUM'))
if version_num < 1100000:
    sys.exit('ERROR: Python bindings of GDAL 1.10 or later required')

	
#On recupere le repertoire courant
currentDir = os.getcwd() + "/"
#On cree le repertoire d'accueil
traitDir = currentDir + "traitement/"
if os.path.isdir(traitDir):
    shutil.rmtree(traitDir)
os.makedirs(traitDir)
#Creation des sous repertoires
empriseDir = "emprise/"
#Création du fichier de log
pathFileLog = traitDir + "log_scan_shp.txt"
fileLog = open(pathFileLog, "w")
#Création du répertoire
os.makedirs(traitDir + empriseDir)

#Création du fichier d'import
today = date.today()
pathFile4pg = destDir + today.isoformat() +"_batch_shp2pg.sh"
file4pg = open(pathFile4pg, "w")
file4pg.write("#!/bin/bash\n\n#emprises $ donnees\n\n")

#On identifie le format source en vue d'une éventuelle conversion
#on convertit si besoin
#cmd.exe /C ogr2ogr.exe -f "ESRI Shapefile" -t_srs EPSG:2154 V:/bdd_metier/cove/acv_ocsol_maj_201606/DONNEES/shp/SCOT_ACV_2010_HAIES_l93.shp V:/bdd_metier/cove/acv_ocsol_maj_201606/DONNEES/SCOT_ACV_2010_HAIES.TAB SCOT_ACV_2010_HAIES 

#On choisit
driver = ogr.GetDriverByName(driverOgr)

#On scanne le répertoire de fichier et on ne retient que les *.SHP et on liste les DBF orphelins
lstNomFile = os.listdir(srcDir)
lstShpFile = []
lstDbfFile = []
lstOutFile = []


for file in lstNomFile:
    fileName = file.split('.')
    ext = fileName[1]

    pathFile =  srcDir + file
    buffertmp = destDir + 'tmp_' + file
    bufferfile = destDir + 'emprise_' + file
	
    datasource = driver.Open(pathFile)
    if datasource is None:
        fileLog.write("Impossible d'ouvrir le ShapeFile suivant : %s" % (file))
		
    if  ext == 'shp':
        lstShpFile.append(pathFile)
        print "début du traitement du fichier %s" % (file)
        datalayer = datasource.GetLayer()
        objetShp = datalayer.GetFeature(0)
        if objetShp is None:
            fileLog.write("ATTENTION : la couche %s ne comporte pas d'objets \n" % (file))
        else :
        #Recuperation du type de géométrie et du nombre d'objets
            geom = objetShp.GetGeometryRef()
            geomType = geom.GetGeometryName()
        if "POLYGON" in geomType:
            arcpy.Dissolve_management(pathFile, bufferfile, "", "", "MULTI_PART", "")
        else :
           arcpy.Buffer_analysis(pathFile, buffertmp, bufferDistance, "", "", "ALL")
           arcpy.Dissolve_management(buffertmp, bufferfile, "", "", "MULTI_PART", "DISSOLVE_LINES")
        
        print "emprise créée pour le fichier %s" % (file)

        file4pg.write('/usr/local/pgsql/bin/shp2pgsql -W "LATIN1" -s 310024140 -a -i -D -g the_geom /DATA/donnees_temp/integration/emprise_%s _emprise_donnee | /usr/local/pgsql/bin/psql -U postgres -d crige' % (file))
        file4pg.write('\n')
        file4pg.write('/usr/local/pgsql/bin/shp2pgsql -W "LATIN1" -s 310024140 -i -I -D -g the_geom /DATA/donnees_temp/integration/%s ' % (file))
        file4pg.write('%s | /usr/local/pgsql/bin/psql -U postgres -d crige' % (fileName[0]))
        file4pg.write('\n\n')
		
    if  ext == 'dbf':
        fileShpVerif =  srcDir + fileName[0] + '.shp'
        if not os.path.isfile(fileShpVerif):
            lstDbfFile.append(fileName[0])
			
fileLog.write("Il y a %s fichiers Shape à traiter\n\n" % (len(lstShpFile)))
			
if len(lstDbfFile) > 0:
    fileLog.write("ATTENTION : il y a %s fichiers DBF sans SHP associé\nVoici la liste des DBF concernés : " % (len(lstDbfFile)))
    fileLog.write(','.join(lstDbfFile) + '\n')
	
lstOutFile = os.listdir(destDir)

for o in lstOutFile:
    outputName = o.split('.')
    tabfile = destDir + outputName[0]+".dbf"
    ext = outputName[1]   
    if  ext == 'dbf' and "emprise_" in str(outputName[0]):
        i=i+1
        os.remove(tabfile)
        table = dbf.Table(tabfile, 'id N(3,0);libelle C(255)')
        table.open("read-write")
        if 'libelle C(255)' not in str(table):
            dbf.delete_fields(tabfile, 'libelle C(255)')
            dbf.add_fields(tabfile, 'libelle C(255)')
        #print "GFY"
		print lastBufferId
        idVal = lastBufferId+i
		print idVal
        libVal = "emprise de : "+str(o[8:-4])
        for record in ((str(idVal),libVal,), ):
            table.append(record)
        table.close()
        print "mise à jour attributaire de %s" % (outputName[0])
    if "tmp_" in str(outputName[0]):
        os.remove(destDir+o)
		
###Buffer via OGR (dissolve inopérant)
	#datalayer = datasource.GetLayer()
	
	#bufferlayer = driver.CreateDataSource(bufferfile)
	#bufferoutput = bufferlayer.CreateLayer(bufferfile,geom_type=ogr.wkbPolygon)
	#bufferdefntn = bufferoutput.GetLayerDefn()
	
	#for feature in datalayer:
		#layergeom = feature.GetGeometryRef()
		#layerbuffer = layergeom.Buffer(bufferDistance)
		#buffermerge = ogr.Geometry(ogr.wkbPolygon)
		#buffermerge = buffermerge.Union(layerbuffer)
		
		##layergeom = layerbuffer.GetGeometryRef()
		##layerbuffer = layergeom.Union()
		##bufferunion = buffergeom.Union(layerbuffer)
		
		#bufferfeat = ogr.Feature(bufferdefntn)
		#bufferfeat.SetGeometry(layerbuffer)
		#bufferfeat.SetGeometry(buffermerge)
		#bufferoutput.CreateFeature(bufferfeat)		

