#!/usr/bin/python
# -*- coding: utf-8 -*-
import psycopg2
import psycopg2.extras

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


basePg = BasePostgis()
resultat = basePg.execute("SELECT id, libelle FROM _emprise_donnee ORDER BY id DESC limit 10",10)

print resultat[]

basePg.close()

