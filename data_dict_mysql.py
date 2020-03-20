#  IMPORTS
import xml.etree.ElementTree as ET
#import glob
from collections import defaultdict
from datetime import datetime
#from elasticsearch import Elasticsearch
import mysql.connector
from mysql.connector.cursor import MySQLCursorPrepared
from mysql.connector import Error
from mysql.connector import errorcode
from data_dict import dataDict
import data_dict_bq as ddbq 


class dataDictMySql:

	def __init__(self, host, db, username, pword ):
		try:
			self.cnx = mysql.connector.connect(user=username, password=pword,
                              host=host,
                              database=db,
                              use_pure=True)
			# keep this cursor alive over function calls
			self.varcursor = self.cnx.cursor(prepared=True)
            
		except mysql.connector.Error as error :
			print("Failed to connect {}".format(error))
		self.ddbx = ddbq.dataDictBQ('')
			
	def __del__(self):
		self.varcursor.close()
		self.cnx.close()	

	def commit():
		self.cnx.commit()
			
	def addStudy(self, study_version_id, study_name, dataset):
		
		try:
			cursor = self.cnx.cursor(cursor_class=MySQLCursorPrepared)

			tInsert = 'INSERT INTO s_study (study_version_id, study_name, bq_dataset_name) VALUES (%s,%s,%s)'
			result  = cursor.execute(tInsert, (study_version_id, study_name, dataset))
			self.cnx.commit()
						
		except mysql.connector.Error as error :
			self.cnx.rollback()
			print("Failed to insert study into MySQL table {}".format(error))
		
		finally:
			cursor.close()
			#self.cnx.close()
				
	def addTable(self, dict, tname):
		tableAtts = dict.theDict.attrib
		print (tableAtts['id'])
		print (tableAtts['study_id'])
		
		try:
			cursor = self.cnx.cursor(cursor_class=MySQLCursorPrepared)

			tInsert = 'INSERT INTO s_table (table_version_id, study_version_id, table_name) VALUES (%s,%s,%s)'
			result  = cursor.execute(tInsert, (tableAtts['id'], tableAtts['study_id'], tname))
			insertString = 'INSERT INTO s_variable (table_version_id, table_name, variable_id, variable_name, variable_type) VALUES (%s, %s,%s,%s,%s)'
			for var in dict.theDict.findall('./variable'):
				vname = var.find('name').text
				# fix name to be a legal BQ column name (just . so far)
				bqname = vname.replace(".", "_")
				typeNode = var.find('type')
				if typeNode is None:
					type = ''
					bqtype = 'string'
				else :
					type = typeNode.text
					type = type.lower()
					bqtype = self.ddbx.bqTypes[type]

				self.addVariable(tableAtts['id'], tname, var.attrib['id'],
				vname, var.find('./type').text, bqname, bqtype)
				
			self.cnx.commit()
						
		except mysql.connector.Error as error :
			self.cnx.rollback()
			print("Failed to insert table into MySQL table {}".format(error))
		
		finally:
			cursor.close()
			#self.varcursor.close()
			#self.cnx.close()
				
	def addTableOnly(self, dict, tname):
		tableAtts = dict.theDict.attrib
		print (tableAtts['id'])
		print (tableAtts['study_id'])
		
		try:
			cursor = self.cnx.cursor(cursor_class=MySQLCursorPrepared)

			tInsert = 'INSERT INTO s_table (table_version_id, study_version_id, table_name) VALUES (%s,%s,%s)'
			result  = cursor.execute(tInsert, (tableAtts['id'], tableAtts['study_id'], tname))
			
			self.cnx.commit()
									
		except mysql.connector.Error as error :
			self.cnx.rollback()
			print("Failed to insert table into MySQL table {}".format(error))
		
		finally:
			cursor.close()
			#self.varcursor.close()
			#self.cnx.close()
				
	def addVariable(self, table_version_id, table_name, variable_id, variable_name, variable_type, column_name, column_type):
		
		try:
			insertString = 'INSERT INTO s_variable (table_version_id, table_name, variable_id, variable_name, variable_type, column_name, column_type) VALUES (%s,%s,%s,%s,%s,%s,%s)'

			result  = self.varcursor.execute(insertString, (
			table_version_id, table_name, variable_id, variable_name, variable_type, column_name, column_type))
			#self.cnx.commit()
						
		except mysql.connector.Error as error :
			self.cnx.rollback()
			print("Failed to insert variable into MySQL table {}".format(error))
		
#		finally:
			#self.varcursor.close()
			#self.cnx.close()
				
	def addVocab(self, dict, variableID, vocabID):
		print (variableID)
		var = dict.theDict.find(".//variable/[@id='"+variableID+"']")
		print (var)
		
		try:
			cursor = self.cnx.cursor(cursor_class=MySQLCursorPrepared)

			vocabInsert = 'INSERT INTO vocab (vocab_id, vocab_name) VALUES (%s,%s)'
			result  = cursor.execute(vocabInsert, (vocabID,variableID))


			insertString = 'INSERT INTO vocab_term (vocab_id, value_id, valueString) VALUES (%s,%s,%s)'
			cursor2 = self.cnx.cursor(prepared=True)
			for val in var.findall('./value'):
				result  = cursor2.execute(insertString, (vocabID, val.get('code'), val.text,))
		
			self.cnx.commit()
						
		except mysql.connector.Error as error :
			self.cnx.rollback()
			print("Failed to insert vocab into MySQL table {}".format(error))
		
		finally:
			cursor.close()
			cursor2.close()
			#self.cnx.close()
				
	def addMap(self, dict, variableID, vocabID, mapID, toScheme, toSchemeID):
		var = dict.theDict.find(".//variable/[@id='"+variableID+"']")
		
		try:
			cursor = self.cnx.cursor(cursor_class=MySQLCursorPrepared)

			mapInsert = "INSERT INTO mapping (map_id, from_scheme, to_scheme, from_vocab_id, to_vocab_id) VALUES (%s, %s, %s, %s, 2000)"	
			result  = cursor.execute(mapInsert, (mapID, variableID, toScheme, vocabID, toSchemeID))

			mapValInsert = 'INSERT INTO valuemap (map_id, valueString) VALUES (%s,%s)'
			for val in var.findall('value'):
				result  = cursor.execute(mapValInsert, (mapID, val.get('code'),))
		
			self.cnx.commit()
						
		except mysql.connector.Error as error :
			self.cnx.rollback()
			print("Failed to insert mapping into MySQL table {}".format(error))
		
		finally:
			cursor.close()
			#self.cnx.close()