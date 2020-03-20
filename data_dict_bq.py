#  IMPORTS
import xml.etree.ElementTree as ET
from google.cloud import bigquery
import glob
import re
import pandas as pd
import sys, getopt

import data_dict as dd


class dataDictBQ:

	def __init__(self, dict ):
		self.dict = dict

		self.bqTypes = {'string': 'STRING', 
			'char': 'STRING', 
			'integer': 'INTEGER', 
			'decimal': 'FLOAT', 
			'numeric': 'FLOAT', 
			'num': 'FLOAT', 
			'encoded value': 'STRING', 
			'decimal, encoded value': 'STRING', # Ouch! Need a complex datatype
			'integer, encoded value': 'STRING',
			'encoded values': 'STRING',
			'year': 'INTEGER'}

			

	def createBQTable(self, table_id, datafilepath):
		dataDict = self.dict
		
		# for future use?	
		unitAbbs = {'milliseconds': 'ms', 
			'pounds': 'lb', 
			'inches': 'in', 
			'beats per minute': 'bpm'} 
		
		#read column names from the tab delimited file
		datafile = open(datafilepath, 'r') 
		for x in range(10):
			datafile.readline()	
		colLine = datafile.readline()
		colLine =  colLine.rstrip()
		datafile.close()
		cols = colLine.split('\t')
		print (cols)

	
		schema = []
		for col in cols:
			if col.lower() == 'dbgap_subject_id':
				schema.append(bigquery.SchemaField("dbGaP_Subject_ID", "STRING", mode="REQUIRED",description="Unique Subject ID in dbGap"))
			elif col.lower() == 'dbgap_sample_id':
				schema.append(bigquery.SchemaField("dbGaP_Sample_ID", "STRING", mode="REQUIRED",description="Unique Sample ID in dbGap"))
			elif col.lower() == 'biosample_accession' or col.lower() == 'biosample accession':
				schema.append(bigquery.SchemaField("BioSample_Accession", "STRING", mode="NULLABLE",description="Accession no in BioSample"))
			else:
				var = dataDict.getVar(col)
				name = var.find('name').text
				# fix name to be a legal BQ column name (just . so far)
				name = name.replace(".", "_")
				desc = var.find('description').text
				typeNode = var.find('type')
				if typeNode is None:
					type = 'string'
				else :
					type = typeNode.text
				# Get the unit so we can 			
				unitNode = var.find('unit')
				if unitNode is not None:
					unit = unitNode.text
					if unit.lower() not in desc.lower():
						desc = desc + ' ('+unit+')'	
				type = type.lower()
				schema.append(bigquery.SchemaField(name, self.bqTypes[type], mode="NULLABLE",description=desc))

		client = bigquery.Client()
		table = bigquery.Table(table_id, schema=schema)
		table.description = dataDict.getTableDesc()
		table = client.create_table(table, exists_ok=True)  # API request
		print(
			"Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
		)
		return client, table

	def loadBQTable(self, client, table_ref, filename):
	
		job_config = bigquery.LoadJobConfig()
		job_config.source_format = bigquery.SourceFormat.CSV
		job_config.skip_leading_rows = 11
		job_config.autodetect = False
	#	job_config.allowJaggedRows = True
		job_config.field_delimiter = '\t'
	
		with open(filename, "rb") as source_file:
			job = client.load_table_from_file(
				source_file,
				table_ref,
				location="US",  # Must match the destination dataset location.
				job_config=job_config,
			)  # API request
	
		job.result()  # Waits for table load to complete
	
		print("Loaded {} rows into {}.".format(job.output_rows, table_ref))

	#%% 


    


	
	

	
	









