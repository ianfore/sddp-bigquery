#  IMPORTS
import xml.etree.ElementTree as ET
from google.cloud import bigquery
import glob
import re
import pandas as pd
import sys, getopt

class dataDict:

	def __init__(self, xmlfile):
		# read the data dictionary
		tree = ET.parse(xmlfile)
		self.theDict = tree.getroot()
		self.vars = {}
		for var in self.theDict.findall('variable'):
			self.vars[var.find('name').text] = var

	
	def decode(self, exptPkg, variable):
		code = exptPkg[variable]
		xp = "variable[name='"+variable+"']/value[@code='"+code+"']"
		valueDef = self.theDict.findall(xp)
		return valueDef[0].text

	def getVar(self, varName):
		return self.vars[varName]

	def getTableDesc(self):
		return self.theDict.find('description').text


def createBQTable(table_id, dataDict, datafilepath):
	bqTypes = {'string': 'STRING', 
		'char': 'STRING', 
		'integer': 'INTEGER', 
		'decimal': 'FLOAT', 
		'numeric': 'FLOAT', 
		'num': 'FLOAT', 
		'encoded value': 'STRING', 
		'decimal, encoded value': 'STRING', # Ouch! Need a complex datatype
		'encoded values': 'STRING',
		'year': 'INTEGER'}
		
	# for future use?	
	unitAbbs = {'milliseconds': 'ms', 
		'pounds': 'lb', 
		'inches': 'in', 
		'beats per minute': 'bpm'} 
		
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
			schema.append(bigquery.SchemaField(name, bqTypes[type], mode="NULLABLE",description=desc))
		
	client = bigquery.Client()

	

	table = bigquery.Table(table_id, schema=schema)
	table.description = dataDict.getTableDesc()
	table = client.create_table(table)  # API request
	print(
    	"Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
	)
	return client, table

def loadBQTable(client, table_ref, filename):
	
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

def usage():
	print (sys.argv[0] +' -w workdir -p project -d dataset -t table')
	
def main(argv):
	workdir = ''
	project = ''
	dataset = ''
	table = '*'
#	workdir = '/Users/forei/ncbi/dbGaP-11218/files/'
#	project = 'isbcgc-216220'
#	dataset = 'MESA'

#	workdir = '/Users/forei/ncbi/dbGaP-11218/files/MESA_ESP_HeartGO/'
#	dataset = 'MESA_ESP_HeartGO'
#	table = 'Subject_Phenotypes'

# 	workdir = '/Users/forei/ncbi/dbGaP-14565/files/'
# 	project = 'isbcgc-216220'
# 	dataset = 'GECCO_CRC_Susceptibility'

	try:
		opts, args = getopt.getopt(argv, "hw:p:d:t:", ["help", "workdir=", "project=", "dataset=", "table="])
	except getopt.GetoptError:
		usage()
		sys.exit(2)
	for opt, arg in opts:
	    if opt in ("-h", "--help"):
	        usage()
	        sys.exit()
	    elif opt in ("-w", "--workdir"):
	        workdir = arg
	    elif opt in ("-p", "--project"):
	        project = arg
	    elif opt in ("-d", "--dataset"):
	        dataset = arg
	    elif opt in ("-t", "--table"):
	        table = arg
	        
	dataset_id = project + "." + dataset
	
	file_list = glob.glob(workdir + '*' + dataset + '_' + table + '*.txt')
	for filepath in file_list:
		print ('\n'+filepath)
		# break out parts of the filename
		p = re.compile('.*\.'+dataset+'_(.*)\.(.*).txt')
		m = p.match(filepath)
		tab = m.group(1)
		consent = m.group(2)
		print (tab, consent)
		
		# find the data dictionary for this file
		dict_list = glob.glob(workdir + '*' + dataset + '*' + tab + '.data_dict.xml')
		schemafilename = dict_list[0]
		print (schemafilename)
		
		# set up table name to use in BigQuery	
		tableName = tab+'_'+consent
		# fix for characters that can't be used in a table name
		tableName = tableName.replace("-", "_")
		table_id = dataset_id + "." + tableName
		
		# read the data dictionary
		dd = dataDict(schemafilename)
		# create the bigquery table
		client, table = createBQTable(table_id, dd, filepath)
		# load the data from tab delimited file
		loadBQTable(client, table, filepath)
    
if __name__ == "__main__":
    main(sys.argv[1:])
    


	
	

	
	









