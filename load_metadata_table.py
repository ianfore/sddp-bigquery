#  IMPORTS
import xml.etree.ElementTree as ET
from google.cloud import bigquery
import glob
import re
import pandas as pd
import sys, getopt

import data_dict as dd
import data_dict_bq as ddbq


#%% 

def usage():
	print (sys.argv[0] +' -w workdir -p project -d dataset -t table')
	
def main(argv):
	workdir = ''
	project = ''
	dataset = 'metadata'
	table = '*'
	single_table = True
#	workdir = '/Users/forei/ncbi/dbGaP-11218/files/'
	project = 'isbcgc-216220'
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
	
	file_list = glob.glob(workdir + '*.txt')
	for filepath in file_list:
		print ('\n'+filepath)
		# break out parts of the filename
		p = re.compile('(.*).txt')
		m = p.match(filepath)
		tableName = m.group(1)
		
		table_id = dataset_id + "." + tableName
		
		# read the data dictionary
		dict = dd.dataDict(schemafilename)
		# create the bigquery table
		bq = ddbq.dataDictBQ(dict)
		(client, table) = bq.createBQTable(table_id, filepath)
		# load the data from tab delimited file
		bq.loadBQTable(client, table, filepath)
    
if __name__ == "__main__":
    main(sys.argv[1:])
    


	
	

	
	









