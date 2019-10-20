# sddp-bigquery
Working with dbGap SDDP data in BigQuery.

Note that all data downloaded from dbGaP must be handled in accordance with your Data Use Agreement. You may not make the data available to anyone else other than as specified by the dbGaP Research Project which you have submitted and had approved. In order to use dbGaP data in the cloud, including BigQuery, your Research Project must specify and be approved for data use in the cloud. 


## create_dbgap_table.py
Create BigQuery tables for tab-delimited data files from dbGaP.
Load the created tables with data from the files.

### Usage
create_dbgap_table.py -w workingDirectory -p GCPProject -d BiqQueryDataset -t Table

-w the working directory containing the *.txt and *.data_dict.xml files from dbGaP

-p name of the Google cloud project

-d name of the BigQuery dataset. 

-t name of the table to create and load. If this is not specified all *.txt files from the dataset will be loaded.

### Examples

1.	Create tables and load all the files for the gecco project (phs-001554)

create_dbgap_table.py -w ~/ncbi/dbGaP-14565/files/gecco/ -p nci-gecco -d GECCO_CRC_Susceptibility

2.	Create and load tables for the gecco subject phenotype data only

create_dbgap_table.py -w ~/ncbi/dbGaP-14565/files/gecco/ -p nci-gecco -d GECCO_CRC_Susceptibility -t Subject_Phenotypes

Separate *.txt files exist for each consent group, separate google cloud tables (generally subject phentotype and sample attributes) will be created for each group you have access to.

### Setup
The working directory should be prepopulated with the *.txt and *.data_dict.xml files downloaded by prefetch, decrypted and unzipped.

Authentication for BigQuery should be set as described here.
https://cloud.google.com/bigquery/docs/reference/libraries

To keep the correspondence between the files and the datasets and tables clear, the name of the dataset is the same as the component of the .txt file names that represents the project. 

To take an example file from GECCO
phs001554.v1.pht007609.v1.p1.c1.GECCO_CRC_Susceptibility_Subject_Phenotypes.GRU.txt


GECCO_CRC_Susceptibility would be the dataset name. The dataset should be created manually in BigQuery before running the program.

The table name created from the file above would be Subject_Phenotypes_GRU
Other Subject_Phenotypes tables would also get created for each consent group you have access to and for which you have downloaded the file.


