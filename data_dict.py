#  IMPORTS
import xml.etree.ElementTree as ET
#import glob
from collections import defaultdict
from datetime import datetime
#from elasticsearch import Elasticsearch


def etree_to_dict(t):
    d = {t.tag: {} if t.attrib else None}
    children = list(t)
    if children:
        dd = defaultdict(list)
        for dc in map(etree_to_dict, children):
            for k, v in dc.items():
                dd[k].append(v)
        d = {t.tag: {k: v[0] if len(v) == 1 else v
                     for k, v in dd.items()}}
    if t.attrib:
        d[t.tag].update((k, v)
                        for k, v in t.attrib.items())
    if t.text:
        text = t.text.strip()
        if children or t.attrib:
            if text:
              d[t.tag]['text'] = text
        else:
            d[t.tag] = text
    return d


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
		
	def list_datatypes(self):
		varCount = 0
		for var in self.theDict.findall('variable'):
			varCount +=1 
			name = var.find('name').text
			desc = var.find('description').text
			typeNode = var.find('type')
			if typeNode is None:
				type = 'string'
			else :
				type = typeNode.text			
			print (name)
		print ('Variable count:', varCount)

	def getdicts(self):
		for var in self.theDict.findall('variable'):
			self.varid +=1 
			vdict = etree_to_dict(var)
			#remove the id
			vitems = vdict['variable']
			del vitems['id']
			print (vdict)
#			self.es.index(index="var-index", doc_type="dbgap_variable", id=self.varid, body=vitems)



