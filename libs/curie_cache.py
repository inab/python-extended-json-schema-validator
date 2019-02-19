#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import sqlite3
import urllib.request
import xml.dom.minidom
from xml.dom.minidom import Node
import dateparser
import collections

# This code is partially inspired in triplelite class from
# https://pypi.org/project/Owlready2/

Curie = collections.namedtuple('Curie',['id','namespace','name','pattern'])

class CurieCache(object):
	CURIE_MIRIAM_LINK='https://www.ebi.ac.uk/miriam/main/export/xml/'
	MIRIAM_NS='http://www.biomodels.net/MIRIAM/'
	
	def __init__(self,filename='curie_cache.sqlite'):
		existsCache = os.path.exists(filename) and (os.path.getsize(filename) > 0)
		initializeCache = not existsCache
		
		# Opening / creating the database, with normal locking
		# and date parsing
		self.conn = sqlite3.connect(filename, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES, check_same_thread = False)
		self.conn.execute("""PRAGMA locking_mode = NORMAL""")
		
		# Database structures
		with self.conn:
			cur = self.conn.cursor()
			updateDatabase = initializeCache
			if initializeCache:
				# Metadata table
				cur.execute("""
CREATE TABLE metadata (
	last_generated DATETIME NOT NULL,
	last_updated DATETIME NOT NULL
)
""")
				# Prefixes table
				cur.execute("""
CREATE TABLE namespaces (
	id VARCHAR(32) NOT NULL,
	namespace VARCHAR(64) NOT NULL,
	name VARCHAR(64) NOT NULL,
	pattern VARCHAR(4096) NOT NULL,
	PRIMARY KEY (id)
)
""")

				# Index on the namespace
				cur.execute("""
CREATE INDEX namespaces_namespace ON namespaces(namespace)
""")
			else:
				# Should we download 
				cur.execute("""
SELECT DATETIME('NOW','-7 DAYS') > last_generated
FROM metadata
""")
				res = cur.fetchone()
				if (res is None) or res[0]:
					updateDatabase = True
		
		if updateDatabase:
			# Download the registry to parse it
			with urllib.request.urlopen(CurieCache.CURIE_MIRIAM_LINK) as f:
				curie_dom = xml.dom.minidom.parse(f)
			
			root = curie_dom.documentElement
			# Does the document have the update dates?
			if root.hasAttribute('date') and root.hasAttribute('data-version'):
				last_generated = dateparser.parse(root.getAttribute('date'))
				last_updated = dateparser.parse(root.getAttribute('data-version'))
				
				cur.execute("""
SELECT TRUE
FROM metadata
WHERE last_updated >= :lu
""",{'lu': last_updated})
				if cur.fetchone() is not None:
					cur.execute("""
UPDATE metadata SET last_generated = :lg
""",{'lg': last_generated})
				else:
					# It is time to drop everything and start again
					with self.conn:
						cur.execute("""DELETE FROM namespaces""")
						cur.execute("""DELETE FROM metadata""")
						
						cur.execute("""
INSERT INTO metadata VALUES (:lg,:lu)
""",{'lg': last_generated,'lu': last_updated})
					
						for elem in root.childNodes:
							if elem.nodeType == Node.ELEMENT_NODE and elem.localName == 'datatype' and elem.namespaceURI == CurieCache.MIRIAM_NS:
								cId = elem.getAttribute('id')
								cPattern = elem.getAttribute('pattern')
								cNS = elem.getElementsByTagNameNS(CurieCache.MIRIAM_NS,'namespace')[0].firstChild.nodeValue
								cName = elem.getElementsByTagNameNS(CurieCache.MIRIAM_NS,'name')[0].firstChild.nodeValue
								cur.execute("""
INSERT INTO namespaces VALUES (:id,:ns,:name,:pat)
""",{'id': cId,'ns': cNS,'name': cName,'pat': cPattern})
		
		cur.close()
		
	# Next methods are to emulate a dictionary
	def __len__(self):
		with self.conn:
			self.conn.execute("""SELECT COUNT(*) FROM namespaces""")
			res = self.conn.fetchone()
			return None  if res is None else res[0]
	
	def keys(self):
		with self.conn:
			cur = self.conn.cursor()
			cur.execute("""SELECT id,namespace FROM namespaces""")
			val = cur.fetchone()
			while val:
				yield val[0]
				yield val[1]
				val = cur.fetchone()
			return
	
	def get(self,key,default=None):
		with self.conn:
			cur = self.conn.cursor()
			cur.execute("""
SELECT id,namespace,name,pattern
FROM namespaces
WHERE
namespace = :query
OR
id = :query
""",{'query': key})
			res = cur.fetchone()
			return Curie(*res)  if res else default
	
	def __iter__(self):
		return self.keys()
	
	def __contains__(self,key):
		if not isinstance(key,(int,float,str)):
			raise KeyError('Key type is not supported')
		
		key = str(key)
		with self.conn:
			cur = self.conn.cursor()
			cur.execute("""
SELECT id,namespace,name,pattern
FROM namespaces
WHERE
namespace = :query
OR
id = :query
""",{'query': key})
			res = cur.fetchone()
			print(res,file=sys.stderr)
			return res is not None
	
	def __getitem__(self,key):
		with self.conn:
			cur = self.conn.cursor()
			cur.execute("""
SELECT id,namespace,name,pattern
FROM namespaces
WHERE
namespace = :query
OR
id = :query
""",{'query': key})
			res = cur.fetchone()
			if not res:
				raise KeyError('Namespace {} not found'.format(key))
			
			return Curie(*res)


if __name__ == '__main__':
	cc = CurieCache("/tmp/prueba.sqlite3")
	print(cc['uniprot'])
	print('pubmed' in cc)
	print(cc['MIR:00000005'])
	print(cc['conejo'])
