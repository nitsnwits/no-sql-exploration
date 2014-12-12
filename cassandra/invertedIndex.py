#!/usr/bin/env python
# Build an inverted index on cassandra
# Provide search functionalities in titles
#

from cassandra.cluster import Cluster
import logging
import sys
import os.path
import time
import random 
from collections import defaultdict
from cassandra.query import named_tuple_factory

log = logging.getLogger(name='InvertedIndex')
log.isEnabledFor(logging.INFO)
path = '/Users/neerajsharma/Downloads/books/'
iteration = 0

class InvertedIndex(object):
	def __init__(self, nodes):
		""" 
		nodes is an array of ['localhost']
		path is the path for books
		"""
		cluster = Cluster(nodes)
		metadata = cluster.metadata
		self.session = cluster.connect('library')
		self.invertedIndex = defaultdict(list)
		print "Created connection to Cassandra."

	def build(self, stopWordsList):
		"""
		Build an inverted index based on titles receveid from database
		stopWordsList is a list of stop words
		"""
		stmtSelect = self.session.prepare("""select title from library.book_details""");
		self.session.row_factory = named_tuple_factory
		resultSet = self.session.execute(stmtSelect)
		for row in resultSet:
			#print "Processing row: " + row.title
			for word in row.title.lower().split(' '):
				if not word in stopWordsList:
					self.invertedIndex[word].append(row.title)
		print "Built InvertedIndex"

	def lookupWord(self, keyword):
		"""
		Support search functionality using the index built
		"""
		resultSet = self.invertedIndex[keyword.lower()]
		return resultSet

	def lookupPhrase(self, phrase):
		"""
		Should Support phrase search which unions the result set of all the words each
		"""
		returnList = []
		wordlist = phrase.split(' ')
		if len(wordlist) <= 1:
			return self.lookupWord(wordlist[0])
		else:
			for word in wordlist:
				returnList += self.lookupWord(word)
		# give weight to more occurrences and result at top
		weightedReturnDict = {}
		for title in returnList:
			if title in weightedReturnDict.keys():
				weightedReturnDict[title] += 1
			else:
				weightedReturnDict[title] = 1
		return weightedReturnDict

	def printPhraseLookupResults(self, weightedReturnDict):
		"""
		Print results with weight in consideration
		"""
		# sort the list from values and return the most weighted result first
		for title in sorted(weightedReturnDict, key=weightedReturnDict.get, reverse=True):
			print "Title: ->\t" + title + "\t\tMatches: -> " + str(weightedReturnDict[title])
		return


def readStopWords(path):
	"""
	Reads stopwords txt and returns a stop words list
	"""
	stopWordsList = []
	fileHandler = open(path, 'r')
	lines = fileHandler.readlines()
	for stopword in lines:
		# I miss Perl's chomp here, removing new lines
		stopWordsList.append(stopword[:-1])
	return stopWordsList


def main():
	path = os.path.abspath('./stopWords.txt')
	stopwords = readStopWords(path)
	ii = InvertedIndex(['localhost'])
	ii.build(stopwords)
	print "Valley: ", ii.lookupPhrase('Valley')
	print "of: ", ii.lookupPhrase('of')
	print "the: ", ii.lookupPhrase('the')
	print "Moon: ", ii.lookupPhrase('Moon')
	print ii.printPhraseLookupResults(ii.lookupPhrase('Valley of the Moon'))

if __name__ == '__main__':
	main()