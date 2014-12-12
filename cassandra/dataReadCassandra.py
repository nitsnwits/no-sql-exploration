from cassandra.cluster import Cluster
import logging
import sys
import os.path
import time

log = logging.getLogger()
log.setLevel('INFO')
log_record_count = 0
path = '/home/abhibel/Downloads/ranganth/books/'

class SimpleClient:
	session = None

	def connect(self, nodes):
		cluster = Cluster(nodes)
		metadata = cluster.metadata
		self.session = cluster.connect('library')
		log.info('Connected to cluster: ' + metadata.cluster_name)
		for host in metadata.all_hosts():
			log.info('Datacenter: %s; Host: %s; Rack: %s', host.datacenter, host.address, host.rack)


	def read_data(self):

		print "Query 1: \t"
		print "What are all the Titles written by one author? \n"
		#open database connection
		start = time.time()
		f = open('c1', 'w')
		print "Start time:\t" + str(start)
		count = self.session.execute("select titles from titlesbyauthor where author='William Shakespeare';")
		for line in count:
			f.write(str(titles)+"\n")
			#print line.titles	
		end = time.time()
		print "End time: \t" + str(end)
		print "Query 1 - Time consumed: \t" + str(end - start) + "\tsecs\n"

#-----------------------------------------------------------------------------------------------------------------------

		print "Query 2: \t"
		print "Find the text of the book with a title name? \n"
		#open database connection
		start = time.time()
		f = open('c2', 'w')
		print "Start time:\t" + str(start)
		count = self.session.execute("select content from book_content where title = 'Washington Square';")
		for line in count:
			f.write(str(line)+"\n")
			#print line.titles	
		end = time.time()
		print "End time: \t" + str(end)
		print "Query 2 - Time consumed: \t" + str(end - start) + "\tsecs\n"

#-----------------------------------------------------------------------------------------------------------------------

		print "Query 4: \t"
		print "New Releases, the titles that were released within the current year? \n"
		#open database connection
		start = time.time()
		f = open('c4', 'w')
		print "Start time:\t" + str(start)
		count = self.session.execute("select titles from titlesbyyear where year=2014;")
		for line in count:
			f.write(str(line)+"\n")
			#print line.titles	
		end = time.time()
		print "End time: \t" + str(end)
		print "Query 4 - Time consumed: \t" + str(end - start) + "\tsecs\n"

#-----------------------------------------------------------------------------------------------------------------------

		print "Query 5: \t"
		print "The books one user has read? 'My books'? \n"
		#open database connection
		start = time.time()
		f = open('c5', 'w')
		print "Start time:\t" + str(start)
		count = self.session.execute("select booksread from users where username='userName';")
		for line in count:
			f.write(str(line)+"\n")
			#print line.titles	
		end = time.time()
		print "End time: \t" + str(end)
		print "Query 5 - Time consumed: \t" + str(end - start) + "\tsecs\n"

#-----------------------------------------------------------------------------------------------------------------------

		print "Query 6: \t"
		print "The books one user wishes to read? 'Wants to read' list! \n"
		#open database connection
		start = time.time()
		f = open('c6', 'w')
		print "Start time:\t" + str(start)
		count = self.session.execute("select wantstoread from users where username='userName';")
		for line in count:
			f.write(str(line)+"\n")
			#print line.titles	
		end = time.time()
		print "End time: \t" + str(end)
		print "Query 6 - Time consumed: \t" + str(end - start) + "\tsecs\n"

#-----------------------------------------------------------------------------------------------------------------------

		print "Query 7: \t"
		print "What type of book clubs are present in the system? \n"
		#open database connection
		start = time.time()
		f = open('c7', 'w')
		print "Start time:\t" + str(start)
		count = self.session.execute("select bookclub from book_details;")
		for line in count:
			f.write(str(line)+"\n")
			#print line.titles	
		end = time.time()
		print "End time: \t" + str(end)
		print "Query 7 - Time consumed: \t" + str(end - start) + "\tsecs\n"

#-----------------------------------------------------------------------------------------------------------------------

		print "Query 8: \t"
		print "Which users belong to which bookclub? \n"
		#open database connection
		start = time.time()
		f = open('c8', 'w')
		print "Start time:\t" + str(start)
		count = self.session.execute("select username, bookclubs from users limit 10;")
		for line in count:
			f.write(str(line.username)+str(line.bookclubs)+"\n")
			#print line.titles	
		end = time.time()
		print "End time: \t" + str(end)
		print "Query 8 - Time consumed: \t" + str(end - start) + "\tsecs\n"

#-----------------------------------------------------------------------------------------------------------------------

		print "Query 9: \t"
		print "If a user is part of a bookclub 'A', he may be interested in knowing what other bookclubs are joined by the other members of bookclub 'A'? \n"
		#open database connection
		start = time.time()
		f = open('c9', 'w')
		print "Start time:\t" + str(start)
		count = self.session.execute("select bookclubs from users limit 10;")
		for line in count:
			f.write(str(line.bookclubs)+"\n")
			#print line.titles	
		end = time.time()
		print "End time: \t" + str(end)
		print "Query 9 - Time consumed: \t" + str(end - start) + "\tsecs\n"

#-----------------------------------------------------------------------------------------------------------------------

		print "Query 10: \t"
		print "Comment and review a book. Even if a user is an author or a reader? \n"
		#open database connection
		start = time.time()
		f = open('c10', 'w')
		print "Start time:\t" + str(start)
		count = self.session.execute("Select comments from comments where username = 'userName';")
		for line in count:
			f.write(str(line.comments)+"\n")
			print line.titles	
		end = time.time()
		print "End time: \t" + str(end)
		print "Query 10 - Time consumed: \t" + str(end - start) + "\tsecs\n"

#-----------------------------------------------------------------------------------------------------------------------

		print "Query 11: \t"
		print "-Read all the comments and reviews put by users for a book? \n"
		#open database connection
		start = time.time()
		f = open('c11', 'w')
		print "Start time:\t" + str(start)
		count = self.session.execute("select comments from comments where title = 'titleName' and username = 'userName';")
		for line in count:
			f.write(str(line.bookclubs)+"\n")
			#print line.titles	
		end = time.time()
		print "End time: \t" + str(end)
		print "Query 11 - Time consumed: \t" + str(end - start) + "\tsecs\n"

#-----------------------------------------------------------------------------------------------------------------------

		print "Query 12: \t"
		print "Read all the comments by a user? \n"
		#open database connection
		start = time.time()
		f = open('c12', 'w')
		print "Start time:\t" + str(start)
		count = self.session.execute("select title, comments from comments where username = 'userName';")
		for line in count:
			f.write(str(line.bookclubs)+"\n")
			#print line.titles	
		end = time.time()
		print "End time: \t" + str(end)
		print "Query 12 - Time consumed: \t" + str(end - start) + "\tsecs\n"

#-----------------------------------------------------------------------------------------------------------------------

		print "Query 13: \t"
		print "Categories of books. Browse books by category? \n"
		#open database connection
		start = time.time()
		f = open('c13', 'w')
		print "Start time:\t" + str(start)
		count = self.session.execute("select title from book_details where bookclub = 'null';")
		for line in count:
			f.write(str(line.bookclubs)+"\n")
			#print line.titles	
		end = time.time()
		print "End time: \t" + str(end)
		print "Query 13 - Time consumed: \t" + str(end - start) + "\tsecs\n"

#-----------------------------------------------------------------------------------------------------------------------

	def close(self):
		self.session.cluster.shutdown()
		self.session.shutdown()
		log.info('Connection closed.')


def main():
	logging.basicConfig()
	client = SimpleClient()
	client.connect(['localhost'])
	#client.load_schema()
	client.read_data()
	client.close()


if __name__=="__main__":
	main()	
