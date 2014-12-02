from cassandra.cluster import Cluster
import logging
import sys
import os.path
import time
import random 

log = logging.getLogger()
log.setLevel('INFO')
log_record_count = 0
path = '/Users/neerajsharma/Downloads/books/'
book_clubs = [ 'Fiction', 'Non-Fiction', 'Biography', 'Autobiography', 'Fairy Tale', 'History']
iteration = 0

class SimpleClient:
	session = None

	def connect(self, nodes):
		cluster = Cluster(nodes)
		metadata = cluster.metadata
		self.session = cluster.connect()
		log.info('Connected to cluster: ' + metadata.cluster_name)
		for host in metadata.all_hosts():
			log.info('Datacenter: %s; Host: %s; Rack: %s', host.datacenter, host.address, host.rack)

	def load_data(self):
		
		for root, dirs, filenames in os.walk (path):
			for file_name in filenames:
				
				# read iteration count
				global iteration
				iteration = iteration + 1
				file_name = path + file_name
				fp = open(file_name, 'r')
				for line in fp:
					
					if (line.find('Title:')!= -1 or line.find('Tile:')!= -1):
						title = line.split(':')[1].strip()

					if line.find('Author:')!=-1:
						author = line.split(':')[1].strip()
						if author != "":
							author_fn = author.split(" ")
							print author_fn
							f_name = author_fn[0]
							if len(author_fn) > 1:
								l_name = author_fn[1]
							else:
								l_name = ""
						else:
							author = "Uknown"
							f_name = ""
							l_name = ""

					if line.lower().find('[etext')!= -1:

						r_date = line.lower().split('[etext')[0]
						if ':' in r_date:
							r_date = r_date.split(':')[1]
						r_date = r_date.strip()
						
					elif line.lower().find('[ebook')!= -1:

						r_date = line.lower().split('[ebook')[0]
						if ':' in r_date:
							r_date = r_date.split(':')[1]
						r_date = r_date.strip()
						
					if line.find('Language:')!=-1:
						language = line.split(':')[1].strip()
					else:
						language = "English"

				fp.close()

				fp = open(file_name,'r')
				# reading whole book
				wholeBook = fp.read()
				fp.close()

				stmnt_bk_det = self.session.prepare("""INSERT INTO library.book_details (title, author , language , releasedate ) VALUES ( ?, ?, ?, ?);""")

				stmnt_bk_cnt = self.session.prepare("""INSERT INTO library.book_content (title, content ) VALUES (?, ?);""")
				
				stmnt_users = self.session.prepare("""INSERT INTO library.users (username, firstname, lastname, booksread, wantstoread, bookclubs) VALUES (?, ?, ?, 					?, ?, ?) """)

				self.session.execute(stmnt_bk_det.bind((
					title, author, language, r_date
					)))

				self.session.execute(stmnt_bk_cnt.bind((
					title, wholeBook
					)))
				titleList = [title]
				bookclubsList = [random.choice(book_clubs)]
				self.session.execute(stmnt_users.bind((
					author, f_name, l_name, titleList, titleList, bookclubsList
					)))

				if (iteration%100 == 0):
					log.info("Updated number of record: " + str(iteration))

	def load_schema(self):
		self.session.execute("""
			CREATE KEYSPACE library WITH replication = 
			{'class': 'SimpleStrategy', 'replication_factor': 1};
			""")

		self.session.execute("""
			CREATE COLUMNFAMILY library.book_details 
			(title varchar PRIMARY KEY, author varchar, language varchar, releasedate varchar, bookclub varchar );""")

		self.session.execute("""
			CREATE COLUMNFAMILY library.book_content (title varchar PRIMARY KEY , content blob );
			""")

		self.session.execute("""
			CREATE COLUMNFAMILY library.titlesByAuthor ( author varchar PRIMARY KEY, titles set<varchar >);			
		""")

		self.session.execute("""
			CREATE COLUMNFAMILY library.titlesByYear ( year int PRIMARY KEY , titles set < varchar > );
		""")

		self.session.execute("""
			CREATE COLUMNFAMILY library.users ( username varchar PRIMARY KEY , firstname varchar , 
				lastname varchar , booksread list < varchar >, wantstoread list <varchar >, bookclubs list < varchar >);			
			""")

	def drop_schema(self):
		self.session.execute("""
			DROP KEYSPACE IF EXISTS library
			""")				

	def close(self):
		self.session.cluster.shutdown()
		self.session.shutdown()
		log.info('Connection closed.')

def main():
	logging.basicConfig()
	client = SimpleClient()
	client.connect(['localhost'])
	client.drop_schema()
	client.load_schema()
	client.load_data()
	#client.close()


if __name__=="__main__":
	main()	
