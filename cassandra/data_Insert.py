from cassandra.cluster import Cluster
import logging
import time

log = logging.getLogger()
log.setLevel('INFO')

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
		line_count = 0

		fp = open('books/11.txt', 'r')
		for line in fp:
			
			if (line.find('Title:')!= -1 or line.find('Tile:')!= -1):
				title = line.split(':')[1].strip()

			if line.find('Author:')!=-1:
				author = line.split(':')[1].strip()

		
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
		fp.close()

		fp = open('books/11.txt','r')
		# reading whole book
		wholeBook = fp.read()
		fp.close()


		stmnt_bk_det = self.session.prepare("""INSERT INTO library.book_details (title, author , language , r_date ) VALUES ( ?, ?, ?, ?);""")

		stmnt_bk_cnt = self.session.prepare("""INSERT INTO library.book_content (title, content ) VALUES (?, ?);""")

		self.session.execute(stmnt_bk_det.bind((
			title, author, language, r_date
			)))

		self.session.execute(stmnt_bk_cnt.bind((
			title, wholeBook
			)))

		log.info('Record added')


	def close(self):
		self.session.cluster.shutdown()
		self.session.shutdown()
		log.info('Connection closed.')

def main():
	logging.basicConfig()
	client = SimpleClient()
	client.connect(['localhost'])
	client.load_data()
	client.close()


if __name__=="__main__":
	main()	
