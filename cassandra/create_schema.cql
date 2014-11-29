CREATE KEYSPACE library WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
use library;
CREATE COLUMNFAMILY book_details ( title varchar PRIMARY KEY , author varchar , language varchar , r_date varchar );
CREATE COLUMNFAMILY book_content (title varchar PRIMARY KEY , content blob );
