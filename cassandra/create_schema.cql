CREATE KEYSPACE library WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
use library;
CREATE COLUMNFAMILY book_details ( title varchar PRIMARY KEY , author varchar , language varchar , releasedate varchar, bookclub varchar );
CREATE COLUMNFAMILY book_content (title varchar PRIMARY KEY , content blob );
CREATE COLUMNFAMILY titlesByAuthor ( author varchar PRIMARY KEY, titles set<varchar >);
CREATE COLUMNFAMILY titlesByYear ( year int PRIMARY KEY , titles set < varchar > );
CREATE COLUMNFAMILY users ( username varchar PRIMARY KEY , firstname varchar , lastname varchar , booksread set < varchar >, wantstoread set <varchar >, bookclubs set < varchar >);
CREATE COLUMNFAMILY comments ( title varchar PRIMARY KEY, comments list < text >, username varchar ) ;

CREATE INDEX titlesByAuthor on book_details (author);
CREATE INDEX titlesByYear on book_details (releasedate);
CREATE INDEX commentsByUsername on comments (username);
CREATE INDEX usersByBookclubs on users (bookclubs);