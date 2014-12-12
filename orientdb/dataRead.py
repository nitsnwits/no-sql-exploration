import pyorient
import time
import threading
import math

client = pyorient.OrientDB("localhost", 2424)  # host, port

# open a connection (username and password)
client.connect("root", "root")
print "connection established!!"
start = 0
end = 0

someValue = client.db_exists( "library", pyorient.STORAGE_TYPE_MEMORY )

if someValue == True:
	
	print "Query 1: \t"
	print "What are all the Titles written by one author? \n"
	#open database connection
	client.db_open("library", "root", "root")
	start = time.time()
	f = open('p1', 'w')
	print "Start time:\t" + str(start)
	count = client.command("select from user where name = 'William Shakespeare' limit 1500")
	for line in count:
		f.write(line.name+"\n")
		#print line.name	
	end = time.time()
	print "End time: \t" + str(end)
	print "Query 1 - Time consumed: \t" + str(end - start) + "\tsecs\n"

#-----------------------------------------------------------------------------------------------------------------------

	print "Query 2: \t"
	print "Find the text of the book with a title name?\n"
	start = time.time()
	f = open('p2', 'w')
	print "Start time:\t" + str(start)
	count = client.command("select whole_book from book where title = 'Divina Commedia di Dante' limit 1500")
	for line in count:
		f.write(str(line.version))
		f.write("\n")
		#print line.version	
	end = time.time()
	print "End time: \t" + str(end)
	print "Query 2 - Time consumed: \t" + str(end - start) + "\tsecs\n"

#-----------------------------------------------------------------------------------------------------------------------

	print "Query 3: \t"
	print "Find the text of the book with a title name?\n"
	start = time.time()
	f = open('p3', 'w')
	print "Start time:\t" + str(start)
	count = client.command("select title from book where title like '%Divina%' limit 1500")
	for line in count:
		f.write(str(line.title))
		f.write("\n")
		#print line.version	
	end = time.time()
	print "End time: \t" + str(end)
	print "Query 3 - Time consumed: \t" + str(end - start) + "\tsecs\n"

#-----------------------------------------------------------------------------------------------------------------------

	print "Query 4: \t"
	print "New Releases, the titles that were released within the current year?\n"
	start = time.time()
	f = open('p4', 'w')
	print "Start time:\t" + str(start)
	count = client.command("select title from book where release_date like '%1997%' limit 1500")
	for line in count:
		f.write(line.title)
		f.write("\n")
		#print line.version	
	end = time.time()
	print "End time: \t" + str(end)
	print "Query 4 - Time consumed: \t" + str(end - start) + "\tsecs\n"

#-----------------------------------------------------------------------------------------------------------------------

	print "Query 5: \t"
	print "The books one user has read 'My books'?\n"
	start = time.time()
	f = open('p5', 'w')
	print "Start time:\t" + str(start)
	count = client.command("select title from (traverse out_completed from (select from user where name = 'nameOfPerson')) where @class = 'book' limit 1500")
	for line in count:
		f.write(str(line.title))
		f.write("\n")
		#print line.version	
	end = time.time()
	print "End time: \t" + str(end)
	print "Query 5 - Time consumed: \t" + str(end - start) + "\tsecs\n"

#-----------------------------------------------------------------------------------------------------------------------

	print "Query 6: \t"
	print "What type of book clubs are present in the system?\n"
	start = time.time()
	f = open('p6', 'w')
	print "Start time:\t" + str(start)
	count = client.command("select category from book limit 1500")
	for line in count:
		f.write(str(line.category))
		f.write("\n")
		#print line.version	
	end = time.time()
	print "End time: \t" + str(end)
	print "Query 6 - Time consumed: \t" + str(end - start) + "\tsecs\n"

#-----------------------------------------------------------------------------------------------------------------------
	print "Query 7: \t"
	print "What type of book clubs are present in the system?\n"
	start = time.time()
	f = open('p7', 'w')
	print "Start time:\t" + str(start)
	count = client.command("select category from bookClubs")
	for line in count:
		f.write(str(line))
		f.write("\n")
		#print line.version	
	end = time.time()
	print "End time: \t" + str(end)
	print "Query 7 - Time consumed: \t" + str(end - start) + "\tsecs\n"

#-----------------------------------------------------------------------------------------------------------------------
	print "Query 8: \t"
	print "Which users belong to which bookclub?\n"
	start = time.time()
	f = open('p8', 'w')
	print "Start time:\t" + str(start)
	count = client.command("select name from (traverse in_interested_in from (select from bookClubs)) where @class = 'user'")
	for line in count:
		f.write(str(line))
		f.write("\n")
		#print line.version	
	end = time.time()
	print "End time: \t" + str(end)
	print "Query 8 - Time consumed: \t" + str(end - start) + "\tsecs\n"

#-----------------------------------------------------------------------------------------------------------------------
	print "Query 9: \t"
	print "If a user is part of a bookclub 'A', he may be interested in knowing what other bookclubs are joined by the other members of bookclub 'A'?\n"
	start = time.time()
	f = open('p9', 'w')
	print "Start time:\t" + str(start)
	count = client.command("select category from (traverse out_interested_in from (select from (select name from (traverse in_interested_in from (select from bookClubs)) where @class = 'user'))) where @class = 'bookClubs' and category <> 'A'")
	for line in count:
		f.write(str(line))
		f.write("\n")
		#print line.version	
	end = time.time()
	print "End time: \t" + str(end)
	print "Query 9 - Time consumed: \t" + str(end - start) + "\tsecs\n"

#-----------------------------------------------------------------------------------------------------------------------
	print "Query 10: \t"
	print "Comment and review a book. Even if a user is an author or a reader?\n"
	start = time.time()
	f = open('p10', 'w')
	print "Start time:\t" + str(start)
	count = client.command("select category from (traverse out_interested_in from (select from (select name from (traverse in_interested_in from (select from bookClubs)) where @class = 'user'))) where @class = 'bookClubs' and category <> 'A'")
	for line in count:
		f.write(str(line))
		f.write("\n")
		#print line.version	
	end = time.time()
	print "End time: \t" + str(end)
	print "Query 10 - Time consumed: \t" + str(end - start) + "\tsecs\n"

#-----------------------------------------------------------------------------------------------------------------------
	print "Query 11: \t"
	print "Read all the comments and reviews put by users for a book?\n"
	start = time.time()
	f = open('p11', 'w')
	print "Start time:\t" + str(start)
	count = client.command("select comment from (traverse in_belongs_to from (select from book where title = 'target_book')) where @class='comments'")
	for line in count:
		f.write(str(line))
		f.write("\n")
		#print line.version	
	end = time.time()
	print "End time: \t" + str(end)
	print "Query 11 - Time consumed: \t" + str(end - start) + "\tsecs\n"

#-----------------------------------------------------------------------------------------------------------------------
	print "Query 12: \t"
	print "Read all the comments by a user?\n"
	start = time.time()
	f = open('p12', 'w')
	print "Start time:\t" + str(start)
	count = client.command("select comment from (traverse out_written from (select from user where name = 'target_name')) where @class='comments'")
	for line in count:
		f.write(str(line))
		f.write("\n")
		#print line.version	
	end = time.time()
	print "End time: \t" + str(end)
	print "Query 12 - Time consumed: \t" + str(end - start) + "\tsecs\n"

#-----------------------------------------------------------------------------------------------------------------------
	print "Query 13: \t"
	print "Categories of books. Browse books by category\n"
	start = time.time()
	f = open('p13', 'w')
	print "Start time:\t" + str(start)
	count = client.command("select title from (traverse out_belongs_to from (select from bookClubs where category = 'target_category')) where @class='book'")
	for line in count:
		f.write(str(line))
		f.write("\n")
		#print line.version	
	end = time.time()
	print "End time: \t" + str(end)
	print "Query 13 - Time consumed: \t" + str(end - start) + "\tsecs\n"

else:
	print "No database found"