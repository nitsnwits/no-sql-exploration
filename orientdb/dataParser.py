
import zipfile
import pyorient
import sys
from random import randint

reload(sys)
sys.setdefaultencoding('utf-8')


client = pyorient.OrientDB("localhost", 2424)

client.connect("root", "root")
print "connection established!!"

dbPresent = client.db_exists( "library", pyorient.STORAGE_TYPE_PLOCAL )
if dbPresent == True:
	print "deleting and creating again"
	client.db_drop("library")

client.db_create("library", pyorient.DB_TYPE_GRAPH, pyorient.STORAGE_TYPE_PLOCAL)
print "created db"
client.db_open("library", "root", "root")

client.command("create class user extends V")
client.command("create property user.name string")
client.command("create class book extends V")
client.command("create property book.title string")
client.command("create property book.release_date string")
client.command("create property book.language string")
#client.command("create property book.whole_book string")
client.command("create class comments extends V")
client.command("create property comments.comment string")
client.command("create class bookClubs extends V")
client.command("create property bookClubs.category string")
client.command("create class written extends E")
client.command("create class read extends E")
client.command("create class completed extends E")
client.command("create class belongs_to extends E")
client.command("create class interested_in extends E")
client.command("insert into bookClubs (category) values ('biography'),('philosophy'),('mystery'),('fantasy'),('history')")


bookCategories = { 1:"biography",
                   2:"philosophy",
                   3:"mystery",
                   4:"fantasy",
                   5:"history"}


print "successfully created vertex and edges"

zp = zipfile.ZipFile('/home/sunny/PycharmProjects/NoSQL226/books.zip')

bookCount = 0
titleCount = 0
authorCount = 0
releaseDateCount = 0
languageCount = 0
bookName = ""
for book in zp.infolist():
	print book.filename
	openBook = zp.open(book)
	data = openBook.readlines()

	book_cat = bookCategories[randint(1,5)]
	language = "English"
	#titleCountInBook = 0
	lineCount = 0
	titleFound = False
	rDateFound = False
	dateCountInBook = 0
	for line in data:
		lineCount = lineCount+1


		if (line.find('Title:')!= -1 or line.find('Tile:')!= -1) and lineCount < 100:
			title = line.split(':')[1].strip()
			#print "title ->"+title
			titleCount = titleCount + 1
			#titleCountInBook = titleCountInBook+1
			#print line
			#titleFound = True
			#if titleCountInBook > 1:
			#	bookName = bookName + book.filename

		if line.find('Author:')!=-1 and lineCount < 100:

			author = line.split(':')[1].strip()
			#print "author ->"+author
			authorCount = authorCount + 1

		'''if line.find('Release Date:')!= -1 and lineCount < 100:

			releaseDateCount = releaseDateCount + 1
			rDateFound = True
			dateCountInBook = dateCountInBook + 1
			if dateCountInBook > 1:
				bookName = bookName+book.filename+" "
		'''
		if line.lower().find('[etext')!= -1 and lineCount < 100 and rDateFound == False:

			rdate = line.lower().split('[etext')[0]
			if ':' in rdate:
				rdate = rdate.split(':')[1]
			rdate = rdate.strip()
			#print rdate
			releaseDateCount = releaseDateCount + 1
			rDateFound = True
			dateCountInBook = dateCountInBook + 1
			if dateCountInBook > 1:
				bookName = bookName+book.filename+" "

		elif line.lower().find('[ebook')!= -1 and lineCount < 100 and rDateFound == False:

			rdate = line.lower().split('[ebook')[0]
			if ':' in rdate:
				rdate = rdate.split(':')[1]
			rdate = rdate.strip()
			#print rdate
			releaseDateCount = releaseDateCount + 1
			rDateFound = True
			dateCountInBook = dateCountInBook + 1
			if dateCountInBook > 1:
				bookName = bookName+book.filename+" "

		if line.find('Language:')!=-1 and lineCount < 100:

			#titleFound = True
			language = line.split(':')[1].strip()
			#print language
			languageCount = languageCount+1


	openBook.close()
	#openBook2 = zp.open(book)

	#wholebook = openBook2.read()

	#if titleFound == False:
	#	bookName = bookName+book.filename+" "
	#if rDateFound == False:
	#	bookName = bookName+book.filename+" "
	book_cat = bookCategories[randint(1,5)]
	bookCount = bookCount+1
	openBook.close()
	author = author.replace("'", "\\'")
	author = author.replace("(", "\\(")
	author = author.replace(")", "\\)")
	author = author.replace('"', '\\"')
	print "select from user where name ='" + author + "'"
	resultset = client.command('select from user where name = "' + author + '"')
	autCount = 0
	if len(resultset) == 0:
		resultset = client.command("insert into user (name) values ('"+author+"')")
	else:
		for threelist in resultset:
			print "in the loop"
			if threelist.name != author:
				autCount = autCount + 1

	if autCount == len(resultset):
		resultset = client.command("insert into user (name) values ('"+author+"')")

	#resultset = client.command("insert into user (name) values ('"+author+"')")
	# replace single quote with double quote to help the parser parse the query for embedded single quotes in the text
	title = title.replace("'", "\\'")
	rdate = rdate.replace("'", "\\'")
	language = language.replace("'", "\\'")
	#wholebook = wholebook.replace("'","\\'")
	#print " query: " + "insert into book (title,release_date,language) values ('"+title+"','"+rdate+"','"+language+"')"
	#resultset2 = client.command("insert into book (title,release_date,language,whole_book) values ('"+title+"','"+rdate+"','"+language+"','"+wholebook+"')")
	resultset2 = client.command("insert into book (title,release_date,language) values ('"+title+"','"+rdate+"','"+language+"')")
	#client.command("create edge written from (select from user where name = '"+author+"') to (select from book where title = '"+title+"')")
	resultset3 = client.command("select from bookClubs where category = '"+book_cat+"'")
	for onelist in resultset:
		print onelist.rid

	for twolist in resultset2:
		print twolist.rid

	for fourlist in resultset3:
		print fourlist.rid
	client.command("create edge written from "+onelist.rid+" to "+twolist.rid)
	client.command("create edge belongs_to from "+twolist.rid+" to "+fourlist.rid)

	print "inserted records in user, book and bookClubs... created edges between them "+str(bookCount)
	# reading whole book
	#wholeBook = openBook.read()



print "total no of files in zip " +str(bookCount)
print "total no of titles of books "+str(titleCount)
print "total no of authors of books "+str(authorCount)
print "total no of release dates found "+str(releaseDateCount)
#print "total no of languages found "+str(languageCount)
print "these are the irregular books "+bookName

#sample query:
#insert into book (title,release_date,language) values ('Divine Comedy Longfellows Translation Hell','april 12 2009','English')