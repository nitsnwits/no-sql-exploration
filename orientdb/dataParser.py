
import zipfile

zp = zipfile.ZipFile('books.zip')

bookCount = 0
titleCount = 0
authorCount = 0
releaseDateCount = 0
languageCount = 0
bookName = ""
for book in zp.infolist():
	#print book.filename
	openBook = zp.open(book)
	data = openBook.readlines()

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
			print language
			languageCount = languageCount+1


	#if titleFound == False:
	#	bookName = bookName+book.filename+" "
	#if rDateFound == False:
	#	bookName = bookName+book.filename+" "
	bookCount = bookCount+1

	# reading whole book
	wholeBook = openBook.read()



print "total no of files in zip " +str(bookCount)
print "total no of titles of books "+str(titleCount)
print "total no of authors of books "+str(authorCount)
print "total no of release dates found "+str(releaseDateCount)
print "total no of languages found "+str(languageCount)
print "these are the irregular books "+bookName