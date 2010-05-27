import pyJaql
#Query 0. Write the books collection from data below to hadoop hdfs.
hdfswrite="$books = [{publisher: 'Scholastic',author: 'J. K. Rowling',title: 'Deathly Hallows',year: 2007},{publisher: 'Scholastic',author: 'J. K. Rowling',title: 'Chamber of Secrets',year: 1999, reviews: [{rating: 10, user: 'joe', review: 'The best ...'},{rating: 6, user: 'mary', review: 'Average ...'}]},{publisher: 'Scholastic',author: 'J. K. Rowling',title: 'Sorcerers Stone',year: 1998},{publisher: 'Scholastic',author: 'R. L. Stine', title: 'Monster Blood IV',year: 1997, reviews: [{rating: 8, user: 'rob', review: 'High on my list...'}, {rating: 2, user: 'mike', review: 'Not worth the paper ...', discussion: [{user: 'ben', text: 'This is too harsh...'},{user: 'jill', text: 'I agree ...'}]}]},{publisher: 'Grosset',author: 'Carolyn Keene',title: 'The Secret of Kane',year: 1930}];$books -> write(hdfs('books'));"

#Query 1. Return the publisher and title of each book.
query1="read(hdfs('books'))-> transform {$.publisher, $.title};"

#Query 2. Find the authors and titles of books that have received a review
query2="read(hdfs('books'))-> filter exists($.reviews)-> transform {$.author, $.title};"

#Query 3a. Project the title from each book using the short-hand projection notation.
query3a="read(hdfs('books'))[*].title;"

#Query 3a-alt. Or using equivalent the long-hand notation. 
query3a_alt="read(hdfs('books'))-> transform $.title;"

#Query 3b. Project the user from each review of each book using the short-hand projection notation.  The double-stars flattens the contained arrays.
query3b="read(hdfs('books'))[*].reviews[*].user -> expand;"

#Query 3b-alt. Or using equivalent the long-hand notation.
query3b_alt="read(hdfs('books'))-> expand $.reviews-> transform $.user;"

#Query 4. Find authors, titles, and reviews of books where a review, prompted a discussion by the user 'ben'. 
query4="read(hdfs('books'))-> filter 'ben' in ($.reviews[*].discussion[*].user -> expand)-> transform { $.author, $.title, $.reviews };"

#Query 5. Find the authors and titles of books that had an average review rating over 5. 
query5="read(hdfs('books'))-> filter avg($.reviews[*].rating) > 5 -> transform {$.author, $.title};"

#Query 6. Show how many books each publisher has published. 
query6="read(hdfs('books'))-> group by $p = ($.publisher) into {publisher: $p, num: count($)}-> sort by [$.publisher];"

#Query 7. Find the publisher who published the most books. 
query7="read(hdfs('books'))-> group by $p = ($.publisher) into {publisher: $p, num: count($)} -> top 1 by [$.num desc];"

#Query 8. Define a function
query8="$introMessage = fn($input, $id) ($input -> filter $.from == $id-> transform { mandatory: $.msg }); [{ from: 101, to: [102],ts: 1243361567, msg: 'Hello, world!'},{ from: 201, to: [20, 81, 94],ts: 1243361567,msg: 'Hello, world! was interesting, but lets start a new topic please' },{ from: 81, to: [201, 94, 40],ts: 1243361567, msg: 'Agreed, this topic is not for Joe, but more suitable for Ann' },{ from: 40, to: [201, 81, 94],ts: 1243361567,msg: 'Thanks for including me on this topic about nothing... reminds me of a Seinfeld episode.'},{ from: 20, to: [81, 201, 94],ts: 1243361567, msg: 'Where did the topic go.. hopefully its more than about nothing.' }  ] -> $introMessage(101);"	

#Query 9. Group exmaple
query9="[{id:1, dept: 1, income:12000},{id:2, dept: 1, income:13000},{id:3, dept: 2, income:15000},{id:4, dept: 1, income:10000},{id:5, dept: 3, income:8000},{id:6, dept: 2, income:5000},{id:7, dept: 1, income:24000}] -> group by $dept_group = $.dept into {$dept_group, total: sum($[*].income)};"

#Query 10. join exmaple
query10="$users = [{name: 'Jon Doe', password: 'asdf1234', id: 1},{name: 'Jane Doe', password: 'qwertyui', id: 2},{name: 'Max Mustermann', password: 'q1w2e3r4', id: 3}];$pages = [{userid: 1, url:'code.google.com/p/jaql/'},{userid: 2, url:'www.cnn.com'},{userid: 1, url:'java.sun.com/javase/6/docs/api/'}];join $users, $pages where $users.id == $pages.userid into {$users.name, $pages.*};"

#Query 11. Expand example
query11="[{name:'Jon Doe', movie_ids:[3,65,8,72]}, {name:'Jane Dean', movie_ids:[5,98,2,65]}]-> expand $.movie_ids;"


queries=[hdfswrite,query1,query2,query3a,query3a_alt,query3b,query3b_alt,query4,query5,query6,query7,query8,query9,query10,query11]

# Run kinds of query samples, demonstrate core features provided by current jaql. like "filter, transform, expand, group,join, function definiation, etc" 
def run(sample_id):
	query=queries[sample_id]
	print"------------------------------------------"
	print "QUERY SAMPLE %d : %s" % (sample_id,query)
	print"------------------------------------------"
	#init jaql environment, startup JVM
	try:
		jaql=pyJaql.Jaql()
		it=jaql.execute(query)
		for record in it:
			print type(record)
			print record
	except pyJaql.JAQLRuntimeException,err:
		print err.msg
	except pyJaql.JVMException,err:
		print err.msg
	

		
#Retreive data field sample, demonstrate how to manipulate result data set.
def retreive_field():
	try:
		jaql=pyJaql.Jaql()
		it=jaql.execute("read(hdfs('books'));")
		for book in it:
			if book["title"]=="Chamber of Secrets":
				print book["reviews"][0]["review"]
	except pyJaql.JAQLRuntimeException,err:
		print err.msg
	except pyJaql.JVMException,err:
		print err.msg

# Set variables in jaql expression and assign values to these variables by 2nd argument of execute() function  
def extension_statement_query():
	try:
		jaql=pyJaql.Jaql()
		it=jaql.execute("read(hdfs('books'))->filter $.title==$title and $.year==$year;",{"title":"Chamber of Secrets","year":1999})
		for book in it:
			print book["reviews"][0]["review"]
	except pyJaql.JAQLRuntimeException,err:
		print err.msg
	except pyJaql.JVMException,err:
		print err.msg
	
# Run a jaql script.
def script_query():
	try:
		jaql=pyJaql.Jaql()
		it=jaql.execute_script("test.jql",{"test":"testExecuteScript.dat"})
		for book in it:
			print book
	except pyJaql.JAQLRuntimeException,err:
		print err.msg
	except pyJaql.JVMException,err:
		print err.msg
	finally:
		jaql.close()	

def testTrue():
	jaql=pyJaql.Jaql()
	it=jaql.evaluate("[{'name':'alex','age':20,'isStudent':true},{'name':'jack','age':23,'isStudent':false}]->filter $.isStudent==$bool and $.name=='alex'->transform $.name;",{"name":"alex","bool":True})
	print it[0]

def eval_script():
	jaql=pyJaql.Jaql()
	value=jaql.evaluate_script("test.jql",{"test":"testEvaluteScript.dat"})
	print value

#RUN SAMPLES
# 1 run a query
#--- run(QUERY_NUM)
if __name__=="__main__":
	run(1)
	#retreive_field()
	#extension_statement_query()
	#testTrue()
	#script_query()

	


