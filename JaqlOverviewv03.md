# Introduction #

In this document, we introduce Jaql, a query language for
JavaScript Object Notation or [JSON](http://www.json.org/).
Although Jaql has been designed specifically for JSON,
we have tried to borrow some of the best features of
[SQL](http://en.wikipedia.org/wiki/SQL), [XQuery](http://www.w3.org/TR/xquery/), and
[PigLatin](http://wiki.apache.org/pig/PigLatin).
Our high-level design objectives include:

  * Semi-structured analytics: easy manipulation and analysis of JSON data
  * Parallelism: Jaql queries that process large amounts of data must be able to take advantage of scaled-out architectures
  * Extensibility: users must be able to easily extend Jaql

We begin with an example of JSON data, then go on to describe
the key features of Jaql and show how it can be used to process
JSON data in parallel using Hadoop's map/reduce framework.
Jaql is designed to be extended. Both [functions](Functionsv03.md)
as well as [IOv03](IOv03.md) can be extended so as to facilitate
plugging in your computation for your data. While
only a high-level description of Jaql is presented here,
all examples can be run by
[downloading](http://www.jaql.org/release/0.2/jaql-0.2.tgz) the
most recent release of Jaql and following the [instructions](Runningv03.md).

_Note:_ Jaql is still in early development,
so beware that it is likely to change over the next few months.
The future development plans are outlined in the [roadmap](RoadMapv03.md).
To see a preview of how we plan to further simplify Jaql, please see the
[slides](http://jaql.googlecode.com/files/Jaql-Pipes-Intro.ppt) that we
presented at the Hadoop User Group on 10/16/2008.

# A JSON Example #

Let's start off with an example of books and their reviews in JSON format

_Note:_ while Jaql can consume strict JSON, it can also consume minor variants of JSON. In the examples, for the sake of clarity, we omit double-quotes around field names and use single-quotes in place of double-quotes for strings.

```
  [
    {publisher: 'Scholastic',
     author: 'J. K. Rowling',
     title: 'Deathly Hallows',
     year: 2007},

    {publisher: 'Scholastic',
     author: 'J. K. Rowling',
     title: 'Chamber of Secrets',
     year: 1999, 
     reviews: [
       {rating: 10, user: 'joe', review: 'The best ...'},
       {rating: 6, user: 'mary', review: 'Average ...'}]},

    {publisher: 'Scholastic',
     author: 'J. K. Rowling',
     title: 'Sorcerers Stone',
     year: 1998},

    {publisher: 'Scholastic',
     author: 'R. L. Stine',
     title: 'Monster Blood IV',
     year: 1997, 
     reviews: [
       {rating: 8, user: 'rob', review: 'High on my list...'}, 
       {rating: 2, user: 'mike', review: 'Not worth the paper ...', 
        discussion:
          [{user: 'ben', text: 'This is too harsh...'}, 
           {user: 'jill', text: 'I agree ...'}]}]},

    {publisher: 'Grosset',
     author: 'Carolyn Keene',
     title: 'The Secret of Kane',
     year: 1930}
  ]
```

This example shows an _array_ of JSON _objects_.
Arrays are delimited by brackets '[.md](.md)' and objects are delimited by braces '{}'.
Objects contain name:value pairs or _members_, where the
value can be an atomic type or a nested value.
In contrast to XML, the type of an atomic value is always known in JSON.
Here, each top-level object represents a book and its reviews.
The 'reviews' for a book object is an array and each entry
in the array corresponds to a review.
Each review consists of a 'rating', its 'user', and the text of the review.
Each review can also contain a 'discussion', which
itself can be discussed, forming a discussion thread.

## Why did we pick JSON? ##

[JSON](http://www.json.org/) has become a popular data format for many Web-based applications because of its simplicity and modeling flexibility.
Wikipedia includes a nice [summary](http://en.wikipedia.org/wiki/JSON)
of JSON's advantages over other data formats like XML.
In contrast to XML, which was originally designed as a markup language,
JSON was actually designed for data.
Moreover, JSON is a language-independent format, with bindings
in a variety of programming languages.
In short, JSON makes it easy to model a wide spectrum of
data, ranging from homogenous flat data to
heterogeneous nested data, and it can do this
in a language-independent format.
We believe that these characteristics make JSON
an ideal data format for many Hadoop applications
and databases in general.

It is important to point out that the Jaql
data model is actually a superset of JSON.
The only difference is that atomic types like date have been added to Jaql to
make certain database operations more efficient.
Also, for readability, the names in name:value pairs
do not always need to be quoted.
When data needs to be exported in standard JSON format,
Jaql can convert non-standard atomic types to strings
and add quotes to names.
For the remainder of this document, we will use the Jaql data
model and JSON interchangeably, although strictly speaking
the Jaql data model is a superset of JSON.

# The Jaql Query Language #

Jaql is a functional query language that provides users with a simple,
declarative syntax to do things like filter, join, and group JSON data.
Jaql also allows user-defined functions to be written and used in expressions.
Let's begin with some examples using the book data presented earlier:
Our first two queries illustrate selection, projection, and filtering.

```
  // Write the books collection from data above.
  // hdfsWrite('books', [ {publisher... ] )
  
  // Query 1. Return the publisher and title of each book.
  for( $b in hdfsRead('books') )
    [{ $b.publisher, $b.title }];
	
  // result...
  [  
    {publisher: 'Scholastic', title: 'Deathly Hallows'},
    {publisher: 'Scholastic', title: 'Chamber of Secrets'},
    {publisher: 'Scholastic', title: 'Sorcerers Stone'},
    {publisher: 'Scholastic', title: 'Monster Blood IV'},
    {publisher: 'Grosset',    title: 'The Secret of Kane'}
  ];
  
  // Query 2. Find the authors and titles of books that have received
  // a review.
  for( $b in hdfsRead('books') )
   if( exists($b.reviews) )
    [{ $b.author, $b.title }];
    
  // result...
  [  
    {author: 'J. K. Rowling', title: 'Chamber of Secrets'},
    {author: 'R. L. Stine',   title: 'Monster Blood IV'}
  ];
```

Query 1 uses a **for** expression to loop over the books
collection.  Each book is bound to the variable $b and the loop body
is evaluated.  Both the in-expression and the loop body must produce
arrays.  The result of the for is an array that contains all of the
loop iteration results. In Query 1, every iteration returns a record
containing the publisher and title.  In Query 2, the **if**
expression causes only books with reviews to be output.

```
  // Query 3a. Project the title from each book using the short-hand
  // projection notation. 
  hdfsRead('books')[*].title;
  
  // Query 3a-alt. Or using equivalent the long-hand notation.
  for( $b in hdfsRead('books') )
    [ $b.title ];

  // Query 3b. Project the user from each review of each book using the short-hand
  // projection notation.  The double-stars flattens the contained arrays.
  hdfsRead('books')[**].reviews[*].user;

  // Query 3b-alt. Or using equivalent the long-hand notation.
  for( $b in hdfsRead('books') )
    for( $r in $b.reviews )
      [ $r.user ];

  // result...
  [ "Deathly Hallows",
    "Chamber of Secrets",
    "Sorcerers Stone",
    "Monster Blood IV",
    "The Secret of Kane" ];

  // Query 4. Find authors, titles, and reviews of books where a review
  // prompted a discussion by the user 'ben'.
  for( $b in hdfsRead('books') )
   if( 'ben' in $b.reviews[**].discussion[*].user )
    [{ $b.author, $b.title, $b.reviews }];
	
  // result...
  [  
    {author: 'R. L. Stine',
     title: 'Monster Blood IV',
     reviews: [
       {rating: 8, user: 'rob', review: 'High on my list...'},
       {rating: 2, user: 'mike', review: 'Not worth the paper ...',
        discussion: [
          {user: 'ben', text: 'This is too harsh...'},
          {user: 'jill', text: 'I agree ...'}]}]}
  ];
```

Queries 3a and 3b shows two short-hand notations for projecting
from an array, as well as their equivalent long-hand notations.

Query 4 is a more complicated variation of Query 2.  Now the
predicate is based on values that are nested three levels deep (books,
reviews, discussion). The **in** predicate checks if the user 'ben'
discussed any book review.

```
  // Query 5. Find the authors and titles of books that had an
  // average review rating over 5.
  for( $b in hdfsRead('books') )
   if( avg($b.reviews[*].rating) > 5 )
    [{ $b.author, $b.title }];
		 	
  // result...
  [  
    {author: 'J. K. Rowling', title: 'Chamber of Secrets'}
  ];
```

Query 5 filters books by their average rating.
For each book $b, the syntax `$b.reviews[*].rating` creates an array of review
ratings for each book `$b`. The average rating for book `$b` is then computed and tested to see if it is greater than 5.

```
  // Query 6. Show how many books each publisher has published.
  group( $b in hdfsRead('books') by $p = $b.publisher into $pubs )
      [{ publisher: $p, num: count($pubs) }];
	
  // result...
  [  
    {publisher: 'Scholastic', num: 4},
    {publisher: 'Grosset', num: 1}
  ];
```

Query 6 illustrates grouping and aggregation.
The **group** expression partitions an input collection into groups.
Books are partitioned into groups by the grouping value `$p`,
which is set to `$b.publisher`.
The `$pubs` variable is bound to an array associated with each
group and used to count how many books each publisher has published.

```
  // Query 7. Find the publisher who published the most books.

  // group books by publisher and compute their book count
  $g = group( $b in hdfsRead('books') by $p = $b.publisher into $pubs )
           [{ publisher: $p, num: count($pubs) }];
    
  // sort publishers by descending book count
  $sorted = sort( $i in $g by $i.num desc );

  // return the top publisher
  $sorted[0];
	 
  // result...
  {publisher: 'Scholastic', num: 4};
```

Query 7 illustrates grouping and sorting.  It shows how
**group** and **sort** can be used to find the publisher who
published the most books.  Variables are used to make it easier to
write the query.  The **group** expression can also be used to
group multiple collections.  To illustrate group, we define the
collections X and Y:

```
  hdfsWrite('X',
    [
      {a:1, b:1}, 
      {a:1, b:2}, 
      {a:2, b:3}, 
      {a:2, b:4}
    ] );

  hdfsWrite('Y',
    [
      {c:2, d:1}, 
      {c:2, d:2}, 
      {c:3, d:3}, 
      {c:3, d:4}
    ] );
```

Query 8 groups both X and Y, which is similar to a group over the
union of the input collections, except that two arrays are generated
for each group, one for each input collection.

```
  // Query 8. Co-group X and Y.
  group( $x in hdfsRead('X') by $g = $x.a into $xgroup,
         $y in hdfsRead('Y') by $g = $y.c into $ygroup )
      [{ g: $g, b: $xgroup[*].b, d: $ygroup[*].d }];

  // result...
  [ 
    {g: 1, b: [1,2], d: []},
    {g: 2, b: [3,4], d: [1,2]},
    {g: 3, b: [],    d: [3,4]}
  ];
```

In Query 8, both X and Y need to be grouped on the same value,
namely `$g`.  The syntax `$xgroup[*].b` and `$ygroup[*].d` in the return
expression projects the 'b' and 'd' values in the arrays created for
each X and Y group, respectively.

Joins can be expressed using group, but the syntax can get a little messy.
Since joins are common, special syntax has been introduced for them:

```
  // Query 9. Join X and Y.
  join( $x in hdfsRead('X') on $x.a,
        $y in hdfsRead('Y') on $y.c )
     [{ $x.a, $x.b, $y.c, $y.d }];
  
  // result...
  [ 
    {a: 2, b: 3, c: 2, d: 1},
    {a: 2, b: 3, c: 2, d: 2},
    {a: 2, b: 4, c: 2, d: 1},
    {a: 2, b: 4, c: 2, d: 2},
  ];
```

Query 9 shows an inner join on 'a' and 'c' in X and Y, respectively.
Although it has not be shown, left-, right-, and full-outer joins
can also be specified using modifiers.

# Running Jaql in Parallel #

In this section, we show how Jaql queries are evaluated in parallel using
Hadoop's map/reduce. Jaql includes a function that directly wraps Hadoop's
map/reduce, called `mapReduce()`.
It takes a Jaql description of
the map and reduce functions as input, and uses it to run a map/reduce
job in Hadoop.

```
// Write to an HDFS file called 'sample'.
  hdfsWrite('sample.dat', [
    {x: 0, text: 'zero'},
    {x: 1, text: 'one'},
    {x: 0, text: 'two'},
    {x: 1, text: 'three'},
    {x: 0, text: 'four'},
    {x: 1, text: 'five'},
    {x: 0, text: 'six'},
    {x: 1, text: 'seven'},
    {x: 0, text: 'eight'}
  ]);
	
  // Run a map/reduce job that counts the number objects
  // for each 'x' value.
  mapReduce( 
    { input:  {type: 'hdfs', location: 'sample.dat'}, 
      output: {type: 'hdfs', location: 'results.dat'}, 
      map:    fn($i) [ [$i.x, 1] ], 
      reduce: fn($x, $v) [ {x: $x, num: count($v)} ]
    });
	
  // Read the results...
  hdfsRead('results.dat');
	
  // result...
  [
    {x: '0', num: 5},
    {x: '1', num: 4}
  ];
```

This example groups the input on 'x' and counts the
number of objects in each group.
The map function must specify how to extract a key-value pair,
and the reduce function must specify how to aggregate the
values for a given key.
Here, the key value is set to $i.x and count($v) is
used to count the values $v associated with each key.
Note that both the map and reduce functions need to output an array
because each input is allowed to produce multiple outputs.

The mapReduce() function touches on a interesting feature of Jaql,
namely that Jaql is a second-order language.
This allows function definitions to be assigned to variables
and later evaluated. In mapReduce(), the map and reduce phases are specified
using Jaql functions. Each mapper and reducer that is spawned for a Jaql job
uses a Jaql interpreter to evaluate the respective jaql map and reduce function.

The mapReduce() is useful for programmers who want to run a map/reduce job
over JSON data and exploit the expressive power of Jaql yet free themselves
from all the little details required to actually set up and run a map/reduce job. However, we typically write queries using the higher level operators (e.g., `for, group, sort, ...`) and let the Jaql rewriter transform the declarative queries into (possiblly multiple) mapReduce() calls. A simple example is Query 1-- a scan (e.g., `for`) is readily translated into a map-only job as follows:

```
  // Query 1. Return the publisher and title of each book.
  for( $b in hdfsRead('books') )
    [{ $b.publisher, $b.title }];


  // Explain Query 1: Jaql automatically rewrites the query into a map-only job
  stRead(
    mapReduce(
      {input  : { type: "hdfs", location: "books"}, 
       output : HadoopTemp(), 
       map    : fn ($mapIn) [ [null,{ $mapIn.publisher, $mapIn.title }]]
      }));
```

Note that Query 1 is rewritten into a valid Jaql query, namely one that directly
corresponds to a map-only job (i.e., a parallel scan of the input file).
The `for` loop's input file directly corresponds to the `input` argument of
mapReduce(). Since no output was specified, it is written to a temporary file that is then read (`stRead`) and returned to the client. The body of the `for` loop is
translated to the `map` function of mapReduce(). In mapReduce(), the `map` function is assumed to take one input (a key is not required), and an array of key, value pairs are returned. For Query 1, no key returned and the value is a projection of the input record.

All of the other example queries listed here are rewritten into mapReduce() function calls and evaluated as map-reduce jobs. For example, `group` is translated to a mapReduce() that includes both a map and reduce function. Similarly, `join` is translated to `group`, which is then translated to mapReduce(). If the output of a `for` loop is `grouped`, both are translated into a single mapReduce(). For grouping, if additional information about the aggregate function is provided, Jaql can automatically rewrite queries so that they can take advantage of map/reduce's `combine`. Please refer to the discussion on [aggregate functions](Functionsv03.md) to see how you can define your own, plug them into Jaql, and how they're evaluated in parallel.

## Word-count Example ##

Jaql can also be used to process non-JSON data; the
[word-count](http://hadoop.apache.org/core/docs/r0.19.0/mapred_tutorial.html#Example%3A+WordCount+v1.0) example provides a good case-study to see how various
Jaql components work together. Lets say you have a file **docs/jaql-overview.html** whose words you want to count. Les first copy the input file into HDFS:

```
hdfsShell("-copyFromLocal docs/jaql-overview.html test");
```

The `hdfsShell` function passes its argument, `"-copyFromLocal docs/jaql-overview.html test"` to the `hadoop fs` command provided by hadoop. The input file is then available as `test` in hdfs so can be read by `hdfsRead` and processed in
parallel using map-reduce. The `hdfsShell` function is a simple example of how Jaql
can be extended with user-defined functions; more examples are provided in [Functionsv03](Functionsv03.md).
To read `test` into Jaql, use the expression below:

```
$lineRdr = hdfsRead("test", 
                    {format: "org.apache.hadoop.mapred.TextInputFormat",  
                     converter: "com.acme.extensions.data.FromLineConverter"});
```

This expression assigns the result of `hdfsRead` to the variable `$lineRdr`.
`hdfsRead` always produces a JSON array; in this case, a JSON array of Strings, each String representing a line from the input file. The conversion to JSON is specified by
the `format` and `converter` parameters. The `format` specifies a Hadoop
InputFormat to convert the file into a collection of `key, value` records.
As in Hadoop's word-count example, the `TextInputFormat` is used to produce `line number, Text` records. The `converter` specifies how to transform a given record into a JSON value. The `FromLineConverter` discards the line-number and trivially converts the `Text` line into a JSON String. Note that without the `format` and
`converter` parameters, the defaults used are `SesquenceFileInputFormat` and Jaql's native JSON binary. More examples of [IOv03](IOv03.md) how how to further cutomize Jaql
to read/write other data sources and formats.

Before we can count words, we need to be able to tokenize a given line into words. The following expression registers an example user-defined function for this task:

```
registerFunction("splitArr", "com.acme.extensions.expr.SplitIterExpr");
```

When the Jaql interpreter encounters a `splitArr` invocation, it uses `splitArr's` associated, implementing class to evaluate the function. For now, it suffices to think of `splitArr` as calling Java's `String.split(...)` method. Further details are given in [Functionsv03](Functionsv03.md). Now that we know how to convert a file into JSON and tokenize lines, we can complete the word-count example:

### Word-count (a) ###
```
1. $tokens = for($line in $lineRdr) (
2.             $tokens = splitArr($line, " "),
3.             for($t in $tokens)
4.               [ [$t, 1] ]
             );

5. group($p in $tokens by $w = $p[0] into $words)
6.   [ [$w, sum($words[*][1])] ]);
```

These Jaql queries directly correspond to the map-reduce word-count example.
Each line is read in (1), tokenized in (2), and each word is output along
with a count of 1 in lines (3-4). This expression produces an arrray of pairs (`[$t,1]`) that is assigned to `$tokens` in line (1). The `$tokens` are
then grouped on word (`$w = $p[0]`) in line (5). Each distinct word (`$w`) is associated with an array of `<word, count>` pairs (`$words`) whose count (`[1]`) is summed in line (6) to produce a final array of pairs, associating for each distinct word in the input file (`test`), its count. A semantically equivalent, yet simpler Jaql query is as follows:

### Word-count (b) ###
```
1. $tokens = for($line in $lineRdr) (
2.             $tokens = splitArr($line, " "),
3.             for($t in $tokens)
4.               [ $t ]
             );

5. group($p in $tokens by $w = $p into $words)
6.   [ [$w, count($words)] ];
```

As in the previous example, each line is read (1) and tokenized (2). Instead of returning an array of `<word, count>` pairs, an array of words is returned in lines (3-4).
As before, the words are grouped in line (5). However, instead of summing word counts using explicit counts as in the previous example, the `$words` array is simply counted. To complete the example, the translated Jaql is shown below:

```
stRead(
  mrAggregate(
    {input: {type:"hdfs", location: "test", 
             inoptions: {format:"org.apache.hadoop.mapred.TextInputFormat",          
                         converter:"com.acme.extensions.data.FromLineConverter"}},
     output: HadoopTemp(), 
     init: fn($mapIn0) 
             for( $p in splitArr($mapIn0, " ") )
               [[$p, [1]]],
     combine: fn($ckey0, $ca0, $cb0) [
                if(isnull($ca0[0]))
                  $cb0[0]
                else if(isnull($cb0[0]))
                  $ca0[0]
                else $ca0[0] + $cb0[0]
              ],
     final: fn($w, $words) 
              [[$w, firstNonNull($words[0], 0)]]
          }
  )
)
```

In this case, the Jaql compiler uses the `mrAggregate` function that directly calls Hadoop's map-reduce. The `input` parameter is derived the the `hdfsRead` expression and the `output` is to a temp file. Since `count` is a distributive aggregate function, it can be decomposed into partial counts which are then summed. The `init` parameter is a function that tokenizes a given line and outputs `<word, 1>` pairs, much like the explicit example shown in **Word-count (a)**. The `combine` parameters is a function that takes two partial results (`$ca0, $cb0`) for a word (`$ckey0`) and returns their sum if both partials are non-null (otherwise returning the non-null partial). The `final` parameter is a function that associates a word with a final count. The `init` and `combine` functions are used in the `map` phase while both the `combine` and `final` functions are used in the `reduce` phase.