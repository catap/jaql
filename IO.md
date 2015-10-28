# Introduction #

Jaql has been designed to flexibly read and write data
from a variety of data stores and formats. Its core IO
functions are `read` and `write` functions that are
parametrized for specific data stores (e.g., file system, database, or a web service)
and formats (e.g., JSON, XML, or CSV).

With regards to query processing, the IO functions also inform Jaql about whether data can be processed in parallel or must be processed serially. The main criteria for parallel access depends on whether or not [InputFormat](http://hadoop.apache.org/mapreduce/docs/r0.21.0/api/org/apache/hadoop/mapred/InputFormat.html) (for read) and [OutputFormat](http://hadoop.apache.org/mapreduce/docs/r0.21.0/api/org/apache/hadoop/mapred/OutputFormat.html) (for write) are available. For example, Hadoop's distributed file system ([HDFS](http://hadoop.apache.org/hdfs/)) has several such Input and Output formats available that are readily used by [MapReduce](http://http://hadoop.apache.org/mapreduce/) jobs. Parallel IO is the pre-requisite for parallel processing with Jaql, so it is the most commonly used way to read and write data.

There are times, however, when a data store is needed but it only supports serial access. Examples of such data sources include standard, local file systems and web services. Jaql supports such serial IO but does not attempt to parallelize expressions that use them.

The general form of Jaql's IO functions is: `[ T ] read( fd )` and `fd write( [T], fd )`. That is, `read` produces an array of type `T` and `write` consumes an array of type `T`. The file descriptor, `fd` is simply a JSON record that determines whether access is parallel or serial as well as formatting, location, and other options. File descriptors can be specified explicitly or through the use of helper functions that construct appropriate, useful file descriptors. Consider the `del` file descriptor constructor: `del( string fileName )`. It requires one argument, `fileName` but can be given additional arguments: `del( string fileName, { * ) options )` to override default behavior.

Here, we describe the supported data stores and formats. For further discussion regarding how to plug-in your own data stores and formats, see the discussion of IOArchitecture and IOExtensibility.

  * [Parallel IO](#Parallel_IO.md)
    * [Text Line Files](#Text_Line_Files.md)
    * [Delimited Text Files](#Delimited_Text_Files.md)
    * [Binary Files](#Binary_Files.md)
    * [Binary Files (schema)](#Binary_Files_(schema).md)
    * [JSON Text Files](#JSON_Text_Files.md)
    * [File Name Globbing Patterns](#Globbing_Patterns.md)
    * [Task List](#Task_List.md)
    * [HBase](#HBase.md)
  * [Serial IO](#Serial_IO.md)
    * [JSON Files](#File.md)
    * [JSON Net Data](#URI.md)

## Parallel IO ##

### Text Line Files ###

**Reading:** ` [ string * ] read(lines( fname )) `

Read a file of string lines. Each line is represented as a single string element of the array that is returned by `read`. By default, the newline terminator is `\n`. Consider the following files:

Example:
```
% cat file1.txt
here is the first line of the file
"here is another line, but with quotes"
1001
```

Using `read(lines('file1.txt'))`, we get:

```
jaql> read(lines('file1.txt'));
[
  "here is the first line of the file",
  "\"here is another line, but with quotes\"",
  "1001"
]

jaql> read(lines('file1.txt')) -> transform strlen($);
[
  34,
  39,
  4
]
```

**Reading (with schema):** `read(lines( fname, { schema: schema long } ))`

Note that while the last line is a number (`1001`), it get converted to a string by default. Where appropriate, each line can be converted to a given schema type if the `schema` option is passed in:

```
% cat file2.txt
123
-1
42
901
```

We can convert this file into an array of longs as follows:
```
jaql> read(lines("file2.txt", { schema: schema long }));
[
  123,
  -1,
  42,
  901
]
```

**Writing (default):** ` fd write([T] ,lines( fname )); `

**Writing (with conversion):** ` fd write([ T ], lines( fname, { schema: schematype } )); `

Write consumes the input array `[ T ]` and writes the file `fname`, outputting one line per element of `[ T ]`. Optionally, the lines can be converted according to the specified schema, ` { schema: schematype } `. Write outputs the file descriptor `fd` that can be used to read the file that was just written. Consider the following examples:

```
// write a file with the array [1,2,3]
jaql> write([1,2,3], lines( "file3.txt" ));
{
  "location": "file3.txt",
  "type": "lines"
}

// or write "file3.txt" using the piping syntax
jaql> [1,2,3] -> write(lines( "file3.txt" ));
{
  "location": "file3.txt",
  "type": "lines"
}

// read the contents back (note the implicit conversion to strings)
jaql> read(lines("file3.txt"));
[
  "1",
  "2",
  "3"
]

// or, exploit the return value of write
jaql> x = [1,2,3] -> write(lines( "file3.txt" ));

// and read it in via the fd that is bound to x
jaql> read(x);
[
  "1",
  "2",
  "3"
]

// finally, here is an example that retains the long input types
jaql> [1,2,3] -> write(lines( "file4.txt", {schema: schema long}));
{
  "inoptions": {
    "schema": schema long
  },
  "location": "file4.txt",
  "type": "lines"
}

// if we can that file, we get:
% cat file4.txt
1
2
3

// if we just read in this file using default lines, we lose the type info
jaql> read(lines( "file4.txt" ));
[
  "1",
  "2",
  "3"
]

// but if we use the fd produced by the write of file4.txt, we get back what we intended
jaql> y = [1,2,3] -> write(lines( "file4.txt", {schema: schema long}));

jaql> read(y);
[
  1,
  2,
  3
]
```

### Delimited Text Files ###

Jaql supports converters (`del`) that make easy to read and write text files that are organized using field and record delimiters. Examples of such files include CSV and DEL (useful for importing and exporting data between DB2 and text files). In contrast to `lines` which is used to produce an array of atomic types, typically of type `string`, `del` is used to produce arrays of records or arrays that are nested only one-level deep. We first consider the more commonly used cases, then go over additional options that are supported by `del`.

**Reading:** `[ [ string * ] ] read(del(fname));`

In its simplest form, `read(del( ... ))` returns an array of elements, one element per line. The size of the element array depends on the number of fields in the lines of `fname`. Note that each input line must have the same number of fields.

```
% cat file5.txt
a,b,c
1,2,3
"hi","there","!"

jaql> read(del("file5.txt"));
[
  [
    "a",
    "b",
    "c"
  ],
  [
    "1",
    "2",
    "3"
  ],
  [
    "hi",
    "there",
    "!"
  ]
]
```

**Reading (with schema):** ` [ schematype * ] read(del(fname, {schema: [ schematype ]}));`

With an optional schema, `read(del(...))` converts its input to the specified schema.
For example:
```
cat file6.txt
1,2,3
-1,-2,-3
100,200,300

// without schema information
jaql> read(del("file6.txt"));
[
  [
    "1",
    "2",
    "3"
  ],
  [
    "-1",
    "-2",
    "-3"
  ],
  [
    "100",
    "200",
    "300"
  ]
]

// with schema information: array of three longs for each element
jaql> read(del( "file6.txt", { schema: schema [ long, long, long ] }));
[
  [
    1,
    2,
    3
  ],
  [
    -1,
    -2,
    -3
  ],
  [
    100,
    200,
    300
  ]
]

// to return records instead, specify the appropriate schema
jaql> read(del( "file6.txt", { schema: schema { a: long, b: long, c: long } }));
[
  {
    "a": 1,
    "b": 2,
    "c": 3
  },
  {
    "a": -1,
    "b": -2,
    "c": -3
  },
  {
    "a": 100,
    "b": 200,
    "c": 300
  }
]
```

**Writing delimited files**

```
jaql> x = [ [1,2,3], [-1,-2,-3], [100, 200, 300] ];

jaql> x -> write(del("file7.txt"));
{
  "location": "file7.txt",
  "type": "del"
}

% cat file7.txt
1,2,3
-1,-2,-3
100,200,300

// for records, a schema is required to determine the order of output fields
jaql> y = [ { a: 1, b: 2}, { a: 10, b: 20 } ];

jaql> y -> write(del( "file8.txt", { schema: schema { a, b }}));

% cat file8.txt
1,2
10,20

// now, reverse the field order by specifying { b, a } instead of { a, b }
jaql> y -> write(del( "file9.txt", { schema: schema { b, a }}));
2,1
20,10

```

**Additional options:**

In addition to schema, `del` has additional options to control delimiters, quotes, etc. Here is a full list of options, specified as name, value pairs that need to be specified in `del's` options record.

```
  schema: schema [ <value schema> ] { <fields> }

    For reads, both record and array schemas are supported. Schema values must be atomic
    and record and arrays must be closed, that is no optional fields are supported.
    For writes, only record schemas are used to control the order of fields in the output.
    
  delimiter: string of length 1

    Used to override the field delimiter to char other than ",".

  quoted: boolean

    Controls whether strings should be quoted.

  ddquote: boolean

    Controls whether the escape character is a double-quote (true) or backslash (false).

  escape: boolean

    Controls whether characters are escaped.
```

### Binary Files ###
While various text-based formats are often used for initial reads and exploration of
new data sets, intermediate file manipulation is often done with more efficient binary formats. Jaql's binary format, also referred to as JSON binary, stores Jaql values using a self-describing format. Jaql uses Hadoop's [Sequence Files](http://hadoop.apache.org/common/docs/r0.21.0/api/org/apache/hadoop/io/SequenceFile.html), where an array corresponds to a given file and each array element corresponds to a single record value. Here are several examples of reading and writing Jaql's binary format:

```
[
    {order: 1, cust: 'c1', items: [ 
      {item: 1, qty: 2},
      {item: 3, qty: 6},
      {item: 5, qty: 10}]},
    {order: 2, cust: 'c2', items: [
      {item: 2, qty: 1},
      {item: 5, qty: 2},
      {item: 7, qty: 3}]},
    {order: 3, cust: 'c1', items: [
      {item: 1, qty: 2},
      {item: 7, qty: 14},
      {item: 5, qty: 10}]} 
  ] -> write(hdfs('orders.dat'));
	
  // Read it back...
  read(hdfs('orders.dat'));
```

Note: Jaql's binary format is not designed for long term persistence that spans multiple versions of Jaql. For long term persistence, either use Jaql's text-based formats or other formats designed for long-term persistence.

### Binary Files (schema) ###
When writing binary files, if schema is available, Jaql's binary format with schema stores data much more compactly than the self-describing binary format (e.g., record field names are not repeated per record if declared in the schema). Similar to the self-describing binary format, Hadoop's Sequence Files represent an array and each element is stored in a single record value. Jaql uses this format for temporary files such as those serialized between the map and reduce steps. However, you can use this format as well to save on performance and disk space. Here are several examples:

```
jaql> data = [
    {order: 1, cust: 'c1', items: [ 
      {item: 1, qty: 2},
      {item: 3, qty: 6},
      {item: 5, qty: 10}]},
    {order: 2, cust: 'c2', items: [
      {item: 2, qty: 1},
      {item: 5, qty: 2},
      {item: 7, qty: 3}]},
    {order: 3, cust: 'c1', items: [
      {item: 1, qty: 2},
      {item: 7, qty: 14},
      {item: 5, qty: 10}]} 
  ];

jaql> schm = schema {
         "order": long,
         "cust": string,
         "items": [
                    {
                      "item": long,
                      "qty": long
                    } * 
                  ]
       };

jaql> data -> write(jaqltemp( "orders.bin", schm );

// note the file size when using self-decribing binary
% ls -l orders.dat
351

// now, the file is much smaller when schema information is provided
% ls -l orders.bin
167

// read back the data
jaql> read(jaqltemp( "orders.bin", schm ));
[
    {order: 1, cust: 'c1', items: [ 
      {item: 1, qty: 2},
      {item: 3, qty: 6},
      {item: 5, qty: 10}]},
    {order: 2, cust: 'c2', items: [
      {item: 2, qty: 1},
      {item: 5, qty: 2},
      {item: 7, qty: 3}]},
    {order: 3, cust: 'c1', items: [
      {item: 1, qty: 2},
      {item: 7, qty: 14},
      {item: 5, qty: 10}]} 
  ];
```

### JSON Text Files ###
The last parallel file format that is discussed is for reading and writing Jaql's text representation of Jaql types. When the types are restricted to JSON types, the text format is JSON (e.g., no dates, binaries, functions, etc.). Two ways of managing such JSON data has become relatively common: (1) store each JSON value of an array in a Text value of a Sequence File record, and (2) use Hadoop's Text Input and Output formats to store each JSON value per line. Since neither of these formats has been wrapped by a file descriptor constructor, we illustrate how to read and write by overriding several default options of Jaql's native binary format.

```
// Options needed to write data as JSON to Hadoop Text File
jaql> txtOutOpt = {format: "org.apache.hadoop.mapred.TextOutputFormat", 
                   converter: "com.ibm.jaql.io.hadoop.converter.ToJsonTextConverter"};

// Options needed to read data as JSON from Hadoop Text File
jaql> txtInOpt = {format: "org.apache.hadoop.mapred.TextInputFormat", 
                  converter: "com.ibm.jaql.io.hadoop.converter.FromJsonTextConverter"};

// write data to Hadoop Text File
jaql> data -> write(hdfs( "file10.txt", txtOutOpt ));

// read data back
jaql> read(hdfs( "file10.txt", txtInOpt ));

// Options needed to write data as JSON to Hadoop Sequence File
jaql> seqOutOpt = {converter: 'com.ibm.jaql.io.hadoop.converter.ToJsonTextConverter', 
                   configurator: 'com.ibm.jaql.io.hadoop.TextFileOutputConfigurator'};

// Options needed to read data as JSON from Hadoop Sequence File
jaql> seqInOpt = { converter: 'com.ibm.jaql.io.hadoop.converter.FromJsonTextConverter' };

// write data to Hadoop Sequence File
jaql> data -> write(hdfs( "file11.seq", seqOutOpt ));

// read data back
jaql> read(hdfs( "file11.seq", seqInOpt ));
```

### Task List ###
A task list is an array where each element of the array is assigned to a single task of a MapReduce job. This is useful, for example, when generating synthetic data sets. For such cases, data is not stored in HDFS at the beginning of the job, yet the job is processed in parallel. Here is an example that illustrates a very simple form of data generation:

```
jaql> fd = {type: 'array', inoptions: {array: [1,2,3]}};

// generate numbers from 1 to $ where $ is bound to an element of array, as defined above
jaql> read(fd) -> transform range(1, $);
[
  [
    1
  ],
  [
    1,
    2
  ],
  [
    1,
    2,
    3
  ]
]
```

Note that a MapReduce job composed of 3 tasks (one for each element of the array above) was evaluated. Thus, each nested array was generated by a separate task (e.g., process).

### HBase ###
Parallel reading and writing of HBase tables was supported at one point but has gone stale. It is currently under development.

### Globbing Patterns ###

Hadoop supports file name [globbing](http://hadoop.apache.org/common/docs/current/api/org/apache/hadoop/fs/FileSystem.html#globStatus%28org.apache.hadoop.fs.Path%29) so Jaql users can take advantage of this feature directly. The value of the `location` field is passed verbatim to Hadoop's FileSystem API's so all globbing patterns that Hadoop supports are also available to jaql users.

Consider a directory structure that starts at `data`, includes sub-directories named by their creation date, that in turn include sub-directories, one per source of data (e.g., database, logger, etc.). Example paths to consider are as follows:

  * data/04\_27\_2010/logs/machine1.log
  * data/04\_27\_2010/db/t1.dat
  * data/0.4\_28\_2010/logs/machine1.log

To read machine1's data for 04/27/2010, simply issue:
```
  read(hdfs("data/04_27_2010/logs/machine1.log"));
```

To read all data collected on 04/27/2010:
```
  read(hdfs("data/04_27_2010/*/*"));
```

When piped to further jaql expressions, the above examples allow you to operate on subsets of the data, effectively eliminating data that is not of current interest.
If needed, all data can be processed by using:
```
  read(hdfs("data/*/*/*"));
```

In some cases, certain paths may be of interest that cannot be easily expressed
using globbing patterns. For example, consider the simple case of needing to
read `data/04_27_2010/db/t1.dat` and `data/0.4_28_2010/logs/machine1.log`.
More generally, one can have an arbitrary collection of paths, modeled as an array
(e.g., ["data/04\_27\_2010/db/t1.dat", "data/0.4\_28\_2010/logs/machine1.log"]). To convert
this to a globbing pattern appropriate for hadoop, use:

```
  makeGlob = fn(ps) (

    strcat("/{", ps 
                 -> transform strcat(".",$) 
                 -> strJoin(","), 
           "}")

  );
```

For example:
```
  makeGlob(["data/04_27_2010/db/t1.dat", "data/0.4_28_2010/logs/machine1.log"]);
  // returns
  "/{.data/04_27_2010/db/t1.dat,.data/0.4_28_2010/logs/machine1.log}"
```

And can be used in conjunction with `read` as follows:

```
  paths = ["data/04_27_2010/db/t1.dat", "data/0.4_28_2010/logs/machine1.log"];
  read(hdfs(makeGlob(paths)));
```

## Serial IO ##
Some data stores do not support parallel access. For such data stores, Jaql's IO functions support serial access. Accessing such data stores may cause Jaql's query processing plan to significantly differ from a plan where parallel access is permitted. In some cases, the entire script may run serially, without any use of MapReduce. Below, we describe several ways of accessing serial data.

### File ###
Jaql can read and write data directly stored in local files. Values are serialized as text and pretty-printed for readability. When Jaql's non-JSON types are not used, the output is valid JSON. Reading and writing local files is useful for debugging small data since the file can be readily manipulated using standard editing tools. Here is an example:

```
// Example 1. Write to a file named 'hey.dat'.
jaql>[{text: 'Hello World'},{text: 'Another example'}] -> write(file('hey.json'));
{
  "location": "hey.json",
  "type": "local"
}	

// Read it back...
jaql> read(file('hey.json'));
[
  {
    "text": "Hello World"
  },
  {
    "text": "Another example"
  }
]

// contents of local file
% cat hey.json
[{
  "text": "Hello World"
},{
  "text": "Another example"
}]
```

### URI ###
Finally, to work with external data sources, we recently added a
httpGet() function that can retrieve JSON data from a URL.  Below are
two examples of httpGet() that get data from
[Freebase](http://www.freebase.com) and
[Yahoo! Traffic](http://developer.yahoo.com/traffic) (the
latter requires you to supply an
[application id](http://developer.yahoo.com/faq/#appid)).

```
  // Get albums recorded by "The Police" using Freebase.
  $artist = "The Police";
  $url = "http://api.freebase.com/api/service/mqlread";
  $query = 
   {query: {album: [], name: $artist, type: "/music/artist"}};
  read(http($url, {query: serialize($query)}))[0].result.album;
      
  // result...
  [ "Outlandos d\'Amour",
    "Reggatta de Blanc",
    "Zenyatta Mondatta",
    "Ghost in the Machine",
    "Synchronicity",
    "Every Breath You Take: The Singles",
    "Greatest Hits",
    "Message in a Box: The Complete Recordings (disc 1)",
    "Message in a Box: The Complete Recordings (disc 2)",
    "Message in a Box: The Complete Recordings (disc 3)",
    "Message in a Box: The Complete Recordings (disc 4)",
    "Live! (disc 1: Orpheum WBCN/Boston Broadcast)",
    "Live! (disc 2: Atlanta/Synchronicity Concert)",
    "Every Breath You Take: The Classics",
    "Their Greatest Hits",
    "Can\'t Stand Losing You",
    "Roxanne \'97 (Puff Daddy remix)",
    "Roxanne \'97"];
  
  // Get traffic incidents from Yahoo!.
  $appid = "YahooDemo"; // Set to your yahoo application ID
   $trafficData = 
      httpGet('http://local.yahooapis.com/MapsService/V1/trafficData',
        { appid:  "YahooDemo",
          street: "701 First Street",
          city:   "Sunnyvale",
          state:  "CA",
          output: "json"
        })[0],
  
  $trafficData.ResultSet.Result[*].title
  
  // result...
  [ "Road construction, on US-101 NB at FAIROAKS AVE TONBTO NB MATHILDA",
    "Road construction, on CA-85 SB at MOFFETT BLVD",
    "Road construction, on CA-237 EB at MATHILDA AVE TOEBTO FAIR OAKS AVE",
    "Road construction, on CA-237 WB at CROSSMAN AVE",
    "Road construction, on I-880 at GATEWAY BLVD"];
```