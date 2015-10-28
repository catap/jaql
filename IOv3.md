# Introduction #

Jaql has been designed to flexibly read and write data
from a variety of data stores and formats.
Input and output are handled through read/write expressions.
For example, `hdfsRead, hdfsWrite` access
[HDFS](http://hadoop.apache.org/core/docs/r0.15.3/hdfs_design.html) files,
`hbaseRead, hbaseWrite` access [HBase](http://hadoop.apache.org/hbase/) tables,
and `localRead, localWrite` access locally stored files.
HDFS files and HBase tables can be partitioned and processed using Map/Reduce so
are examples of Jaql's Hadoop IO. Raw HDFS or standard
filesystem files where raw bytes are expected are examples of Jaql's stream-oriented IO.
We first go over a few examples, then discuss Hadoop IO, stream IO, how to customize
them, and finally, how to add your own data stores.

# Reading and Writing JSON Data #

Using Jaql, JSON data can be stored and retrieved
from a variety of data sources including ordinary files.
Jaql queries can take a _collection_ as input and generate
a new collection as output, where a
collection corresponds to a JSON array.
An example using an ordinary file to store a collection
is as follows:

```
  // Example 1. Write to a file named 'hey.dat'.
  localWrite('hey.dat', [{text: 'Hello World'}]);
	
  // Read it back...
  localRead('hey.dat');
```

Here, a single object with the 'Hello World' string is
being written to a file called 'hey.dat' in the current directory.
It is also possible to read and write JSON data to
Hadoop [HDFS](http://lucene.apache.org/hadoop/hdfs_design.html) files and
[HBase](http://wiki.apache.org/lucene-hadoop/Hbase) tables.
We have made integration with Hadoop a priority by having all data types implement the
[WritableComparable](http://lucene.apache.org/hadoop/api/org/apache/hadoop/io/WritableComparable.html) interface.
By integrating Jaql with HDFS and HBase, we are able to store JSON data
and process it in parallel using Hadoop's map/reduce framework.

Examples using Hadoop are provided below.
Our second example writes to an HDFS
[SequenceFile](http://lucene.apache.org/hadoop/api/org/apache/hadoop/io/SequenceFile.html).
A SequenceFile is a collection of key-value pairs.
Jaql's hdfsWrite() function only writes data into the value field, leaving the key field empty.
In example 2, the input data is represented as a literal, but in general the input
can be an expression that is the result of a Jaql query.

```
  // Example 2. Write to a Hadoop SequenceFile named: 'orders.dat'.
  hdfsWrite('orders.dat', [
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
  ]);
	
  // Read it back...
  hdfsRead('orders.dat');
```

Our third example writes to an HBase table.
An HBase table is a collection of records, with each record containing a
primary key and a set of column name-value pairs.
Column names in HBase are of type
[Text](http://lucene.apache.org/hadoop/api/org/apache/hadoop/io/Text.html),
while column values are simply byte arrays.
HBase uses a two-part naming scheme for columns of the form 'column family:column'.
In Jaql, we use a sharp '#' instead of a colon ':' to separate column families from columns,
since the colon is already used as a separator in JSON.
If a column family is not specified, a special 'default' column family is used, as in the
following example.

```
  // Example 3. Write to an HBase table named 'webcrawl'.
  hbaseWrite('webcrawl', [
    {key: "www.cnn.com", page:'...', rank: 0.9,
     inlinks:[
       {link: 'www.news.com', anchor: 'newsite'},
       {link: 'www.jscript.com', anchor: 'look here'}]},
    {key: "www.json.org", page:'...', rank: 0.8}
  ]);
	
  // Read it back...
  hbaseRead('webcrawl');
```

In hbaseWrite(), all objects in the input are written to a
single HBase table.
Each top-level JSON object is stored as an Hbase record with the specified key,
and each name:value pair in the object is stored as a separate column-value pair in
the record.
Values are serialized as a byte array.
Note that only the outermost name:value pairs in top-level objects are
stored as separate columns in Hbase.
Nested arrays and objects are serialized within these.
In example 3, two HBase records are written.
Each record is used to store the content, rank, and in-links (if any) of a web page.

# Hadoop IO #

In Hadoop's [map-reduce](http://hadoop.apache.org/core/docs/r0.15.3/mapred_tutorial.html), data is accessed using [InputFormat](http://hadoop.apache.org/core/docs/r0.15.3/api/org/apache/hadoop/mapred/InputFormat.html) and [OutputFormat](http://hadoop.apache.org/core/docs/r0.15.3/api/org/apache/hadoop/mapred/OutputFormat.html). Classes that implement these interfaces provide enough information to map-reduce so that the data can be partitioned and processed in parallel.


Jaql's I/O framework supports any Input(Output)Format to be plugged-in.
However, Input(Output)Formats work with `Writables` while Jaql expects `Items`.
Thus, the framework makes it easy to convert `Writables` to and from `Items`.

## Default Input/Output Format ##

In order to make the discussion more concrete, lets look under the hood of the
`hdfsRead('books.jqlb')` expression.
First, `hdfsRead` is a `macro` expression that
makes use of a more generic expression called `read`.
The following is the actual `read` expression that is invoked
when using `hdfsRead`:

```
  read('hdfs', 'books.jqlb');
```

The first argument to the `read` expression is the name
associated with a type of data store.  Just as names are associated to
function implementations in the function registry, names are
associated to data store types in the storage registry. The second
argument to `read` is the path of a file stored in HDFS.

For the hdfs data store type, the registry entry specifies default Input(Output)Formats.
The defaults for Jaql are `SequenceFileInputFormat` and `SequenceFileOutputFormat`.

## Text-based Input/Output Format ##

Suppose that the HDFS file is a text file (i.e, new-line delimitted records). The [InputFormat](http://hadoop.apache.org/core/docs/r0.15.3/api/org/apache/hadoop/mapred/InputFormat.html) to use in this case is `TextInputFormat`. For this case, Jaql's default can be overriden as follows:

```
  read('hdfs', 'books.jql', {format: 'org.apache.hadoop.mapred.TextInputFormat'});
```

The additional argument to `read` is a record that specifies which class
will implement the [InputFormat](http://hadoop.apache.org/core/docs/r0.15.3/api/org/apache/hadoop/mapred/InputFormat.html). More generally, this record can specify any options that
are specific to a give data store type. In the example above, a `TextFile`'s
records are `Text`, not Jaql `Items` (i.e., binary JSON) so are converted
by implementing a converter and specifying the converter as an option `read`.
An example converter is implemented as follows:

```
  public class FromJSONTxtConverter extends HadoopRecordToItem { 
  ...
  @Override
  protected WritableToItem createValConverter() {
    return new WritableToItem() {
      public void convert(Writable src, Item tgt) {
        // expect src is of type Text
        // use a JSON parser to parse it
        // set tgt
      }
      ...
    };
    ...
  }
```

The `FromJSONTxtConverter` takes as input a `Writable` value and sets the
`Item` to the parsed result. The following shows how to use it in `read`:

```
  read('hdfs', 'books.jql', {format    : 'org.apache.hadoop.mapred.TextInputFormat',
                             converter : 'com.acme.extensions.data.FromJSONTxtConverter'});
```

The `read` expression uses `TextInputFormat` to read the file
specified at 'path' in HDFS. For each record retrieved from the file, it will use
`FromJSONTxtConverter` to convert each `Text Writable` to an `Item`.

## Using Custom File Formats ##

While extensible, the `read` expression is cumbersome to specify in this manner.
There are several options to hide the details. The simplest is to define a function:

```
$myRead = fn($path) read('hdfs', $path, 
                            {format    : 'org.apache.hadoop.mapred.TextInputFormat',
                             converter : 'com.acme.extensions.data.FromJSONTxtConverter'});
  
  $myRead('books.jql');
```

Another option is to register a new data store type. This is done through
a storage registry that maps a **name** to records that specify options for input and output. This is how Jaql keeps track of defaults for each data store. For example, the registry entry for 'hdfs' files is the following:

```
  {type:       'hdfs',
   inoptions :	{adapter      : 'com.ibm.impliance.jaql.DefaultHadoopInputAdapter', 
                 format       : 'org.apache.hadoop.mapred.SequenceFileInputFormat', 
                 configurator : 'com.ibm.impliance.jaql.FileInputConfigurator'},
   outoptions:	{adapter      : 'com.ibm.impliance.jaql.DefaultHadoopOutputAdapter', 
                 format       : 'org.apache.hadoop.mapred.SequenceFileOutputFormat', 
                 configurator : 'com.ibm.impliance.jaql.FileOutputConfigurator'}};
```

A data store is named by (type:'hdfs') which is used by `read` to find
associated options. There are two sets of options, one for input and one for output.
The default file format is a `SequenceFile` whose records are key, value
pairs whose types are `WritableComparable` and `Writable`, respectively.
By default, Jaql ignores the key. Since a converter is not specified, Jaql assumes that the value's type is an `Item`.

The 'hdfs' registry example includes additional options. The first is an `Adapter`. This is the glue that brings together all other options for a data source and encapsulates how to access data and how to
convert the data to `Items` (if needed). Thus, it produces an `Item` iterator
for Jaql from any data store. For Hadoop data, the adapter to use is `DefaultHadoopInput(Output)Adapter`.
The Hadoop adapter allows any existing Input(Output)Format to be swapped in along with any converter
(as shown by the earlier example). Another example of an adapter in Jaql is the StreamAdapter. It allows
access directly to byte stream data. Access to other data stores is possible by implementing the Adapter interface.

## Using Custom File Formats in Map-Reduce ##

If an Input(Output)Format can be specified for a given data store, Hadoop's map-reduce can use it as an input(output). Accordingly, the Hadoop adapter informs the Jaql compiler that Hadoop's map-reduce can be used if appropriate. However, Input(Output)Formats may require specific configuration prior to submitting a map-reduce job. In Jaql, this is exposed through the "configurator" option. For example, an Input(Output)Format requires
that a file path be specified before the job is submitted. The `com.ibm.impliance.jaql.FileInputConfigurator`
does exactly this: the Adapter passes 'books.jql' and all options to the configurator, which then configures the job appropriately. For many HDFS files, `com.ibm.impliance.jaql.FileInputConfigurator` is sufficient,
but if needed, it can be overriden.

Returning to the example, the new data store type is registered as follows:

```
  registerAdapter({type     :	'myHDFSFile',
                   inoptions:	{adapter      : 'com.ibm.jaql.io.hadoop.DefaultHadoopInputAdapter', 
                                 format       : 'org.apache.hadoop.mapred.TextInputFormat', 
                                 converter    : 'com.acme.extensions.data.FromJSONTxtConverter',
                                 configurator : 'com.ibm.jaql.io.hadoop.FileInputConfigurator'}});
```

The Hadoop adapter and configurator are specified, along with the `TextInputFormat` and custom converter. The newly registered data store is used as follows:

```
  read('myHDFSFile', 'books.jql');
```

If a new data store type is very common, it may be convenient to
define a Jaql function that hides some of the details:

```
  $readMyFile = fn($name) read('myHDFSFile', $name);

  $readMyFile('books.jql');
```

In addition to HDFS files, Jaql supports HBase as a data source. This is supported by
the same Hadoop adapter, but parameterized by Input(Output)Formats and configurators that are specific to HBase.
For HBase, the 'path' represents a table name. Columns and column families can be specified as additional options.

Finally, in order to do something more interesting with these examples,
consider a query that will be rewritten to
map-reduce. The simplest example is a for-loop over a `read` expression. Jaql translates such a query into the following map-reduce expression:

```
  $q = for( $i in read('myHDFSFile', 'example.jql') )
         [ {key: $i.publisher, ($i.title): $i.year} ];
  
  hbaseWrite('mytable', $q);
  
  // translates to:
  mapReduce({
    input:  {type: 'myHDFSFile', location: 'example.jql'},
    output: {type: 'hbase'     , location: 'mytable'},
    map:    fn($i) [ [null, {$i.key, $i.abc, $i.xyz}] ] 
  });
```

In this example, an HBase table is loaded in parallel with a projection from a JSON text formatted file. The `mapReduce()` function specifies its input and output using records. Each record specifies the data store type, a location, and possibly additional options.

## Example: Delimited File Formats ##

The previous examples showed how JSON binary data and JSON text data is processed using
JAQL and Hadoop's Input/Output format interfaces. However, JSON-formatted data is not required for JAQL. In this example, we show how a delimitted file can be processed using JAQL. As in the previous examples, an appropriate [InputFormat](InputFormat.md) is needed along with a converter that transforms an [InputFormat](InputFormat.md) record into JSON. Lets start with the following data that is stored in a file named `delimited.dat`:

```
1,2,3
foo,bar,baz
a longer example of a string,followed by another followed by a number,42
```

First the file is loaded into HDFS:

```
hdfsShell("-copyFromLocal src/test/com/ibm/jaql/delimited.dat delimited.dat");
```

Next, an appropriate reader is defined by parameterizing `hdfsRead`:

```
$dRead = fn($data) 
           hdfsRead($data, {format: "org.apache.hadoop.mapred.TextInputFormat", 
                            converter: "com.acme.extensions.data.FromDelimitConverter"});
```

The format is a `TextInputFormat`, as we've seen before and its converter is
`FromDelimitConverter` whose core method is as follows:

```
public class FromDelimitConverter extends HadoopRecordToItem {
...
@Override
  protected WritableToItem createValConverter()
  {
    return new WritableToItem() {
      public void convert(Writable src, Item tgt)
      {
        if (src == null || tgt == null) return;
        Text t = null;
        if (src instanceof Text)
        {
          t = (Text) src;
        }
        else
        {
          throw new RuntimeException("tried to convert from: " + src);
        }
        
        String[] vals = new String(t.getBytes()).split(delimitter);
        try {
          if(header == null) {
1.          setArray(vals, (FixedJArray)tgt.get());
          } else {
2.          setRecord(vals, header, (MemoryJRecord)tgt.get());
          }
        } catch(Exception e) {
          throw new RuntimeException(e);
        }
      }
      
      private void setRecord(String[] vals, JArray names, MemoryJRecord tgt) throws Exception
      {
        int n = (int) names.count();
        if(n != vals.length) 
          throw new RuntimeException("values and header disagree in length: " + vals.length + "," + n);
        
        for(int i = 0; i < n; i++) {
          JString name = (JString) names.nth(i).get();
          ((JString)tgt.getRequired(name.toString()).getNonNull()).set(vals[i]);
        }
      }
      
      private void setArray(String[] vals, FixedJArray tgt) {
        tgt.clear();
        int n = vals.length;
        for(int i = 0; i < n; i++) {
          tgt.add(new JString(vals[i])); // FIXME: memory
        }
      }
      
      public Item createTarget()
      {
        if(header == null)
          return new Item(new FixedJArray());
        else {
          int n = (int)header.count();
          MemoryJRecord r = new MemoryJRecord(n);
          try {
            for(int i = 0; i < n; i++) {
              r.add( (JString)header.nth(i).getNonNull(), new JString());
            }
          } catch(Exception e) { throw new RuntimeException(e);}

          return new Item(r);
        }
      }
    };
...
```

`FromDelimitConverter` actually does a bit more than convert a delimited file
into JSON. It also permits an optional header file to be specified that is used
to name the fields present in a data file. Like any other read expression, `dRead`
returns a JSON array where each array element corresponds to a line from the input file.
If no header is specified, then each line is converted into a JSON array (line 1), possibly varying in arity from one line to the other. If a header is specified, then each line is converted into a JSON record whose names are specified in the header file (line 2). `dRead` can be used to read `delimited.dat` as follows:

```
jaql> $dRead("delimited.dat");
[
  [
    "1", "2", "3"
  ],
  [
    "foo", "bar", "baz"
  ],
  [
    "a longer example of a string","followed by another followed by a number","42"
  ]
]
```

To produce an array of records, the record field names can be specified using an
array of names. When the `FromDelimitConverter` is parameterized by such an array,
it produces records instead of arrays. The following, slightly different read expression, can be used:

```
$dhRead = fn($data, $header)
            hdfsRead($data, {format: "org.apache.hadoop.mapred.TextInputFormat",		                     converter: "com.acme.extensions.data.FromDelimitConverter",
			     header: $header});
```

Using `dhRead`, the data file, `delimit.dat`, can be read as followed:

```
$dhRead("delimited.dat", ["a", "b", "c"]);
[
  {
    "a": "1", "b": "2", "c": "3"
  },
  {
    "a": "foo", "b": "bar", "c": "baz"
  },
  {
    "a": "a longer example of a string", 
    "b": "followed by another followed by a number", 
    "c": "42"
  }
]
```

Alternatively, the schema could have been stored in a header file, `delimited.hdr`:

```
a,b,c
```

and loaded into HDFS:

```
hdfsShell("-copyFromLocal build/test/com/ibm/jaql/delimited.hdr delimited.hdr");
```

Note that `dRead` can be used to convert the header into an array:

```
jaql> $dRead("delimited.hdr");
[
  [ "a", "b", "c" ]
]
```

This in turn can be used to read `delimit.dat` using `delimit.hdr`:

```
$dhRead("delimited.dat", $dRead("delimited.hdr")[0]);##
[
  {
    "a": "1", "b": "2", "c": "3"
  },
  {
    "a": "foo", "b": "bar", "c": "baz"
  },
  {
    "a": "a longer example of a string",
    "b": "followed by another followed by a number",
    "c": "42"
  }
]
```

# Stream IO #

The Hadoop-based IO is useful when processing large data sets.
However, we expect that reading from an [InputStream](http://java.sun.com/j2se/1.5.0/docs/api/java/io/InputStream.html)
or writing to an [OutputStream](http://java.sun.com/j2se/1.5.0/docs/api/java/io/OutputStream.html)
will also be needed when manipulating small data sets.
For this purpose, we provide an additional type of adapter: StreamAdapters.
StreamAdapters open an input or output stream given a URI.
For example, `localRead, localWrite` and `httpGet` expressions
are based on StreamAdapters. Just as Hadoop adapters allow for conversions between `Writables` and `Items`, Stream adapters also provide for converting between bytes and `Items`.

For example, consider accessing a local file that is formatted as JSON text. The only
class to implement is a converter that can borrow from the previous example:

```
  public class FromJSONTxtConverter implements StreamToItem {
    ...
    public void setInputStream(InputStream in) {
      // set the input stream
    }
    
    public boolean read(Item v) throws IOException {
      // parse the input stream to get the next v
    }
    ...
  }
```

The new data source is registered and tested as follows:

```
  localRead('books.jql', {format: 'com.acme.extensions.data.FromJSONTxtConverter'});
```

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
  $freebase = 
      httpGet('http://www.freebase.com/api/service/mqlread', 
        { queries: 
            serialize(
              { myquery: 
                { query:
                  [{ type: "/music/artist",
                     name: $artist,
                     album: []
                   }] 
                }
              }
            ) })[0];
  
  $freebase.myquery.result[**].album;
      
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
        { appid:  $appid,
          street: "701 First Street",
          city:   "Sunnyvale",
          state:  "CA",
          output: "json"
        })[0];

  $trafficData.ResultSet.Result[*].Title;
  
  // result...
  [ "Road construction, on US-101 NB at FAIROAKS AVE TONBTO NB MATHILDA",
    "Road construction, on CA-85 SB at MOFFETT BLVD",
    "Road construction, on CA-237 EB at MATHILDA AVE TOEBTO FAIR OAKS AVE",
    "Road construction, on CA-237 WB at CROSSMAN AVE",
    "Road construction, on I-880 at GATEWAY BLVD"];
```

# Extending IO #

Adapters can be extended in order to access data that are not suitable for
Hadoop and Stream adapters. An example is access to relational databases, e.g., through a JDBC driver.
The following lists the Adapter interface:

```
  public interface StorableInputAdapter {
    
    protected void initializeFrom(Item args);
    
    public void open() throws IOException;
    
    public abstract ItemReader getItemReader() throws IOException;
    
    public void close() throws IOException;
  }
```

The `initializeFrom` method is used to bind-in arguments that are passed in from the expression
(e.g., `'path'` from `myRead('path')`).
The `open` sets up access to a data store whereas `close` releases resources.
Finally, the `Iter` consumes data from the data source and produces `Items` as input to Jaql.