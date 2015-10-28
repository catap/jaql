# Hadoop IO #

In Hadoop's [map-reduce](http://hadoop.apache.org/core/docs/r0.18.3/mapred_tutorial.html), data is accessed using [InputFormat](http://hadoop.apache.org/core/docs/r0.18.3/api/org/apache/hadoop/mapred/InputFormat.html) and [OutputFormat](http://hadoop.apache.org/core/docs/r0.18.3/api/org/apache/hadoop/mapred/OutputFormat.html). Classes that implement these interfaces provide enough information to map-reduce so that the data can be partitioned and processed in parallel.


Jaql's I/O framework supports any Input(Output)Format to be plugged-in.
However, Input(Output)Formats work with `Writables` while Jaql expects `Items`.
Thus, the framework makes it easy to convert `Writables` to and from `Items`.

## Default Input/Output Format ##

In order to make the discussion more concrete, lets look under the hood of the
`read(hdfs('books.jqlb'))` expression.
First, `hdfs` is an expression that constructs a file-handle that is
input to `read`. The `hdfs` expression will evaluate to produce the
following expression:

```
  read({type: 'hdfs', location: 'books.jqlb'});
```

The first argument to the `read` expression is the name
associated with a type of data store.  Just as names are associated to
function implementations in the function registry, names are
associated to data store types in the storage registry. The second
argument to `read` is the path of a file stored in HDFS.

For the hdfs data store type, the registry entry specifies default Input(Output)Formats.
The defaults for Jaql are `SequenceFileInputFormat` and `SequenceFileOutputFormat`.

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
  read({type:"local", location:'build/test/cache/books.json', inoptions:{format : 'com.ibm.jaql.io.stream.converter.JSONTextInputStream'}});
```