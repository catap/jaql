## Text-based Input/Output Format ##

Suppose that the HDFS file is a text file (i.e, new-line delimitted records). The [InputFormat](http://hadoop.apache.org/core/docs/r0.18.3/api/org/apache/hadoop/mapred/InputFormat.html) to use in this case is `TextInputFormat`. For this case, Jaql's default can be overriden as follows:

```
  read('hdfs', 'books.jql', {format: 'org.apache.hadoop.mapred.TextInputFormat'});
```

The additional argument to `read` is a record that specifies which class
will implement the [InputFormat](http://hadoop.apache.org/core/docs/r0.18.3/api/org/apache/hadoop/mapred/InputFormat.html). More generally, this record can specify any options that
are specific to a give data store type. In the example above, a `TextFile`'s
records are `Text`, not Jaql `Items` (i.e., binary JSON) so are converted
by implementing a converter and specifying the converter as an option `read`.
An example converter is implemented as follows:

```
  public class FromJSONTxtConverter extends HadoopRecordToItem
  {

  @Override
  protected FromItem<WritableComparable> createKeyConverter()
  {
    return null;
  }

  @Override
  protected FromItem<Writable> createValConverter()
  {
    return new FromItem<Writable>() {
      JsonParser parser = new JsonParser();
      
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
        
        try
        {
          parser.ReInit(new StringReader(t.toString()));
          Item data = parser.JsonVal();
          tgt.set(data.get());
        }
        catch (ParseException pe)
        {
          throw new RuntimeException(pe);
        }
      }

      public Item createTarget()
      {
        return new Item();
      }

    };
  }
}
```

The `FromJSONTxtConverter` takes as input a `Writable` value and sets the
`Item` to the parsed result. The following shows how to use it in `read`:

```
  read('hdfs', 'books.jql', {format    : 'org.apache.hadoop.mapred.TextInputFormat',
                             converter : 'com.ibm.jaql.io.hadoop.converter.FromJSONTxtConverter'});
```

The `read` expression uses `TextInputFormat` to read the file
specified at 'path' in HDFS. For each record retrieved from the file, it will use
`FromJSONTxtConverter` to convert each `Text Writable` to an `Item`.

## Using Custom File Formats ##

While extensible, the `read` expression is cumbersome to specify in this manner.
There are several options to hide the details. The simplest is to define a function:

```
$myRead = fn($path) read({type:'hdfs', location:$path, 
                          inoptions: {format    : 'org.apache.hadoop.mapred.TextInputFormat', 
                                      converter : 'com.ibm.jaql.io.hadoop.converter.FromJSONTxtConverter'}});
  
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
                             converter    : 'com.ibm.jaql.io.hadoop.converter.FromJSONTxtConverter',
                             configurator : 'com.ibm.jaql.io.hadoop.FileInputConfigurator'}});
```

The Hadoop adapter and configurator are specified, along with the `TextInputFormat` and custom converter. The newly registered data store is used as follows:

```
  read({type:'myHDFSFile', location:'example.jql'});
```

If a new data store type is very common, it may be convenient to
define a Jaql function that hides some of the details:

```
  $readMyFile = fn($name) read({type: 'myHDFSFile', location: $name});

  $readMyFile('books.jql');
```

In addition to HDFS files, Jaql supports HBase as a data source. This is supported by
the same Hadoop adapter, but parameterized by Input(Output)Formats and configurators that are specific to HBase.
For HBase, the 'path' represents a table name. Columns and column families can be specified as additional options.

Finally, in order to do something more interesting with these examples,
consider a query that will be rewritten to
map-reduce. The simplest example is a for-loop over a `read` expression. Jaql translates such a query into the following map-reduce expression:

```
  read({type:'myHDFSFile', location:'example.jql'})
    -> transform {key: $.publisher, ($.title): $.year}
    -> hbaseWrite('example');
  
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
$dhRead = fn($data,$header) 
           read({type:"hdfs", location:$data,
                 inoptions:{format: "org.apache.hadoop.mapred.TextInputFormat", 
                            converter: "com.acme.extensions.data.FromDelimitConverter",
                            header?: $header}});

$dRead = fn($data) $dhRead($data, null);
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