
# system #
## externalfn() ##

> _**Description**_ An expression that constructs a JSON value for a Java UDF

> _**Parameters**_ (1 inputs)
> Input Types: `( arg0, required: schema any)`

> _**Output**_ `schema any`

## ls() ##

> _**Description**_ This function returns an array of file objects that match a user provided glob / path filter.
> Usage :

> [{ <file status fields> } ](.md) ls(string glob);

> Input Parameters: glob or a path / file pattern string. The path pattern is absolute if it begins with a slash.
> Output: An array of file status records that adhere to the following schema
> {
> > "accessTime": Date,
> > "blockSize": Long,
> > "group": String,
> > "length": Long,
> > "modifyTime": Date,
> > "owner": String,
> > "path": String,
> > "permission": String,
> > "replication": Long

> }

> In standalone mode, the path pattern is applied to the local filesystem.
> In distributed modes, ls applies the path pattern per default to the HDFS file system.
> To apply this function on the local file system in distributed mode, the path pattern has to be prefixed with 'file:///'

> _**Parameters**_ (1 inputs)
> Input Types: `( arg0, required: schema any)`

> _**Output**_ `schema any`

> _**Examples**_
```
jaql> ls('.'); // This returns status information about the current directory 
              [
                {
                  "accessTime": null,
                  "blockSize": 0,
                  "group": "supergroup",
                  "length": 0,
                  "modifyTime": date('2011-01-13T02:33:18.857Z'),
                  "owner": "user01",
                  "path": "hdfs://localhost:54310/user/user01",
                  "permission": "rwxr-xr-x",
                  "replication": 0
                }
              ]

jaql> // This returns all files in the current directory with a file extension of 'java'. It also filters for a owner uid of 'user01' and projects onto the filename and owner fields.
              [
                {
                  "owner": "user01",
                  "path": "hdfs://localhost:54310/user/user01/test.java"
                }
              ]

jaql> count(ls('*')); // This counts all file objects in the current directory
               15

jaql> ls('*'); // This returns all files in the current hdfs directory.
              [
                {
                  "accessTime": date('2011-01-12T20:15:50.502Z'),
                  "blockSize": 67108864,
                  "group": "supergroup",
                  "length": 834,
                  "modifyTime": date('2011-01-13T02:33:18.857Z'),
                  "owner": "user01",
                  "path": "hdfs://localhost:54310/user/user01/books",
                  "permission": "rw-r--r--",
                  "replication": 1
                }
                {...
                }
              ]

jaql> ls('file:///*'); // This returns all java files in the local file system's root directory.
              [
                {
                  "accessTime": null,
                  "blockSize": 33554432,
                  "group": "root",
                  "length": 4096,
                  "modifyTime": date('2010-03-16T21:16:47.000Z'),
                  "owner": "root",
                  "path": "file:/.cache",
                  "permission": "rwxr-xr-x",
                  "replication": 1
                }
                {...
                }
              ]

```

---

# core #
## catch() ##

> _**Description**_ Wrap any expression with catch to guard against exceptions.
> Usage:

> T1|null catch( T1 e1, { errThresh: long } | null, T2 e2);

> Wrap the catch expression around the first argument, e1, that needs to be guarded
> for exceptions.

> The second argument is optional. It specifies an exception handling policy. If unspecified or null, the default
> exception handling policy is used. By default, if an exception occurs, it is propagated
> (which typically results in aborted execution). This default can be overridden globally
> using the registerExceptionHandler function, or at can be overridden per usage of catch
> by using the second argument. Such an override allows catch to throw an exception errThresh
> times before propagating the exception. Thus, the default has errThresh set to 0.

> The third argument, e2, is optional and is used to specify an expression whose value is logged when an exception
> is thrown.

> Catch returns the result of evaluating e1 (whose type is T1). If an exception is thrown, but
> skipped, then null is returned.

> Note that catch s a "blocking" call: the result of e1 will be materialized. If e1 could
> be streamed (e.g., read(...)), when used in the context of catch, its result will be entirely
> materialized.

> _**Parameters**_ (1 - 3 inputs)
> Input Types: `( arg0, required: schema any),( arg1 = null: schema any),( arg2 = null: schema any)`

> _**Output**_ `schema any`

> _**Examples**_
```
jaql> data = [ ["a",0], ["a",1], ["b",0], ["c",0], ["c",1], ["c",2]];

jaql> data -> write(hdfs("test"));

jaql> read(hdfs("test"))
      -> transform catch(if ($[0] == "a") ($.badFieldAccess) // cause exceptions on ["a", 0] and ["a", 1]
                         else ($), 
                         { errThresh: 1000 }) 
      -> group by g = $[0] 
         into { g: g, 
                num: count($), 
                err: catch(if(g == "b") (g.badFieldAccess) // cause exception/null on the "b" group
                           else ("ok"), 
                           { errThresh: 1000 }) 
              }; 
[
 {
   "err": "ok",
   "g": null,
   "num": 2
  },
  {
   "err": null,
   "g": "b",
   "num": 1
  },
  {
   "err": "ok",
   "g": "c",
   "num": 3
  }
]

```
## compare() ##

> _**Description**_ This function compares two JSON values with each other

> Usage :

> long compare(T1 val1, T1 val2);

> Input Parameters: Two JSON values of the same type

> Output: Returns -1, 0, or 1 as val1 is less than, equal to, or greater than val2.

> _**Parameters**_ (2 inputs)
> Input Types: `( x, required: schema any),( y, required: schema any)`

> _**Output**_ `schema long`

> _**Examples**_
```
jaql> compare(1,2);    // Compares two integers
              -1

jaql> data1 = [ ["a",0], ["b",1]];

jaql> data2 = [ ["a",0], ["b",1]];

jaql> compare(data, data2); // Compares the two data objects.
              0

```
## getHdfsPath() ##

> _**Description**_ This function returns the absolute path of a file or directory in HDFS

> Usage :

> string getHdfsPath(string);

> Input Parameters: The input string is either a fileName, or a directory name residing in HDFS. Absolute paths have to be prefixed with a slash.

> Output: A string value containing the absolute path name of the file system object in HDFS context.

> _**Parameters**_ (1 inputs)
> Input Types: `( arg0, required: schema any)`

> _**Output**_ `schema any`

> _**Examples**_
```
jaql> getHdfsPath("books");
                "hdfs://localhost:54310/user/user01/books"

jaql> getHdfsPath("/user/user01");
                "hdfs://localhost:54310/user/user01"

```
## getOptions() ##

> _**Description**_ Return Jaql's options as a record
> Usage:

> { **: any,**} getOptions();

> Jaql maintains globally accessible options, e.g., name-value pairs.
> These options are represented as a record; the getOptions function
> returns these options. Note that if you set the field "conf" with
> a record, those options are overlaid onto Hadoop's JobConf when a
> MapReduce job is run. Using getOptions and setOptions, one can
> override settings in the default JobConf.

> _**Parameters**_ (0 inputs)
> Input Types: {{{}}}

> _**Output**_ `schema any`

> _**Examples**_
```
jaql> getOptions();
 {
   "conf": {
     "io.sort.factor": 20
   }
 }

```
## index() ##

> _**Description**_ index(array, index) returns the value at the position index in the array
> > passed in. It is equivalent to `array[index]`, but it captures a simpler case
> > that does not use path expressions. `array[index]` is transformed to use the
> > index() function for better performance.


> Note: index() is zero-based.

> Usage:
> any index( array, long )

> _**Parameters**_ (2 inputs)
> Input Types: `( arg0, required: schema any),( arg1, required: schema any)`

> _**Output**_ `schema any`

> _**Examples**_
```
jaql> a = [ 1, 2, 3 ];
              index( a, 1 )   // same as a[ 1 ]; result: 2

```
## listVariables() ##

> _**Description**_ This function lists all global variables that are in scope.

> Usage :

> [{ <variable fields> }] listVariables()

> Output: Returns an array of JSON records, each representing a global variable.
> > Every JSON record adheres to he following schema
> > {
> > > "var":     string,
> > > "schema":  schema,
> > > "isTable": bool,
> > > "package": string,
> > > "module":  string,
> > > "alias":   string

> > }


> var is the name of a variable
> schema is the schema of the variable
> isTable is true if the schema is an array of records
> package is the package name that declared the variable, or "" for the top script
> module is the module name that declared the variable, or "" for the top script
> alias is the module alias used to refer to the variable in the current context, or ""

> _**Parameters**_ (0 inputs)
> Input Types: {{{}}}

> _**Output**_ {{{schema [
> > {
> > > "var": string,
> > > "schema": schematype,
> > > "isTable": boolean

> > } **> > ]}}}**


> _**Examples**_
```
jaql> listVariables()->filter $.var == "T"; // List all variables and filter for a variable called 'T'
 [ 
   {
     "var": "T",
     "schema": schema [{ id: long, name: string }...] ,
     "isTable": false
       "package": "",
     "module":  "",
     "alias":   ""
   }
 ]

```
## perPartition() ##

> _**Description**_ perPartition() declares that a function f can be evaluated in parallel using an arbitrary
> > partitioning of the input array in.


> Usage:
> array perPartition( array in, array fn( array ) f )

> perPartition() returns the array obtained by applying f to in.

> Note: This function is declared experimental. perPartiton() states
> that the partitioning of the input array does not influence the result of f.
> Since f is can be an arbitrary function, this statement may or may not be correct.
> If f is sensitive to the partitioning of in, the result of this function
> is depending on the evaluation strategy of the query and may be arbitrary.

> _**Parameters**_ (2 inputs)
> Input Types: `( arg0, required: schema any),( arg1, required: schema any)`

> _**Output**_ `schema any`

> _**Examples**_
```
jaql> [1, 2, 3] -> perPartition( -> javaUDF() )
                    // declares javaUDF as perPartition() - useful because we cannot naturally infer
                    // the perPartition property from javaUDF()

jaql> [1, 2, 3] -> perPartition( -> filter $ != 2 -> transform $ * 5 )
                    // correct but superfluous as filter and transform are truly perPartition() but inferred by
                    // the JAQL compiler

jaql> [1, 2, 3] -> perPartition( -> group into count( $ ) ) ;
                    // yields wrong results in parallel environments as the count() is sensitive as to how the
                    // input array is partitioned. Here are the possible results:
                    //
                    // [ 3 ]
                    // [ 2, 1 ]
                    // [ 1, 2 ]
                    // [ 1, 1, 1 ]
                    //

```
## range() ##

> _**Description**_ Range generates a continuous array of numbers

> Usage:
> range(size)      = [0,size-1]
> range(size,null) = [0,size-1]
> range(start,end) = [start,end]
> range(start,end,skip) = if skip > 0 then for(i = start, i <= end, i += skip)
> > else error

> range(size,null,skip) = if skip > 0 then for(i = 0, i < size, i += skip)
> > else error


> _**Parameters**_ (1 - 3 inputs)
> Input Types: `( startOrSize, required: schema long?),( end = null: schema long?),( by = 1: schema long)`

> _**Output**_ {{{schema [
> > long **> > ]}}}**

## registerExceptionHandler() ##


> _**Description**_ Register a default exception handling policy.
> Usage:

> bool registerExceptionHandler( { errThresh: long } );

> This function allows the default exception handling policy to be overridden.
> Currently, the policy can specify how many exceptions to skip before propagating
> the exception up the call stack. This is specified by the errThresh field of the
> input. By default, errThresh is set to 0, meaning that no exceptions are skipped.

> When an exception is skipped, the enclosing expression decides what to do. If the
> exception occurs in the catch function, then it returns null and logs the results of
> a user supplied expression. If the exception occurs in a transform, then the result is
> skipped and logged.

> _**Parameters**_ (1 inputs)
> Input Types: `( arg0, required: schema any)`

> _**Output**_ `schema any`

> _**Examples**_
```
jaql> registerExceptionHandler({errThresh: 5});

jaql> data = [ ["a",0], ["a",1], ["b",0], ["c",0], ["c",1], ["c",2]];

jaql> data -> write(hdfs("test"));

jaql> read(hdfs("test")) -> filter $[1] == 0 -> transform $.badTypeAssumption;
 []

```
## setOptions() ##

> _**Description**_ Set Jaql's options as a record
> Usage:

> bool setOptions( {**: any,**} );

> Jaql maintains globally accessible options, e.g., name-value pairs.
> These options are represented as a record; the setOptions function
> modified these options. Note that if you set the field "conf" with
> a record, those options are overlaid onto Hadoop's JobConf when a
> MapReduce job is run. Using getOptions and setOptions, one can
> override settings in the default JobConf.

> _**Parameters**_ (1 inputs)
> Input Types: `( arg0, required: schema any)`

> _**Output**_ `schema any`

> _**Examples**_
```
jaql> setOptions( { conf: { "io.sort.factor": 20  }} );

```
## tee() ##

> _**Description**_ tee() streams an array into each specified function. It returns its input array. tee() can
> > be thought of as analogous to the tee command in unix which is used to replicate data
> > streams.


> Usage:
> array tee( array in, any fn( array ) f1, ..., any fn( array ) fN )

> tee() applies the functions f1 through fN to in. It returns in.
> tee() can be called with no functions in which case tee() is effectively a no-operation
> on the array in.

> The functions passed to tee() usually perform a side-effect
> as the result of the function f1 through fN is discarded. A typical use of tee() would be
> to write the input array to different output files.

> Callers to function should not assume a particular order of evaluation of f1 through fN.

> _**Parameters**_ (1 - 2 inputs)
> Input Types: `( arg0, required: schema any),( arg1 = null: schema any)...`

> _**Output**_ `schema any`

> _**Examples**_
```
jaql> [ 1, 2, 3 ] -> tee( -> localWrite( del( '/tmp/test1.csv' ) ) ); // write [1,2,3] to a file and return it

jaql> [ 1, 2, 3 ] -> tee( -> localWrite( del( '/tmp/test1.csv' ) ),
                                      -> localWrite( del( '/tmp/test2.csv' ) ) ); // write [1,2,3] to two files and return it

jaql> [ 1, 2, 3 ]
                   -> tee( -> localWrite( del( '/tmp/test1.csv' ) ) ) 
                   -> localWrite( del( '/tmp/test2.csv' ) ); // write [1,2,3] to two files and return the result of localWrite()

jaql> [ 1, 2, 3 ] -> tee( -> filter $ != 2
                                         -> transform $ * 5
                                         -> localWrite( del( '/tmp/test1.csv' ) ), // write [5,15] to test1.csv
                                      -> localWrite( del( '/tmp/test2.csv' ) ) );  // write [1,2,3] to test2.csv and return it

```
## timeout() ##

> _**Description**_ Wrap any expression to limit the amount of time it will run.
> Usage:

> T timeout(T e, long millis);

> Given an arbitrary expression e (of type T), all it to be evaluated
> for now more than millis ms. If e completes in less than millis time,
> then its value is returned. Otherwise, an exception is thrown.

> _**Parameters**_ (1 - 2 inputs)
> Input Types: `( arg0, required: schema any),( arg1 = null: schema any)`

> _**Output**_ `schema any`

> _**Examples**_
```
jaql> sleep = javaudf("com.ibm.jaql.fail.fn.SleepFn"); // simple function where we can control its evaluation time

jaql> timeout(sleep(10000), 5000); // this should throw an exception

jaql> timeout(sleep(5000), 10000)); // this should complete successfully in 5 seconds

```

---

# hadoop #
## loadJobConf() ##

> _**Description**_ load a Hadoop JobConf into a record
> Usage:

> { **: string,** } loadJobConf( string? filename )

> If filename to conf is not specified, then the default JobConf is loaded.

> _**Parameters**_ (0 - 1 inputs)
> Input Types: `( arg0 = null: schema any)`

> _**Output**_ `schema any`

> _**Examples**_
```
jaql> loadJobConf( "vendor/hadoop/0.20/conf/mapred-site.xml" );
 {
   "fs.checkpoint.dir": "${hadoop.tmp.dir}/dfs/namesecondary",
   "fs.checkpoint.edits.dir": "${fs.checkpoint.dir}",
   "fs.checkpoint.period": "3600",
   "fs.checkpoint.size": "67108864",
   "fs.default.name": "file:///",
   "fs.file.impl": "org.apache.hadoop.fs.LocalFileSystem",
   ...

```
## mapReduce() ##

> _**Description**_ This function runs a MapReduce job from within JAQL

> Usage :

> mapReduce(JSON record);

> Input: A JSON record that describes the MapReduce job to run. This record adheres to the following schema
> > {
> > > input:  {type: string, location: string},
> > > output: {type: string, location: string},
> > > map:     JAQL function
> > > combine: JAQL function
> > > reduce:  JAQL function
> > > }


> Input and output expect a file-descriptors (fd), more information on this can be found in the I/O wiki at "http://code.google.com/p/jaql/wiki/IO"
> Note that input and output can also be an array of file descriptors, which is needed for other functions like co-group, union, and tee, for example.
> Also note that the output is also a file descriptor, i.e. result of a map-reduce invocation can be read.

> The map function takes as input an array and produces an array of pairs of the grouping key and value.
> The combiner function takes as input the output of the map function and performs partial aggregation on the map side before sending it to the reduce function.
> The reduce function processes the output of the map function or combiner function and operates on the grouping key and an array of values.

> Output: A JSON record describing the type and location of the reduce output, i.e. file descriptor
> > {
> > > type:     String,
> > > location: String

> > }


> _**Parameters**_ (1 inputs)
> Input Types: `( arg0, required: schema any)`

> _**Output**_ `schema any`

> _**Examples**_
```
jaql> [
    {x: 0, text: 'zero'},
    {x: 1, text: 'one'},
    {x: 0, text: 'two'},
    {x: 1, text: 'three'},
    {x: 0, text: 'four'},
    {x: 1, text: 'five'},
    {x: 0, text: 'six'},
    {x: 1, text: 'seven'},
    {x: 0, text: 'eight'}
  ] -> write(hdfs('sample.dat')); 
 
 // MapReduce job that groups by the value of variable x, and counts the number of objects within each group.

jaql> mapReduce( 
              { input:  {type: 'hdfs', location: 'sample.dat'}, 
                output: {type: 'hdfs', location: 'results.dat'}, 
                map:    fn(v) ( v -> transform [$.x, 1] ),
                reduce: fn(x, v) ( v -> aggregate into [{x: x, num: count($)}] )
              });
              {
                "type": "hdfs",
                "location": "results.dat"
                }

jaql> read(hdfs('results.dat'));
              [
                {
                  "num": 5,
                  "x": 0
                },
                {
                  "num": 4,
                  "x": 1
                }
              ]

```
## mrAggregate() ##

> _**Description**_ This function runs a MapReduce job from within JAQL and allows for running multiple algebraic aggregates in one pass

> Usage :

> mrAggregate(JSON record);

> Input: A JSON record that describes the MapReduce job to run. This record adheres to the following schema
> > {
> > > input:  {type: string, location: string},
> > > output: {type: string, location: string},
> > > map:       JAQL function
> > > aggregate: JAQL function
> > > final:     JAQL function
> > > }


> Input and output expect a file-descriptors (fd), more information on this can be found in the I/O wiki at "http://code.google.com/p/jaql/wiki/IO"
> Note that input and output can also be an array of file descriptors, which is needed for other functions like co-group, union, and tee, for example.
> Also note that the output is also a file descriptor, i.e. result of a map-reduce invocation can be read.

> This function is evaluated running map/reduce. It allows a user however to specify an array of partial aggregates in the aggregate parameter.
> These algebraic aggregates (have to be commutative and associative combiner functions) are then evaluated without making multiple passes over the
> group as it would be necessary with the mapReduce function.
> More information can be found in the JAQL wiki at "http://code.google.com/p/jaql/wiki/Functions"

> Output: A JSON record describing the type and location of the reduce output, i.e. file descriptor
> > {
> > > type:     String,
> > > location: String

> > }


> // Generate some data

> _**Parameters**_ (1 inputs)
> Input Types: `( arg0, required: schema any)`

> _**Output**_ `schema any`

> _**Examples**_
```
jaql> range(1,10) -> expand each i ( range(1,100) -> transform each j { a: i, b: i * j } )
              -> write(hdfs('nums'));
              
 // Run a MapReduce job that run multiple partial aggregates in a single pass

jaql> mrAggregate( {
                input: { type: "hdfs", location: "nums" }, 
                output: HadoopTemp(),
                map: fn ($) ( $ -> transform [ $.a, $ ] ),
                aggregate: fn (k, $) [ combine($[*].b, fn(a,b) gcd2( a,b ) ), sum($[*].b), count($) ],
                final: fn (k, aggs) [{ a:k, g: aggs[0], s:aggs[1], c:aggs[2] }]
              } )
              -> read()
              -> sort by [$.a];
              
              [
                {
                  "a": 1,
                  "c": 10,
                  "g": 1,
                  "s": 55
                },
                {..}
              ]

```
## nativeMR() ##

> _**Description**_ launch a natively specified MapReduce job
> Usage:
> { status: boolean } nativeMR( { job conf } conf , { apiVersion: "0.0" | "1.0", useSessionJar: boolean } options );

> Launch a stand-alone map-reduce job that is exclusively described by job conf settings.
> The conf can be obtained using loadJobConf or it can be specified using a record literal
> that lists the needed name/value pairs for the job. If apiVersion is set to "0.0", then
> the old Hadoop MapReduce API is used. Otherwise, the new API is used.

> The useSessionJar is convenient for those native MapReduce jobs that use jaql libraries.
> Since the jaql client already packages up jars when submitting jobs to Hadoop's MapReduce,
> the useSessionJar is used to specify that the job's jar should use the client's currently
> packaged jar.

> _**Parameters**_ (1 - 2 inputs)
> Input Types: `( arg0, required: schema any),( arg1 = null: schema any)`

> _**Output**_ `schema any`

> _**Examples**_
```
jaql> nativeMR( loadJobConf( "myJob.conf" ) );
 { status: true }

```
## readConf() ##

> _**Description**_ read a value from jaql's current Hadoop JobConf
> Usage:

> string readConf(string name, string? dflt);

> Jaql stores the JobConf that is associated with the current
> map-reduce job. This function reads name from this JobConf and
> returns its value, otherwise it returns the dflt value.

> _**Parameters**_ (1 - 2 inputs)
> Input Types: `( arg0, required: schema any),( arg1 = null: schema any)`

> _**Output**_ `schema any`

> _**Examples**_
```
jaql> readConf( "mapred.reduce.tasks" );
 "1"

```

---

# io #
## hdfsShell() ##

> _**Description**_ This expression allows for running HDFS shell commands. This is equivalent to executing 'hadoop fs'

> Usage :

> long hdfsShell(string);

> Input: A HDFS file system command, that is supported by hadoop's FsShell.

> Output: The command output.
> > Return value -1 --> command failed.
> > Return value  0 --> command successfully executed.


> _**Parameters**_ (1 inputs)
> Input Types: `( arg0, required: schema any)`

> _**Output**_ `schema any`

> _**Examples**_
```
jaql> hdfsShell('-copyFromLocal /home/user01/books books'); // Copies a file from the local file system to the HDFS file system.
              0

jaql> hdfsShell('-ls *.java');  // Lists all files in the current HDFS directory
              -rw-r--r--   1 user01 supergroup       1485 2011-01-12 21:19 /user/user01/test.java
              0

```

---

# array #
## arrayToRecord() ##

> _**Description**_ This function generates a JSON Record by merging two input arrays. The current sort order is maintained for the merging process.

> Usage :

> [T1](T1.md) arrayToRecord(JSON array1, JSON array2);

> Input Parameters: Two JSON arrays, where the first array contains the list of names, the second array contains the respective list of values
> > If count(array1) > count(array2) --> A null value is used to fill up all names that do not have an associated value
> > If count(array1) > count(array2) --> Error


> Output: Returns a JSON record that contains all merged name value pairs from array1 and array2

> _**Parameters**_ (2 inputs)
> Input Types: `( arg0, required: schema any),( arg1, required: schema any)`

> _**Output**_ `schema any`

> _**Examples**_
```
jaql> arrayToRecord(["DEF", "ABC"], [123,345]); // Merging two arrays of equal size. The sort order is maintained for each array during the merge.
              {
                "ABC": 345,
                "DEF": 123
              }

jaql> arrayToRecord(["DEF", "ABC"], [123]); // More names than values, a null-value is used for the name slack.
              {
                "ABC": null,
                "DEF": 123
              }

jaql> arrayToRecord(range(1,5)->transform(strcat("s",$)), range(1,5)); // Combine function with other data generation functions.
              {
                "s1": 1,
                "s2": 2,
                "s3": 3,
                "s4": 4,
                "s5": 5
              }

```
## deempty() ##

> _**Description**_ This function removes empty sub-objects and null values from an JSON array.

> Usage :

> [T2](T2.md) deempty([T1](T1.md));

> Output: Returns a JSON array without empty sub-objects, e.g. empty records and array.

> _**Parameters**_ (1 inputs)
> Input Types: `( arg0, required: schema any)`

> _**Output**_ `schema any`

> _**Examples**_
```
jaql> [{type:'array',value:[1,2]}, {}, null, []]->deempty();  // Return JSON object without the null / empty elements
              [
                {
                  "type": "array",
                  "value": [
                    1,
                    2
                  ]
                }
              ]

jaql> [1,2,3, null]->deempty();  // Remove null element from array object
              [
                1,
                2,
                3
              ]

```
## distinct() ##

> _**Description**_ List distinct values from an array, remove duplicates.

> Usage:
> [any](any.md) distinct( [any](any.md) )

> _**Parameters**_ (1 inputs)
> Input Types: `( arg0, required: schema any)`

> _**Output**_ `schema any`

> _**Examples**_
```
jaql> distinct( [1, 1d, 1m, 1.5d, 1.5m, 1.50d, 1.50m ] ) -> sort by [$];
 [ 1,1.5 ]

```
## enumerate() ##

> _**Description**_ Take an input an array of any type and returns an array of pairs, one pair per input value.
> Each pair will list the ordinal value of the array value (e.g., its index in the array), along with the value
> of the array.

> Usage:
> [[long, T](.md),**] enumerate( [T, \* ](.md) )**

> _**Parameters**_ (1 - 2 inputs)
> Input Types: `( arg0, required: schema any),( arg1 = null: schema any)`

> _**Output**_ `schema any`

> _**Examples**_
```
jaql> enumerate( ["a", "b", "c"]);
 [ [ 0, "a"] , [1, "b"], [2, "c"] ]

```
## exists() ##

> _**Description**_ Usage : bool exists(any);
> If the argument is null, return null ,
> If the argument is a empty array , return false ,
> If the argument is an array with at least one element, return true ,
> If the argument is not an array or a null, return true.

> _**Parameters**_ (1 inputs)
> Input Types: `( arg0, required: schema any)`

> _**Output**_ `schema any`

> _**Examples**_
```
jaql> exists(null);
 null

jaql> exists([]);
 false

jaql> exists([...]);
 true //when the array has at least one element (even a null)

jaql> exists(...);
 true //when the argument is not an array or a null

```
## lag1() ##

> _**Description**_ This function is deprecated and should not be used.

> lag1(arr)
> > arr is [A ](.md),
> > returns [{prev: A, cur: A} ](.md)


> If arr has k items, the result has k - 1 items.
> result[.md](.md).prev is the first k-1 items
> result[.md](.md).cur  is the last k-1 items.

> eg: [1,2,3] -> lag1()  ==  [{ prev: 1, cur: 2 }, { prev: 2, cur: 3 } ](.md)

> _**Parameters**_ (1 inputs)
> Input Types: `( arg0, required: schema any)`

> _**Output**_ `schema any`

## nextElement() ##

> _**Description**_ Given an array in this function associates the next element with
> > each element of in.


> Usage:
> [{ cur: any, next?: any } ](.md) nextElement( array in )

> Note:  If in has k items, the result has k items. The record returned for the last
> > element does not have a next field.


> _**Parameters**_ (1 inputs)
> Input Types: `( arg0, required: schema any)`

> _**Output**_ `schema any`

> _**Examples**_
```
jaql> [1,2,3] -> nextElement()  
                  [ { cur: 1, next: 2 }, 
                    { cur: 2, next: 3 }, 
                    { cur: 3 } ]

```
## pair() ##

> _**Description**_ Combines two values to an array.

> Usage:
> array pair( any , any );

> The arguments can be any type of date, each of them will be one element of the return array.

> _**Parameters**_ (2 inputs)
> Input Types: `( arg0, required: schema any),( arg1, required: schema any)`

> _**Output**_ `schema any`

> _**Examples**_
```
jaql> pair("element1", "element2");
 [ "element1" , "element2" ]

```
## pairwise() ##

> _**Description**_ Combine two arrays (A,B) into one array C, assume A = [a1,a2,a3 ... ](.md) , B = [b1,b2,b3 ...](.md) , pairwise combines every
> elements in the same position in each array, produces C = [[a1,b1](.md) , [a2,b2] , [a3,c3] ... ].

> Usage:
> array pairwise( array A , array B );

> _**Parameters**_ (2 - 3 inputs)
> Input Types: `( arg0, required: schema any),( arg1, required: schema any),( arg2 = null: schema any)...`

> _**Output**_ `schema any`

> _**Examples**_
```
jaql> pairwise([1,2],[3,4]);
 [
  [1,3],
  [2,4]
 ]

```
## powerset() ##

> _**Description**_ This function returns the power-set of a list of items.

> Usage :

> [[T...](.md)... ]powerset([T...])

> Output: JSON array containing the power-set of the input items

> _**Parameters**_ (1 inputs)
> Input Types: `( arg0, required: schema any)`

> _**Output**_ `schema any`

> _**Examples**_
```
jaql> [1,2,3] -> powerset() ==  [ [], [1], [2], [1,2], [3], [1,3], [2,3], [1,2,3] ]

jaql> ['a', 'b', 'c'] -> powerset() ==  [ [], ['a'], ['b'], ['a','b'], ['c'], ['a','c'], ['b','c'], ['a','b','c'] ]

```
## prevAndNextElement() ##

> _**Description**_ Given an array in this function associates the previous and the next element with
> > each element of in.


> Usage:
> [{ cur: any, prev?: any, next?: any } ](.md) prevAndNextElement( array in )

> Note:  If in has k items, the result has k items. The record returned for the first
> > element does not have a prev field. The record returned for the last element
> > does not have a next field.


> _**Parameters**_ (1 inputs)
> Input Types: `( arg0, required: schema any)`

> _**Output**_ `schema any`

> _**Examples**_
```
jaql> [1,2,3] -> previousAndNextElement()  
                  [ { cur: 1, next: 2 }, 
                    { cur: 2, prev: 1, next: 3 }, 
                    { cur: 3, prev: 2 } ]

```
## prevElement() ##

> _**Description**_ Given an array in this function associates the previous element in the array in with
> > each element of in.


> Usage:
> [{ cur: any, prev?: any } ](.md) prevElement( array in )

> Note:  If in has k items, the result has k items. The record returned for the first
> > element does not have a prev field.


> _**Parameters**_ (1 inputs)
> Input Types: `( arg0, required: schema any)`

> _**Output**_ `schema any`

> _**Examples**_
```
jaql> [1,2,3] -> nextElement()  
                  [ { cur: 1 }, 
                    { cur: 2, prev: 1 }, 
                    { cur: 3, prev: 2 } ]

```
## removeElement() ##

> _**Description**_ Remove element from array in the given position.

> Usage:
> array removeElement( array arr , int position);

> _**Parameters**_ (2 inputs)
> Input Types: `( arg0, required: schema any),( arg1, required: schema any)`

> _**Output**_ `schema any`

> _**Examples**_
```
jaql> removeElement([1,2,3],0);
 [ 2,3 ]

```
## replaceElement() ##

> _**Description**_ Replace an element of the target array with a given value.

> Usage :
> array replaceElement( array arr , int  position, value v );

> _**Parameters**_ (3 inputs)
> Input Types: `( arg0, required: schema any),( arg1, required: schema any),( arg2, required: schema any)`

> _**Output**_ `schema any`

> _**Examples**_
```
jaql> replaceElement([1,2,3],2,100);
 [ 1,2,100 ]

```
## reverse() ##

> _**Description**_ Reverse an array

> Usage:
> array reverse(array arr)

> _**Parameters**_ (1 inputs)
> Input Types: `( arg0, required: schema any)`

> _**Output**_ `schema any`

> _**Examples**_
```
jaql> range(1,10) -> reverse();
  [ 10,9,8,7,6,5,4,3,2,1 ]

jaql> [[0],[1,2],[3,4,5],[6,7,8,9]] -> transform reverse($)->reverse();
 [ [9,8,7,6] , [5,4,3] , [2,1], [0] ] // reverse sequence

```
## shift() ##

> _**Description**_ This function is deprecated. Do not use!

> _**Parameters**_ (3 inputs)
> Input Types: `( arg0, required: schema any),( arg1, required: schema any),( arg2, required: schema any)`

> _**Output**_ `schema any`

## slice() ##

> _**Description**_ slice(array, startIndex, stopIndex) returns elements of array starting at position
> > startIndex up to the position stopIndex. It is equivalent to `array[startIndex:stopIndex]`,
> > but it captures a simpler case that does not use path expressions. `array[startIndex:stopIndex]`
> > is transformed to use the slice() function for better performance.


> Note: slice() is zero-based.

> Usage:
> array slice( array, long, long )

> _**Parameters**_ (3 inputs)
> Input Types: `( arg0, required: schema any),( arg1, required: schema any),( arg2, required: schema any)`

> _**Output**_ `schema any`

> _**Examples**_
```
jaql> a = [ 1, 2, 3, 4 ];
              slice( a, 1, 2 )   // same as a[ 1 : 2 ]; result: [ 2, 3 ]

```
## slidingWindow() ##

> _**Description**_ Given an array input and two predicates start and end, this function associates each element
> > curElem in input with a sub-array of input, called a window, which is computed using the
> > predicates start and end. The window is a "sliding" window since for each curElem the window is
> > computed relative to curElem potentially starting at curElem or any position after curElem and
> > ending at any position at startElem or after startElem. The window is said to be sliding across
> > the input array.


> Usage
```
                   [ { cur: any, window: [ any ] } ] slidingWindow( [any] input,
                                                                    ( boolean function( startElem, cur ) ) start,
                                                                    ( boolean function( cur, endElem ) ) end )
```
> The following invariant always holds for each invocation of start and end for any given curElem
> curElem <= startElem <= endElem.

> In order to find the sliding window of each element curElem = input[curIndex](.md) in input,
> slidingWindow() repeatedly evaluates the predicate start() starting
> from curElem until start() evaluates to true or the end of the array is reached:
```
                   start( input[ curIndex ], input[ curIndex ] )     --> false
                   start( input[ curIndex + 1 ], input[ curIndex ] ) --> false
                   start( input[ curIndex + 2 ], input[ curIndex ] ) --> false
                   ...
                   start( input[ curIndex + n ], input[ curIndex ] ) --> true (or end of array)
```
> Using the index startIndex = curIndex + n, slidingWindow() then tries to find the end of the
> window by evaluating the predicate end() starting from startIndex until not true or the end
> of the array is reached:
```
                   end( input[ startIndex ], input[ startIndex ] )     --> true
                   end( input[ startIndex ], input[ startIndex + 1 ] ) --> true
                   end( input[ startIndex ], input[ startIndex + 1 ] ) --> true
                   ...
                   end( input[ startIndex ], input[ startIndex + k + 1 ] ) --> false (or end of array)
```
> Then the sliding window for curElem is defined as input[startIndex : startIndex + k ](.md).

> Note: slidingWindow() computes its window by value and not by position in the array.
> See slidingWindowBySize() for a positional version of slidingWindows().

> Note: slidingWindow() is optimized as a streaming operation. The memory used by slidingWindow()
> > is proportional to the size of the window, not the input array.


> _**Parameters**_ (3 inputs)
> Input Types: `( input, required: schema [ * ]?),( start, required: schema function),( end, required: schema function)`

> _**Output**_ {{{schema [
> > { **}**
> > ]}}}


> _**Examples**_
```
jaql> // return the next element in array (by value; not index)
                   [1, 2, 3, 4] -> slidingWindow( fn( first, cur ) first == cur + 1, fn(cur,last) last < cur + 2 );
                   
                   [
                     {
                       "cur": 1,
                       "window": [ 2 ]
                     },
                     {
                       "cur": 2,
                       "window": [ 3 ]
                     },
                     {
                       "cur": 3,
                       "window": [ 4 ]
                     },
                     {
                       "cur": 4,
                       "window": []
                     }
                   ];
 
                   // Note that                  
                   [4, 3, 2, 1] -> slidingWindow( fn( first, cur ) first == cur + 1, fn( cur, last ) last < cur + 2 );
                   
                   [ ]; // empty

                   // return every following element after the first element greater than the current element
                   [1, 2, 2, 2, 3, 4] -> slidingWindow( fn( first, cur ) first > cur, fn(cur,last) true );

                   [
                     {
                       "cur": 1,
                       "window": [ 2, 2, 2, 3, 4 ]
                     },
                     {
                       "cur": 2,
                       "window": [ 3, 4 ]
                     },
                     {
                       "cur": 2,
                       "window": [ 3, 4 ]
                     },
                     {
                       "cur": 2,
                       "window": [ 3, 4 ]
                     },
                     {
                       "cur": 3,
                       "window": [ 4 ]
                     },
                     {
                       "cur": 4,
                       "window": []
                     }
                   ];

```
## slidingWindowBySize() ##

> _**Description**_ Given an array input and two longs size and offset, this function associates each element
> > curElem in input with a sub-array of input, called a window, which is computed using
> > size and offset. The window is a "sliding" window since for each curElem the window is
> > computed relative to curElem. The window is said to be sliding across
> > the input array.


> Usage
```
                   [ { cur: any, window: [ any ] } ] slidingWindowBySize( [any] input,
                                                                          long size,
                                                                          long offset = 1 - size,
                                                                          boolean exact = false ) )
```
> In order to find the sliding window of each element curElem = input[curIndex](.md) in input,
> slidingWindow() associates curElem with the slice of input from curIndex + offset to
> curIndex + offset + size:

> input[curIndex + offset : curIndex + offset + size ](.md)

> The optional parameter exact can be specified by callers to indicate if only
> windows of size size are returned.

> Note: slidingWindow() computes its window in a positional way.
> See slidingWindowBySize() for a value-based version of slidingWindowsBySize().

> Note: slidingWindowBySize() is optimized as a streaming operation. The memory used by slidingWindow()
> > is proportional to the size of the window, not the input array.


> _**Parameters**_ (2 - 4 inputs)
> Input Types: `( input, required: schema [ * ]?),( size, required: schema long),( offset = null: schema long?),( exact = false: schema boolean)`

> _**Output**_ {{{schema [
> > { **}**
> > ]}}}


> _**Examples**_
```
jaql> // return the next element in the array (by index; not value)
                   [1, 2, 3, 4] -> slidingWindowBySize( 1, 1 );
                   
                   [
                     {
                       "cur": 1,
                       "window": [ 2 ]
                     },
                     {
                       "cur": 2,
                       "window": [ 3 ]
                     },
                     {
                       "cur": 3,
                       "window": [ 4 ]
                     },
                     {
                       "cur": 4,
                       "window": []
                     }
                   ];

```
## toArray() ##

> _**Description**_ This function wraps the input into a JSON array. In case the input is a JSON array or null, this function simply returns the input.

> Usage :

> [T1](T1.md)toArray(T1)

> Output: A JSON array that wraps the input object

> _**Parameters**_ (1 inputs)
> Input Types: `( arg0, required: schema any)`

> _**Output**_ `schema any`

> _**Examples**_
```
jaql> {var:"HadoopTemp"} -> toArray();
               [
                 {
                   "var": "HadoopTemp"
                 }
               ]

```
## tumblingWindow() ##

> _**Description**_ Given an input array input, a start and a stop predicate, tumblingWindows() computes
> > non-overlapping sub-arrays, also called windows, of input. Starting from the current index
> > in input or the beginning of input, tumblingWindow() probes the predicates start and stop to
> > find an appropriate window to return. Once that window has been determined, it is returned and
> > the current index is advanced past the last element in the returned window. Elements contained
> > in this window are nnot returned again. The window has "tumbled".


> Usage
```
                  [ [ any ] ] tumblingWindow( [any] input,
                                              function stop,
                                              function start,
                                              boolean firstGroup = true,
                                              boolean lastGroup = true )
```
> where stop is a function defined with the following signature

> boolean stop( any prev, any first, any last, any next, long size )

> and start is a function  defined with the following signature

> boolean start( any prev, any first, any size )

> Note: tumblingWindow() uses a call-by-name model to probe start and stop. That means, that the
> > names of the parameters to the user-provider predicates has to match the parameter names
> > provided here.


> In order to find each tumbling window in input, tumblingWindow() keeps the state of the last
> element it looked and the current element. The last element may be null if it is to determine the
> first window. Then, tumblingWindows() repeatedly evaluates the predicate start() beginning
> at the current element until start() evaluates to true or the end of the array is reached:
```
                   start( input[ lastIndex ], input[ currentIndex ], 1 )     --> false
                   start( input[ lastIndex + 1 ], input[ curIndex + 1 ], 2 ) --> false
                   start( input[ lastIndex + 2 ], input[ curIndex + 2 ], 3 ) --> false
                   ...
                   start( input[ lastIndex + n ], input[ curIndex + n ], n + 1 ) --> true (or end of array)
```
> Let startElem = input[curIndex + n ](.md). Then, tumblingWindow() determines the end of the window by
> probing stop():
```
                   stop( input[ lastIndex + n ], input[ startIndex ], input [ startIndex ], input [ startIndex + 1 ], 1 )             --> false
                   stop( input[ lastIndex + n ], input[ startIndex ], input [ startIndex + 1 ], input [ startIndex + 2 ], 2 )         --> false
                   stop( input[ lastIndex + n ], input[ startIndex ], input [ startIndex + 2 ], input [ startIndex + 3 ], 3 )         --> false
                    ...
                   stop( input[ lastIndex + n ], input[ startIndex ], input [ startIndex + k ], input [ startIndex + k + 1 ], k + 1 ) --> true
                      (or end of array )                   
```
> The current window is defined as the slice of input[startIndex: startIndex + k ](.md).

> If firstGroup is set to false, the first window is not returned. If lastGroup is set to false,
> the last window is not returned.

> _**Parameters**_ (2 - 5 inputs)
> Input Types: `( input, required: schema [ * ]?),( stop, required: schema function),( start = null: schema function?),( firstGroup = true: schema boolean?),( lastGroup = true: schema boolean?)`

> _**Output**_ {{{schema [
> > [\* ](.md) **> > ]}}}**


> _**Examples**_
```
jaql> // Split an array into an array of sub-arrays of size two
  
                   [1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ]
                     -> tumblingWindow( fn( prev, first, last, next, size ) size == 2, fn( prev, first, size) true );
                     
                   [
                     [ 1, 2 ],
                     [ 3, 4 ],
                     [ 5, 6 ],
                     [ 7, 8 ]
                   ]
                     
                   // Split an array into an array of sub-arrays using null as delimiter value
                   [1, 2, null, 3, 4, 5, null, 6, 7, 8]
                     -> tumblingWindow( fn( prev, first, last, next, size) isnull( next ), fn( prev, first, size) isnull( prev ) );

                   [
                     [ 1, 2 ],
                     [ 3, 4, 5 ],
                     [ 6, 7, 8 ]
                   ]

```
## tumblingWindowBySize() ##

> _**Description**_ Given an input array input, and a size parameter, tumblingWindows() returns
> > non-overlapping sub-arrays, also called tumbling windows, of input of size size.


> Usage
> [[ any ](.md) ] tumblingWindowBySize( [any](any.md) input,
> > long size,
> > boolean lastGroup = true )


> If lastGroup is set to false, the last ( possibly incomplete group ) is not returned.

> _**Parameters**_ (2 - 3 inputs)
> Input Types: `( input, required: schema [ * ]?),( size, required: schema long | double | decfloat),( lastGroup = true: schema boolean?)`

> _**Output**_ `schema [ * ]`

> _**Examples**_
```
jaql> // Split an array into an array of sub-arrays of size two
  
                   [1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ] -> tumblingWindowBySize( 2 );
                     
                   [
                     [ 1, 2 ],
                     [ 3, 4 ],
                     [ 5, 6 ],
                     [ 7, 8 ]
                   ]
                    
                   // Split an array into an array of sub-arrays of size three, don't return the incomplete last
                   // window
  
                   [1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ] -> tumblingWindowBySize( 3, false );
                     
                   [
                     [ 1, 2, 3 ],
                     [ 4, 5, 6 ]
                   ]

```
## union() ##

> _**Description**_ This function unions multiple JSON arrays into one JSON array in arbitrary order without removing duplicates (like SQL's UNION ALL)

> Usage

> [T1](T1.md)union(JSON array1, JSON array2, ...)

> Input Parameters: An arbitrary number of JSON arrays
> Output: A JSON array containing the union of all input arrays

> _**Parameters**_ (1 - 2 inputs)
> Input Types: `( arg0, required: schema any),( arg1 = null: schema any)...`

> _**Output**_ `schema any`

> _**Examples**_
```
jaql> union([1,2],[3,4,5]); // Union of two elements with different count of elements.
              [
                1,
                2,
                3,
                4,
                5
              ]

jaql> union([1,2],null,[5,6]); // Union with null element 
              [
                1,
                2,
                5,
                6
              ]

```

---

# index #
## keyLookup() ##

> _**Description**_ This function performs a left outer join on two sets of key/value pairs. It works similar to a hash-join in relational databases.
> > In the first step, the function builds a hash table on the inner key/value pairs (expr[1](1.md)).
> > For each key/value in the outer pairs (expr[0](0.md))
> > > return [key, value1, value2] tuples.


> Usage :

> [[key,value1](.md) <outer key/value pairs> ] -> keyLookup([[key,value2](.md) < inner key/value pairs > ]) ==> [[key, value1, value2](.md) ]

> Input: - JSON array of outer key value pairs
> Input Parameter: - JSON array of inner key/value pairs
> Output: JSON array of joined key/value pairs

> If the outer key does not exist in the inner set, null is returned for the inner value.
> So this is preserving the outer input (left outer join)

> Note: The function assumes that the inner keys are unique, otherwise an arbitrary value is kept.

> // Join between an inner and an outer array of key/value pairs

> _**Parameters**_ (2 inputs)
> Input Types: `( arg0, required: schema any),( arg1, required: schema any)`

> _**Output**_ `schema any`

> _**Examples**_
```
jaql> [ [1,1], [1,2], [2,3] ] -> keyLookup( [ [1,5], [2,6]] );
              [ 
                [1, 1, 5],[1, 2, 5], [ 2, 3, 6]
              ]
 // Join with non unique inner key values.

jaql> [ [1,1], [1,2], [2,3] ] -> keyLookup( [ [1,2], [1,5],[2,6]] );
              [
                [1, 1, 5], [1, 2, 5], [2, 3, 6]
              ]
              
 // Non existing inner key value, preserving the outer key value though

jaql> [ [1,1], [2,3], [3,4] ] -> keyLookup( [ [1,2], [1,5],[2,6]] );
              [
                [1, 1, 5], [2, 3, 6], [3, 4, null]
              ]

```
## keyMerge() ##

> _**Description**_ Given two arrays of key-value pairs ( sub-arrays ) a1 and a2, this function
> > returns an array resulting from merging a1 and a2 based on the key of each pair.


> Note: The input arrays a1 and a2 must be sorted on key. The behavior of this function
> is undefined if there are duplicate keys within a2.

> Usage
> Let k be of type T:
> [[ T, any, any ](.md) ] keyMerge( [[ T key1, any value1 ](.md) ] a1, [T key2, any value2 ](.md) ] a2 )

> For each key/value1 pair in a1 find the key/value2 pair in a2 and add [key, valu1, value2 ](.md)
> to the result array which is returned upon the end of a1.

> This function only requires a single key from each array to be in memory at a time.

> _**Parameters**_ (2 inputs)
> Input Types: `( arg0, required: schema any),( arg1, required: schema any)`

> _**Output**_ `schema any`

> _**Examples**_
```
jaql> [ [ 1, 10 ], [ 2, 20 ], [ 3, 30 ] ] -> keyMerge( [ [ 1, 100 ], [ 2, 200 ], [ 3, 300 ] ] );
 
                  [ [ 1, 10, 100 ],
                    [ 2, 20, 200 ],
                    [ 3, 30, 300 ] ]

```
## probeLongList() ##

> _**Description**_ probeLongList() probes as array of longs of build keys passed in using an array of key/value pairs
> > called input. probeLongList() returns an array of size size(in):
> > For each key/value pair in input, probeLongList() represents the lookup result as a number
> > index in the following way:


> - index is >= 0 if the value is found in the list of keys.
> (it is the index of the key in the sorted list of keys, but that may change in the future)
> - index < 0  if not found
> (it is the (-(insertion point) - 1 ) as defined by Arrays.binarySearch(),
> > but that may change in the future)


> probeLongList() returns an array of tuples that pairs up key, value, and the index.

> Usage
> [[ long key, any value, long index ](.md) ] probeLongList( [[ long? key, any value ](.md) ],
> > [long? key ](.md) )


> probeLongList() builds a compact in-memory representation of an array of longs of build keys.


> Note that all probe items are returned.
> This allows us to support in and not-in predicates, as well as just simple annotations.
> Nulls are tolerated in the probe keys, but they will never find a match.
> Null [key,value] pairs are not tolerated; a pair is always expected.

> There is currently an implementation limit of 2B values (~16GB of memory).

> _**Parameters**_ (2 inputs)
> Input Types: `( arg0, required: schema any),( arg1, required: schema any)`

> _**Output**_ `schema any`

> _**Examples**_
```
jaql> [ [ 1, 10 ], [ 2, 20 ], [ 3, 30 ], [ 4, 30 ] ] -> probeLongList([ 1, 2, 4] );
 
                   [
                     [ 1, 10, 0 ],
                     [ 2, 20, 1 ],
                     [ 3, 30, -3 ],
                     [ 4, 40, 2 ]                     
                   ]

```

---

# schema #
## check() ##

> _**Description**_ This function checks whether the first argument matches the schema given in the second argument.
> > If so, the function returns the first argument. Otherwise, it throws an exception.


> Usage :

> T1 check(T1 val1, schema any)

> Input Parameters: A JSON object that is to be verified against the schema.
> Output: The JSON object is returned in case the verification was successful.

> // Checks that the first parameter is of type long

> _**Parameters**_ (2 inputs)
> Input Types: `( arg0, required: schema any),( arg1, required: schema any)`

> _**Output**_ `schema any`

> _**Examples**_
```
jaql> check(1, schema long);
              1
              
 // Verifies the schema of a JSON record

jaql> check({a:"test", b:3}, schema{a:string, b:long});
              {
                "a": "test",
                "b": 3
              }             
              
 // Verifies the schema of a JSON array

jaql> check([true,2], schema [boolean,long]);
              [
                true,
                2
              ]

```
## typeof() ##

> _**Description**_ This function returns the type of a JSON object

> Usage :

> string typeof(T1 val1)

> Input Parameters: Any JSON object
> Output: The string value of the JSON object type

> _**Parameters**_ (1 inputs)
> Input Types: `( arg0, required: schema any)`

> _**Output**_ `schema any`

> _**Examples**_
```
jaql> typeof(1);
              "long"

jaql> typeof("test");
              "string"

jaql> typeof([true,2]);
              "array"

jaql> typeof({a:"test", b:3});
              "record"

```

---

# xml #
## jsonToXml() ##

> _**Description**_ An expression for converting JSON to XML. It is called as follows:
> ```
jsonToXml()``` . It is counterpart of {@link XmlToJsonFn}. But it
> does not perform a conversion which is reverse to the conversion in
> {@link XmlToJsonFn}. The reason is:
> <ol>
<blockquote><li>There is no concepts such as namespace in JSON</li>
<li>The conversion is for a conversion from general JSON to XML. It is the<br>
commons case that the JSON to be converted is not converted from XML.</li>
</ol></blockquote>

> Only a JSON value satisfying the following conditions can be converted to
> XML:
> <ol>
<blockquote><li>It is a JSON record whose size is 1.</li>
<li>The value of the only JSON pair in this JSON record is not JSON array.</li>
</ol></blockquote>

> An array nested in another array does not inherit the nesting array. For
> example, ```
{content: [[1, 2]]}``` is converted to:

> <pre>
&lt;content&gt;<br>
&lt;array&gt;1&lt;/array&gt;<br>
&lt;array&gt;2&lt;/array&gt;<br>
&lt;/content&gt;<br>
<br>
<pre><br>
<br>
_*Parameters*_ (1 inputs)<br>
Input Types: ( arg0, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
== typedXmlToJson() ==<br>
<br>
_*Description*_ This function converts an XML string to a JSON object and tries to preserve the type information<br>
<br>
Usage :<br>
<br>
T1 typedXmlToJson(string <XML string>)<br>
<br>
Output: A JSON object that represents the XML string<br>
<br>
This function is similar to xmlToJson, except that it creates typed data, i.e., instead of producing all values as strings,<br>
It tries to cast each value to a closest type.<br>
<br>
// Typed conversion creates a string and a long for the two elements of the array<br>
<br>
_*Parameters*_ (1 inputs)<br>
Input Types: ( arg0, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; typedXmlToJson("&lt;?xml version=\"1.0\" encoding=\"UTF-8\"?&gt;&lt;array&gt;&lt;value&gt;test&lt;/value&gt;&lt;value&gt;1&lt;/value&gt;&lt;/array&gt;");<br>
              {<br>
                "array": <br>
                {<br>
                  "value": <br>
                  [<br>
                    "test",<br>
                    1<br>
                  ]<br>
                }<br>
              }<br>
<br>
 // Typed conversion creates data values that matches the type information<br>
<br>
jaql&gt; typedXmlToJson("&lt;?xml version=\"1.0\" encoding=\"UTF-8\"?&gt;&lt;array&gt;&lt;value type=\"string\"&gt;test&lt;/value&gt;&lt;value type=\"long\"&gt;2&lt;/value&gt;&lt;/array&gt;");<br>
              {<br>
                "array": {<br>
                  "value": [<br>
                  {<br>
                    "@type": "string",<br>
                    "text()": "test"<br>
                  },<br>
                  {<br>
                    "@type": "long",<br>
                    "text()": 2<br>
                  } ]<br>
                }<br>
              }<br>
<br>
</code></pre>
== xmlToJson() ==<br>
<br>
_*Description*_ This function converts an XML string to a JSON object, all values are created with string type<br>
<br>
Usage :<br>
<br>
T1 xmlToJson(string <XML string>)<br>
<br>
Output: A JSON object that represents the XML string<br>
<br>
This function is similar to typedXmlToJson, except that it creates non-typed data, i.e., all values are created with strings type<br>
<br>
// Non-Typed conversion creates a string for a long value<br>
<br>
_*Parameters*_ (1 inputs)<br>
Input Types: ( arg0, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; xmlToJson("&lt;?xml version=\"1.0\" encoding=\"UTF-8\"?&gt;&lt;array&gt;&lt;value&gt;test&lt;/value&gt;&lt;value&gt;1&lt;/value&gt;&lt;/array&gt;");<br>
              {<br>
                "array": <br>
                {<br>
                  "value": <br>
                  [<br>
                    "test",<br>
                    "1"<br>
                  ]<br>
                }<br>
              }<br>
<br>
 // Typed conversion creates data values that does not match the type information<br>
<br>
jaql&gt; xmlToJson("&lt;?xml version=\"1.0\" encoding=\"UTF-8\"?&gt;&lt;array&gt;&lt;value type=\"string\"&gt;test&lt;/value&gt;&lt;value type=\"long\"&gt;2&lt;/value&gt;&lt;/array&gt;");<br>
              {<br>
                "array": {<br>
                  "value": [<br>
                  {<br>
                    "@type": "string",<br>
                    "text()": "test"<br>
                  },<br>
                  {<br>
                    "@type": "long",<br>
                    "text()": "2"<br>
                  } ]<br>
                }<br>
              }<br>
<br>
</code></pre>
== xpath() ==<br>
<br>
_*Description*_ This function runs an XPath on an XML document<br>
<br>
Usage :<br>
<br>
[T1] xpath(string <XML string>, string <xpath>, string <namespace>)<br>
<br>
Output: A JSON array containing the result of the xpath filter<br>
<br>
_*Parameters*_ (2 - 3 inputs)<br>
Input Types: ( arg0, required: schema any),( arg1, required: schema any),( arg2 = null: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; xpath("&lt;?xml version=\"1.0\" encoding=\"UTF-8\"?&gt;&lt;record&gt;&lt;content type=\"record\"&gt;&lt;city&gt;Beijing&lt;/city&gt;&lt;no type=\"array\"&gt;&lt;value   type=\"long\"&gt;1&lt;/value&gt;&lt;value type=\"long\"&gt;2&lt;/value&gt;&lt;value type=\"long\"&gt;3&lt;/value&gt;&lt;/no&gt;&lt;/content&gt;&lt;/record&gt;", <br>
                    "record/content/city");<br>
                    [<br>
                      {<br>
                        "city": "Beijing"<br>
                      }<br>
                    ]<br>
<br>
</code></pre>
== xslt() ==<br>
<br>
_*Description*_ This function runs XSLT on an XML document<br>
<br>
Usage :<br>
<br>
{T1} xslt(string <XML string>, string <xslt>)<br>
<br>
Output: A JSON record holding the result of the xslt transformation<br>
<br>
_*Parameters*_ (2 inputs)<br>
Input Types: ( arg0, required: schema any),( arg1, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; xslt("&lt;?xml version=\"1.0\" encoding=\"UTF-8\"?&gt;&lt;?xml-stylesheet type=\"text/xsl\" ?&gt;<br>
                    &lt;record&gt;&lt;content type=\"record\"&gt;&lt;city&gt;Beijing&lt;/city&gt;&lt;no type=\"array\"&gt;&lt;value type=\"long\"&gt;1&lt;/value&gt;&lt;value type=\"long\"&gt;2&lt;/value&gt;&lt;/no&gt;&lt;/content&gt;&lt;/record&gt;",<br>
                   "&lt;?xml version=\"1.0\" ?&gt;&lt;xsl:stylesheet version=\"1.0\" xmlns:xsl=\"http://www.w3.org/1999/XSL/Transform\"&gt;<br>
                    &lt;xsl:template match=\"city\"&gt;	&lt;p&gt;&lt;city&gt;&lt;xsl:value-of select=\".\"/&gt;&lt;/city&gt;&lt;/p&gt;	&lt;/xsl:template&gt; &lt;/xsl:stylesheet&gt;");<br>
                    {<br>
                      "p": <br>
                      {<br>
                        "city": "Beijing"<br>
                      }<br>
                    }<br>
<br>
</code></pre>
----<br>
=regex=<br>
== regex() ==<br>
<br>
_*Description*_ Create a regular expression (regex).<br>
<br>
Usage:<br>
regex regex(string reg)<br>
<br>
regex(string reg) defines a regular expression, specified by a string, the regular-expression constructs complies<br>
with standard java.<br>
<br>
_*Parameters*_ (1 - 2 inputs)<br>
Input Types: ( arg0, required: schema any),( arg1 = null: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; reg = regex("[a-z]+"); regex_match(reg,"abc bcd");<br>
 ["abc"]<br>
<br>
</code></pre>
== regex_extract() ==<br>
<br>
_*Description*_ Capture every first substrings which match each group (A group is a pair of parentheses used to<br>
group subpatterns.) specified in the regular expression. Return a string array like :<br>
["match_group1", "match_group2" , "match_group3" ...]<br>
<br>
Usage:<br>
[string] regex_extract(regex reg, string text)<br>
<br>
reg is the regular expression, text is the target string. For example, given a regular expression<br>
(a(b*))+(c*)<br>
it contains 3 groups:<br>
group 1: (a(b*))<br>
group 2: (b*)<br>
group 3: (c*)<br>
if input is "abbabcd", by use of regex_extract function, substrings matches each group(1-3) will be captured, this function<br>
will return a string array, like<br>
[ "ab", "b", "c"]<br>
where "ab" is the first hit matches group 1, as well as "b" to group 2, "c" to group 3.<br>
<br>
_*Parameters*_ (2 inputs)<br>
Input Types: ( arg0, required: schema any),( arg1, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; regex_extract(regex("(a(b*))+(c*)"),"abbabcd");<br>
 [ "ab", "b", "c"]<br>
<br>
jaql&gt; regex_extract(regex("(a(b*))"),"abbabcd");<br>
 [ "abb", "bb"]<br>
<br>
</code></pre>
== regex_extract_all() ==<br>
<br>
_*Description*_ Capture all the substrings which match each group (A group is a pair of parentheses used to<br>
group subpatterns.) specified in the regular expression. Return a string array like<br>
[[match1_group1, match1_group2 ...] , [match2_group1, match2_group2] ... ]<br>
<br>
Usage:<br>
[string] regex_extract(regex reg, string text)<br>
<br>
regex_extract_all(regex("(a(b*))"),"abbabcd");<br>
<br>
reg is the regular expression, text is the target string. For example, given a regular expression<br>
(a(b*))<br>
it contains 3 groups:<br>
group 1: (a(b*))<br>
group 2: (b*)<br>
if input is "abbabcd", by use of regex_extract function, substrings matches each group(1-2) will be captured, this function<br>
will return a string array, like<br>
[<br>
["abb","bb"],<br>
["ab","b"]<br>
]<br>
<br>
where "abb" and "bb" is the first match of group 1 and 2 when scaning the text, "ab" and "b" is the second(last) match.<br>
<br>
_*Parameters*_ (2 inputs)<br>
Input Types: ( arg0, required: schema any),( arg1, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; regex_extract_all(regex("(a(b*))+(c*)"),"abbabcd");<br>
 [ <br>
  [ "ab", "b", "c"]<br>
 ]<br>
<br>
jaql&gt; regex_extract_all(regex("(a(b*))"),"abbabcd");<br>
  [ <br>
   ["abb","bb"],<br>
   ["ab","b"]<br>
  ]<br>
<br>
</code></pre>
== regex_match() ==<br>
<br>
_*Description*_ Returns the first substring in input that matches the pattern against the regular expression.<br>
<br>
Usage:<br>
<br>
regex_match(regex reg , string text)<br>
<br>
reg is the regular expression, text is the target string.<br>
<br>
_*Parameters*_ (2 inputs)<br>
Input Types: ( arg0, required: schema any),( arg1, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; regex_match(regex("[a-z]?"),"abbabcd");<br>
 "a" //this example performs a non-greedy matching<br>
<br>
jaql&gt; regex_match(regex("[a-z]*"),"abbabcd");<br>
 "abbabcd"//this example performs a greedy matching<br>
<br>
</code></pre>
== regex_spans() ==<br>
<br>
_*Description*_ Match a subset of the input, return a [begin,end] pair that indexes into the<br>
original string, where begin indicates the start index of the previous match, as well as end<br>
indicates the offset after the last character matched.<br>
<br>
Usage:<br>
[string] regex_spans(regex reg, string text);<br>
<br>
_*Parameters*_ (2 inputs)<br>
Input Types: ( arg0, required: schema any),( arg1, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; regex_spans(regex("bcd"),"abbabcd");<br>
 [ span(4,6) ]<br>
<br>
jaql&gt; regex_spans(regex("[a-z]+"),"abbabcd");<br>
 [ span(0,6) ]<br>
<br>
</code></pre>
== regex_test() ==<br>
<br>
_*Description*_ Check if the target string contains substring matches given regular expression.<br>
If exist at least 1 match, return true, else return false<br>
<br>
Usage:<br>
bool regex_test(regex reg , string text)<br>
<br>
_*Parameters*_ (2 inputs)<br>
Input Types: ( arg0, required: schema any),( arg1, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; regex_test(regex("[a-z]?"),"abbabcd");<br>
 true<br>
<br>
jaql&gt; regex_test(regex("aaa"),"abbabcd");<br>
 false<br>
<br>
</code></pre>
----<br>
=binary=<br>
== base64() ==<br>
<br>
_*Description*_ Convert an ascii/utf8 base64 string into a binary string.<br>
<br>
Usage:<br>
<br>
binary base64(string str)<br>
<br>
_*Parameters*_ (1 inputs)<br>
Input Types: ( arg0, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; base64("utf8string");<br>
  hex('BAD7FCB2DAE20000')<br>
<br>
</code></pre>
== hex() ==<br>
<br>
_*Description*_ Convert a hexadecimal string into a binary string.<br>
<br>
Usage:<br>
<br>
binary hex(string str)<br>
<br>
_*Parameters*_ (1 inputs)<br>
Input Types: ( arg0, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; hex("a00f");<br>
  hex('A00F')<br>
<br>
</code></pre>
----<br>
=date=<br>
== date() ==<br>
<br>
_*Description*_ Format a string to date value.<br>
<br>
Usage:<br>
date date(string datestr)<br>
<br>
_*Parameters*_ (1 - 2 inputs)<br>
Input Types: ( arg0, required: schema any),( arg1 = null: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; date('2000-01-01T11:59:59Z');<br>
 date('2000-01-01T12:00:00.000Z');<br>
<br>
</code></pre>
== dateMillis() ==<br>
<br>
_*Description*_ Represent the date using milliseconds.<br>
<br>
Usage:<br>
long dateMillis(date d)<br>
<br>
the argument is restricted with date type, or it causes bad casting exception.<br>
<br>
_*Parameters*_ (1 inputs)<br>
Input Types: ( arg0, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; dateMillis(date('2000-01-01T12:00:00Z'));<br>
 946728000000<br>
<br>
</code></pre>
== dateParts() ==<br>
<br>
_*Description*_ Return a record which stores all readable fields of a date, including year, montch, day, dayofweek ... e.g.<br>
<br>
Usage:<br>
<br>
record dateParts(date d)<br>
<br>
_*Parameters*_ (1 inputs)<br>
Input Types: ( arg0, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; dateParts(date('2000-01-01T12:00:00Z'));<br>
 {<br>
 "day": 1,<br>
 "dayOfWeek": 6,<br>
 "hour": 12,<br>
 "millis": 946728000000,<br>
 "minute": 0,<br>
 "month": 1,<br>
 "second": 0,<br>
 "year": 2000,<br>
 "zoneOffset": 0<br>
 }<br>
<br>
</code></pre>
== now() ==<br>
<br>
_*Description*_ Return current system date time.<br>
<br>
Usage:<br>
date now()<br>
<br>
_*Parameters*_ (0 inputs)<br>
Input Types: {{{}}}<br>
<br>
_*Output*_ schema any<br>
<br>
----<br>
=nil=<br>
== denull() ==<br>
<br>
_*Description*_ remove nulls from a given array<br>
Usage:<br>
[T] denull([T]);<br>
<br>
_*Parameters*_ (1 inputs)<br>
Input Types: ( arg0, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; denull( [1, null, 3] );<br>
 [ 1, 3 ]<br>
<br>
</code></pre>
----<br>
=agg=<br>
== any() ==<br>
<br>
_*Description*_ This function picks any value form an input JSON array. If there is at least one non-null values, picks a non-null value.<br>
<br>
Usage :<br>
<br>
T1 any([T1])<br>
<br>
Output: If exists, this function returns any single non-null value from the input JSON array.<br>
Null is only returned if there is no non-null value in the array.<br>
<br>
_*Parameters*_ (1 inputs)<br>
Input Types: ( a, required: schema [ * ]?)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; [null, 1, null]-&gt;any();<br>
              1<br>
<br>
jaql&gt; []-&gt;any();<br>
              null<br>
<br>
</code></pre>
== argmax() ==<br>
<br>
_*Description*_ This function returns the maximum value of a JSON array after applying a function on every value.<br>
<br>
Usage :<br>
<br>
T1 argmax([T1] array1, schema function(T1));<br>
<br>
Input: - A JSON array array1, which is searched for the max value in context of a function.<br>
- A function that is applied on every element of the array before evaluating the maximum<br>
<br>
_*Parameters*_ (2 inputs)<br>
Input Types: ( a, required: schema [ * ]?),( f, required: schema function)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; argmax([-3,-2,-1], fn(v) (v)); <br>
              -1<br>
<br>
jaql&gt; argmax([-3,-2,-1], fn(v) (v*v)); <br>
              -3<br>
<br>
</code></pre>
== argmin() ==<br>
<br>
_*Description*_ This function returns the minimum value of a JSON array after applying a function on every value.<br>
<br>
Usage :<br>
<br>
T1 argmin([T1] array1, schema function(T1));<br>
<br>
Input: - A JSON array array1, which is searched for the min value in context of a function.<br>
- A function that is applied on every element of the array before evaluating the minimum<br>
<br>
_*Parameters*_ (2 inputs)<br>
Input Types: ( a, required: schema [ * ]?),( f, required: schema function)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; argmin([-3,-2,-1], fn(v) (v)); <br>
              -3<br>
<br>
jaql&gt; argmin([-3,-2,-1], fn(v) (v*v)); <br>
              -1<br>
<br>
</code></pre>
== array() ==<br>
<br>
_*Description*_ Usage :<br>
<br>
[T1] array([T1])<br>
<br>
Output:<br>
<br>
_*Parameters*_ (1 inputs)<br>
Input Types: ( arg0, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; [1]-&gt;array();<br>
              [<br>
                1<br>
              ]<br>
<br>
</code></pre>
== avg() ==<br>
<br>
_*Description*_ This function calculates the arithmetic mean (average) of a list of numbers<br>
<br>
Usage :<br>
<br>
long avg([long,...])<br>
<br>
Input Parameters: A JSON array of numbers<br>
Output: The arithmetic mean over all numbers in the input array<br>
<br>
_*Parameters*_ (1 inputs)<br>
Input Types: ( arg0, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; [100, 500, 700] -&gt; avg();<br>
              433<br>
<br>
jaql&gt; books = [<br>
               {publisher: 'Scholastic',<br>
                author: 'J. K. Rowling',<br>
                title: 'Chamber of Secrets',<br>
                year: 1999, <br>
                reviews: [<br>
                  {rating: 10, user: 'joe', review: 'The best ...'},<br>
                  {rating: 6, user: 'mary', review: 'Average ...'}]}<br>
              ];<br>
  <br>
              // Retrieves all the books with an average rating higher than 5<br>
              books-&gt; filter avg($.reviews[*].rating) &gt; 5 -&gt; transform {$.author, $.title};<br>
              [<br>
                {<br>
                  "author": "J. K. Rowling",<br>
                  "title": "Chamber of Secrets"<br>
                }<br>
              ]<br>
<br>
</code></pre>
== count() ==<br>
<br>
_*Description*_ This function counts the number of elements in a JSON array<br>
<br>
Usage :<br>
<br>
long count([T1])<br>
<br>
Output: The number of elements in the JSON array<br>
<br>
_*Parameters*_ (1 inputs)<br>
Input Types: ( arg0, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; [1,2,3]-&gt;count();<br>
              3<br>
<br>
jaql&gt; books = [<br>
               {publisher: 'Scholastic',<br>
                author: 'J. K. Rowling',<br>
                title: 'Chamber of Secrets',<br>
                year: 1999, <br>
                reviews: [<br>
                  {rating: 10, user: 'joe', review: 'The best ...'},<br>
                  {rating: 6, user: 'mary', review: 'Average ...'}]}<br>
              ];<br>
              <br>
              // Counts the number of books per publisher<br>
              books-&gt; group by $p = ($.publisher) into {publisher: $p, num: count($)} -&gt; sort by [$.publisher];<br>
              [<br>
                {<br>
                  "publisher": "Scholastic",<br>
                  "num": 1<br>
                }<br>
              ]<br>
<br>
</code></pre>
== javauda() ==<br>
<br>
_*Description*_ This function is a function constructor for a user-defined aggregate (function) that<br>
is written in Java.<br>
<br>
Usage<br>
function javauda( string className,<br>
any args )<br>
<br>
className is a loadable class which implements the JavaUda interface.<br>
<br>
The result of javauda() is a function<br>
<br>
any myJavaUda( array )<br>
<br>
This function is declared experimental. Specifics to be done.<br>
<br>
_*Parameters*_ (1 - 2 inputs)<br>
Input Types: ( class, required: schema string),( args = null: schema any)...<br>
<br>
_*Output*_ schema function<br>
<br>
== javaudacall() ==<br>
<br>
_*Description*_<br>
<br>
_*Parameters*_ (2 - 3 inputs)<br>
Input Types: ( array, required: schema [ * ]?),( class, required: schema string),( args = null: schema any)...<br>
<br>
_*Output*_ schema any<br>
<br>
== max() ==<br>
<br>
_*Description*_ Find the max value in an array.<br>
Usage:<br>
<br>
any max( [ any ] );<br>
<br>
Max takes an array as input and returns the max value from the array. The type of the array's elements is not restricted.<br>
<br>
_*Parameters*_ (1 inputs)<br>
Input Types: ( arg0, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; max([1,2,3]);<br>
 3<br>
<br>
jaql&gt; max(["a","b","c"]);<br>
 "c"<br>
<br>
jaql&gt; read(hdfs("someFileOfLongs")) -&gt; group into max($);<br>
<br>
jaql&gt; read(hdfs("someFileOfPairs")) -&gt; group by g = $[0] into { first: g, maxSecond: max($[*][1]) };<br>
<br>
</code></pre>
== min() ==<br>
<br>
_*Description*_ Find the minimum value in an array.<br>
Usage:<br>
<br>
any max( [ any ] );<br>
<br>
Max takes an array as input and returns the minimum value from the array. The type of the array's elements is not restricted.<br>
<br>
_*Parameters*_ (1 inputs)<br>
Input Types: ( arg0, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; min([1,2,3]);<br>
 1<br>
<br>
jaql&gt; min(["a","b","c"]);<br>
 "a"<br>
<br>
jaql&gt; read(hdfs("someFileOfLongs")) -&gt; group into min($);<br>
<br>
jaql&gt; read(hdfs("someFileOfPairs")) -&gt; group by g = $[0] into { first: g, minSecond: min($[*][1]) };<br>
<br>
</code></pre>
== pickN() ==<br>
<br>
_*Description*_ select N elements from an array<br>
Usage:<br>
[T] pickN( [T], long n );<br>
<br>
Select n elements from the input array.<br>
<br>
_*Parameters*_ (2 inputs)<br>
Input Types: ( arg0, required: schema any),( arg1, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; pickN( [1,2,3], 2 )<br>
 [1,2]<br>
<br>
</code></pre>
== singleton() ==<br>
<br>
_*Description*_ ensure that an array has only one element, otherwise, throw an exception<br>
Usage:<br>
T singleton( [T] );<br>
<br>
_*Parameters*_ (1 inputs)<br>
Input Types: ( arg0, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; [1] -&gt; singleton<br>
 1<br>
<br>
jaql&gt; [1,2] -&gt; singleton // throws an exception<br>
<br>
</code></pre>
== sum() ==<br>
<br>
_*Description*_ compute the sum of an array of numbers<br>
Usage:<br>
number sum( [ number ] );<br>
<br>
Note that sum is currently evaluated using a sequential plan.<br>
To get parallelism, use group by:<br>
<br>
_*Parameters*_ (1 inputs)<br>
Input Types: ( arg0, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; read(hdfs("someNumbers")) -&gt; group into sum($);<br>
<br>
</code></pre>
== topN() ==<br>
<br>
_*Description*_ compute the top N values from an array<br>
Usage:<br>
[T] topN( [T], long n, cmp(x) );<br>
<br>
Given an input array, a limit n, and a comparator function, compute the top n elements<br>
of the input array. This implementation uses a heap to efficiently use memory and lower<br>
the network traffic that is needed for aggregation.<br>
<br>
_*Parameters*_ (3 inputs)<br>
Input Types: ( arg0, required: schema any),( arg1, required: schema any),( arg2, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; [1,2,3] -&gt; write(hdfs("test1"));<br>
<br>
jaql&gt; read(hdfs("test1")) -&gt; topN( 2, cmp(x) [x desc ] ); // Simplest example<br>
 [ 3, 2 ]<br>
<br>
jaql&gt; read(hdfs("test1")) -&gt; group into topN( $, 2, cmp(x) [ x desc ] ); // Now, with group by (this uses map-reduce)<br>
<br>
jaql&gt; [ [ 1, 1 ], [1, 2], [2, 0], [2, 11], [3, 3], [3, 4], [3, 5] ] -&gt; write(hdfs("test2"));<br>
<br>
jaql&gt; read(hdfs("test2")) -&gt; group by n = $[0] into { num: n, top: topN($[*][1], 1, cmp(x) [ x desc ]) }; // Complex data<br>
<br>
</code></pre>
== uda() ==<br>
<br>
_*Description*_ This function is a function constructor for a user-defined aggregate (function).<br>
<br>
Usage<br>
function uda( function init,<br>
function accumulate,<br>
function combine,<br>
fucntion final )<br>
<br>
The result of uda() is a function<br>
<br>
any myUda( array )<br>
<br>
This function is declared experimental. Specifics to be done.<br>
<br>
_*Parameters*_ (4 inputs)<br>
Input Types: ( init, required: schema function),( accumulate, required: schema function),( combine, required: schema function),( final, required: schema function)<br>
<br>
_*Output*_ schema function<br>
<br>
== udacall() ==<br>
<br>
_*Description*_<br>
<br>
_*Parameters*_ (5 inputs)<br>
Input Types: ( array, required: schema [ * ]?),( init, required: schema function),( accumulate, required: schema function),( combine, required: schema function),( final, required: schema function)<br>
<br>
_*Output*_ schema any<br>
<br>
----<br>
=number=<br>
== abs() ==<br>
<br>
_*Description*_ Return the absolute value of a numeric value<br>
<br>
Usage:<br>
number abs(number)<br>
<br>
_*Parameters*_ (1 inputs)<br>
Input Types: ( arg0, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; abs(-100);<br>
 100<br>
<br>
jaql&gt; abs(-3.14)<br>
 3.14<br>
<br>
</code></pre>
== decfloat() ==<br>
<br>
_*Description*_ Construct a decfloat value<br>
<br>
Usage:<br>
<br>
decfloat decfloat( string | number )<br>
<br>
_*Parameters*_ (1 inputs)<br>
Input Types: ( arg0, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; decfloat(5m);<br>
 5m<br>
<br>
jaql&gt; decfloat("5");<br>
 5m<br>
<br>
jaql&gt; decfloat("-1.5e-5");<br>
 -0.000015m<br>
<br>
jaql&gt; 5m instanceof schema decfloat(value=5m);<br>
 true<br>
<br>
</code></pre>
== div() ==<br>
<br>
_*Description*_ div(A,B) divides A by B, return a numric value.<br>
<br>
_*Parameters*_ (2 inputs)<br>
Input Types: ( arg0, required: schema any),( arg1, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; div(4,2);<br>
 2<br>
<br>
</code></pre>
== double() ==<br>
<br>
_*Description*_ Get the double value of a numric value.<br>
<br>
Usage:<br>
double double(number A);<br>
<br>
_*Parameters*_ (1 inputs)<br>
Input Types: ( arg0, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; 22d instanceof schema double;<br>
 true<br>
<br>
jaql&gt; double(5);<br>
 5.0<br>
<br>
jaql&gt; double(5m);<br>
 5.0<br>
<br>
jaql&gt; double(5d);<br>
 5.0<br>
<br>
</code></pre>
== exp() ==<br>
<br>
_*Description*_ raise base of natural log (e) to arg: e^a pow(x,y) = exp( y * ln(x) )<br>
<br>
Usage:<br>
<br>
decfloat | double exp( decfloat | double );<br>
<br>
_*Parameters*_ (1 inputs)<br>
Input Types: ( arg0, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; exp( 10 );<br>
 22026.465794806718<br>
<br>
jaql&gt; exp( 10m );<br>
 22026.46579480671789497137069702148m<br>
<br>
</code></pre>
== ln() ==<br>
<br>
_*Description*_ Return the natural logarithm of a numeric value<br>
<br>
Usage:<br>
number abs(number)<br>
<br>
_*Parameters*_ (1 inputs)<br>
Input Types: ( arg0, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
== long() ==<br>
<br>
_*Description*_ Parse the given atom value to long value<br>
<br>
Usage:<br>
long long(anyatom)<br>
<br>
_*Parameters*_ (1 inputs)<br>
Input Types: ( arg0, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; long(3.14)<br>
 3<br>
<br>
jaql&gt; long(3)<br>
 3<br>
<br>
jaql&gt; long(true)<br>
 1<br>
<br>
</code></pre>
== mod() ==<br>
<br>
_*Description*_ Return the modulus of a and b, both a and b are numeric values<br>
<br>
Usage:<br>
number mod(number a, number b)<br>
<br>
_*Parameters*_ (2 inputs)<br>
Input Types: ( arg0, required: schema any),( arg1, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; mod(3,2)<br>
 1<br>
<br>
</code></pre>
== pow() ==<br>
<br>
_*Description*_ Raise a number to power<br>
<br>
Usage:<br>
number pow(number a , number b)<br>
<br>
_*Parameters*_ (2 inputs)<br>
Input Types: ( arg0, required: schema any),( arg1, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; mod(3,2)<br>
 1<br>
<br>
</code></pre>
== toNumber() ==<br>
<br>
_*Description*_ convert a value to number<br>
Usage:<br>
number toNumber( e );<br>
<br>
Currently, this function converts booleans and strings.<br>
If a number is given as input, it returned verbatim.<br>
<br>
_*Parameters*_ (1 inputs)<br>
Input Types: ( arg0, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
----<br>
=string=<br>
== convert() ==<br>
<br>
_*Description*_ Converts an input value (string, array of strings or record with string values) to the specified types.<br>
Usage:<br>
<br>
T convert( string s, schema T );<br>
<br>
_*Parameters*_ (2 inputs)<br>
Input Types: ( arg0, required: schema any),( arg1, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; convert( "1", schema long);<br>
 1<br>
<br>
jaql&gt; convert( { a: "1" }, schema { a: long } );<br>
 {<br>
   "a": 1<br>
 }<br>
<br>
</code></pre>
== endsWith() ==<br>
<br>
_*Description*_ test whether a string has a given suffix<br>
Usage:<br>
bool endsWith( string s, string suffix )<br>
<br>
_*Parameters*_ (2 inputs)<br>
Input Types: ( arg0, required: schema any),( arg1, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
== json() ==<br>
<br>
_*Description*_ convert a string in json format into jaql's data model<br>
Usage:<br>
<br>
T json( string json );<br>
<br>
_*Parameters*_ (1 inputs)<br>
Input Types: ( arg0, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; json( "[1,2,3]" );<br>
 [1,2,3]<br>
<br>
</code></pre>
== serialize() ==<br>
<br>
_*Description*_ return a sctring representation of any value<br>
Usage:<br>
string serialze( value );<br>
<br>
_*Parameters*_ (1 inputs)<br>
Input Types: ( arg0, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
== startsWith() ==<br>
<br>
_*Description*_ bool startsWith(string target, string prefix)<br>
Check if a target string starts with a given prefix, return true or false<br>
<br>
_*Parameters*_ (2 inputs)<br>
Input Types: ( arg0, required: schema any),( arg1, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
== strcat() ==<br>
<br>
_*Description*_ Concats one or more strings to a new string<br>
<br>
Usage:<br>
string strcat(string ... str)<br>
<br>
_*Parameters*_ (0 - 1 inputs)<br>
Input Types: ( arg0 = null: schema any)...<br>
<br>
_*Output*_ schema any<br>
<br>
== strJoin() ==<br>
<br>
_*Description*_ Build a string that concatentates all the items, adding sep between each item.<br>
Nulls are removed, without any separator.<br>
If you want nulls, use firstNonNull(e,'how nulls appear').<br>
<br>
Usage:<br>
string strJoin(array items, string sep)<br>
<br>
_*Parameters*_ (2 inputs)<br>
Input Types: ( arg0, required: schema any),( arg1, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
== strLen() ==<br>
<br>
_*Description*_ long strLen(string str)<br>
return the lenght of the given string<br>
<br>
_*Parameters*_ (1 inputs)<br>
Input Types: ( arg0, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
== strPos() ==<br>
<br>
_*Description*_ This function returns the index of the first occurrence of search string within a string<br>
<br>
Usage :<br>
<br>
long strPos(string <string>, string <search string>, long <startIndex = 0>)<br>
<br>
Input Parameters: string: the string that is search within<br>
search string: the string that is search for<br>
startIndex: the starting position of the search within search<br>
Output: the index of the first occurrence of search string within string<br>
<br>
// Search for 'sample' within string.<br>
<br>
_*Parameters*_ (2 - 3 inputs)<br>
Input Types: ( arg0, required: schema any),( arg1, required: schema any),( arg2 = null: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; strPos('This is a sample string', 'sample', 0);<br>
              10<br>
              <br>
 // Only the first occurrence is reported<br>
<br>
jaql&gt; strPos('The This That These Those', 'Th', 0);<br>
              0<br>
<br>
</code></pre>
== strPosList() ==<br>
<br>
_*Description*_ This function returns the indexes of all occurrences of search string within a string.<br>
<br>
Usage :<br>
<br>
[long] strPosList(string <string>, string <search string>, long <startIndex = 0>)<br>
<br>
Input Parameters: string: the string that is search within<br>
search string: the string that is search for<br>
startIndex: the starting position of the search within search<br>
Output: A JSON array with the indexes of all occurrences of search string within string<br>
<br>
// Search for 'sample' within string.<br>
<br>
_*Parameters*_ (2 - 3 inputs)<br>
Input Types: ( arg0, required: schema any),( arg1, required: schema any),( arg2 = null: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; strPosList('This is a sample string', 'sample', 0);<br>
              [<br>
                10<br>
              ]<br>
              <br>
 // Only the first occurrence is reported<br>
<br>
jaql&gt; strPosList('The This That These Those', 'Th', 0);<br>
              [<br>
                0,<br>
                4,<br>
                9,<br>
                14,<br>
                20<br>
              ]<br>
<br>
</code></pre>
== strReplace() ==<br>
<br>
_*Description*_ Replace a substring with the replacement only if it matches the given regular expression (regex).<br>
Usage:<br>
[string] strReplace(string val, regex pattern, string replacement)<br>
<br>
_*Parameters*_ (3 inputs)<br>
Input Types: ( arg0, required: schema any),( arg1, required: schema any),( arg2, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; reg=regex("[a-z]+"); // define a regular expression, match at least one character.<br>
 val = "&lt;abc&gt;,&lt;bcd&gt;,&lt;cde&gt;"; // deine a string <br>
 strReplace(val,reg,"a"); // replace all the match substrings with "a"<br>
 <br>
 "&lt;a&gt;,&lt;a&gt;,&lt;a&gt;"<br>
<br>
</code></pre>
== strSplit() ==<br>
<br>
_*Description*_ Split a string with given delimiter.<br>
Usage:<br>
[string] strSplit(string val, string delimiter);<br>
<br>
_*Parameters*_ (2 inputs)<br>
Input Types: ( arg0, required: schema any),( arg1, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; strSplit("a,b,c,d",",");<br>
 [ "a","b","c","d" ]<br>
<br>
</code></pre>
== strToLowerCase() ==<br>
<br>
_*Description*_ Convert a string to lower case.<br>
<br>
Usage:<br>
string strToLowerCase(string val)<br>
<br>
_*Parameters*_ (1 inputs)<br>
Input Types: ( arg0, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; strToLowerCase("aBcDEFgHiJ");<br>
 "abcdefghij"<br>
<br>
</code></pre>
== strToUpperCase() ==<br>
<br>
_*Description*_ Convert a string to upper case.<br>
<br>
Usage:<br>
string strToUpperCase(string val)<br>
<br>
_*Parameters*_ (1 inputs)<br>
Input Types: ( arg0, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; strToUpperCase("abcDEFgHijk");<br>
 "ABCDEFGHIJK"<br>
<br>
</code></pre>
== substring() ==<br>
<br>
_*Description*_ Get a certain substring of a string, start from beginIdx , end to endIdx. If endIdx is not given or larger<br>
than the lenght of the string, return the substring from beginIdx to the end of the string.<br>
<br>
Usage:<br>
string substring(string val, int beginIdx, int endIndx ?);<br>
<br>
_*Parameters*_ (2 - 3 inputs)<br>
Input Types: ( arg0, required: schema any),( arg1, required: schema any),( arg2 = null: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; substring("I love the game", 2, 7);<br>
 "love"<br>
<br>
jaql&gt; substring("I love the game", 2);<br>
 "love the game"<br>
<br>
jaql&gt; substring("I love the game", 2, 20);<br>
 "love the game"<br>
<br>
</code></pre>
----<br>
=function=<br>
== fence() ==<br>
<br>
_*Description*_ evaluate a function in a separate process<br>
Usage:<br>
[T2] fence( [T1],  T2 fn(T1 x) );<br>
<br>
The fence function applies the function argument to each element of<br>
the input array to produce the output array. In particular, the fence<br>
function is evaluated in a separate process. A common use of fence is<br>
to shield the Jaql interpreter from user-defined functions that exhaust<br>
memory, for example.<br>
<br>
_*Parameters*_ (2 inputs)<br>
Input Types: ( arg0, required: schema any),( arg1, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; [1,2,3] -&gt; write(hdfs("test"));<br>
<br>
jaql&gt; read(hdfs("test")) -&gt; fence( fn(i) i + 1 );<br>
 [2,3,4]<br>
<br>
</code></pre>
== javaudf() ==<br>
<br>
_*Description*_ construct a jaql function from a given class<br>
Usage:<br>
fn javaudf( string className );<br>
<br>
The javaudf function constructs a function that knows how to evaluate itself<br>
given a className that specifies its body. The function can then be assigned<br>
to a variable (like any other value) and invoked (like any other function).<br>
This is the primary means by which users can supply user-defined functions.<br>
<br>
_*Parameters*_ (1 inputs)<br>
Input Types: ( arg0, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; split = javaudf("com.acme.extensions.fn.Split1"); // define the function and assign it to the variable split<br>
<br>
jaql&gt; path = '/home/mystuff/stuff';<br>
<br>
jaql&gt; split1(path, "/"); // invoke the split function<br>
<br>
</code></pre>
----<br>
=random=<br>
== randomDouble() ==<br>
<br>
_*Description*_ return a uniformly distributed double value between 0.0 (inclusive) and 1.0 (exclusive)<br>
Usage:<br>
double randomDouble( long? seed )<br>
<br>
The optional seed parameter is used to seed the internally used random number generator.<br>
<br>
Note: randomDouble will produce a pseudo-random sequence of doubles when called in sequence.<br>
If its called by multiple processes, in parallel (as done in MapReduce), then there are no<br>
guarantees (and in fact, if all sequential instances use the same seed, you'll get common<br>
prefixes).<br>
<br>
_*Parameters*_ (0 - 1 inputs)<br>
Input Types: ( arg0 = null: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
== randomLong() ==<br>
<br>
_*Description*_ return a uniformly distributed long value<br>
Usage:<br>
long randomLong( long? seed )<br>
<br>
The optional seed parameter is used to seed the internally used random number generator.<br>
<br>
Note: randomLong will produce a pseudo-random sequence of longs when called in sequence.<br>
If its called by multiple processes, in parallel (as done in MapReduce), then there are no<br>
guarantees (and in fact, if all sequential instances use the same seed, you'll get common<br>
prefixes).<br>
<br>
_*Parameters*_ (0 - 1 inputs)<br>
Input Types: ( arg0 = null: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
== registerRNG() ==<br>
<br>
_*Description*_ register a random number generator<br>
Usage:<br>
<br>
T registerRNG( T key, long seed );<br>
<br>
Register an RNG with a given name, key, and a seed.<br>
<br>
_*Parameters*_ (2 inputs)<br>
Input Types: ( arg0, required: schema any),( arg1, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; registerRNG('r', fn() 17);<br>
 "r"<br>
<br>
</code></pre>
== sample01RNG() ==<br>
<br>
_*Description*_ return a uniformly distributed double value between 0.0 (inclusive) and 1.0 (exclusive) from a registered RNG<br>
Usage:<br>
<br>
double sample01RNG( string key )<br>
<br>
An RNG associated with key must have been previously registered using registerRNG.<br>
<br>
_*Parameters*_ (1 inputs)<br>
Input Types: ( arg0, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; registerRNG('r', fn() 17);<br>
 "r"<br>
<br>
jaql&gt; sample01RNG('r');<br>
 0.6973704783607497<br>
<br>
</code></pre>
== sampleRNG() ==<br>
<br>
_*Description*_ return a uniformly distributed long value from a registered RNG<br>
Usage:<br>
<br>
long sampleRNG( string key )<br>
<br>
An RNG associated with key must have been previously registered using registerRNG.<br>
<br>
_*Parameters*_ (1 inputs)<br>
Input Types: ( arg0, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; registerRNG('r', fn() 17);<br>
 "r"<br>
<br>
jaql&gt; sampleRNG('r');<br>
 -4937981208836185383<br>
<br>
</code></pre>
== uuid() ==<br>
<br>
_*Description*_ Generate a type 4 UUID (random method)<br>
Usage:<br>
<br>
binary uuid()<br>
<br>
_*Parameters*_ (0 inputs)<br>
Input Types: {{{}}}<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; uuid();<br>
 hex('389878514428442CAE1B86033A32F249')<br>
<br>
</code></pre>
----<br>
=record=<br>
== arity() ==<br>
<br>
_*Description*_ Return the size of a record.<br>
<br>
Usage :<br>
long arity(record r);<br>
<br>
_*Parameters*_ (1 inputs)<br>
Input Types: ( arg0, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; arity({a:1,b:2,c:3});<br>
 3<br>
<br>
</code></pre>
== fields() ==<br>
<br>
_*Description*_ Convert each key-value pair of a record to a [key,value] array.<br>
<br>
Usage:<br>
array fields(record r)<br>
<br>
_*Parameters*_ (1 inputs)<br>
Input Types: ( arg0, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; fields({a:1, b:2, c:3});<br>
 [ ["a",1] , ["b",2] , ["c",3] ]<br>
<br>
jaql&gt; fields({a:1, b:2, c:3}) -&gt; transform $[0];<br>
 [ "a","b","c" ] //this example indicates a way to extract all the key values in a record.<br>
<br>
</code></pre>
== names() ==<br>
<br>
_*Description*_ Extract all the keys in a record and return as an array.<br>
names($rec) == for $k,$v in $rec return $k == fields($rec)[*][0];<br>
<br>
_*Parameters*_ (1 inputs)<br>
Input Types: ( arg0, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; names({a:1, b:2, c:3});<br>
 [ "a","b","c" ]<br>
<br>
</code></pre>
== record() ==<br>
<br>
_*Description*_ Convert a array to a single record.<br>
<br>
Usage:<br>
record record(array arr);<br>
<br>
the argument arr will be like [record1,record2,record3...], it has restricted format since a record can not contain any<br>
duplicate keys, so this function asserts record1, record2 ... contains no same keys.<br>
<br>
_*Parameters*_ (1 inputs)<br>
Input Types: ( arg0, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; record([{A:11},{B:22}]);<br>
 {<br>
  "A": 11,<br>
  "B": 22<br>
 }<br>
<br>
</code></pre>
== remap() ==<br>
<br>
_*Description*_ Join two records.<br>
<br>
Usage:<br>
record remap(record old, record new)<br>
<br>
remap joins two records, old and new together, produce a new record and return, remove duplicate key-values of old record.<br>
<br>
_*Parameters*_ (2 inputs)<br>
Input Types: ( arg0, required: schema any),( arg1, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; remap({a:1,b:2},{a:3,d:4});<br>
 {<br>
   "a" : 3, <br>
   "b" : 2,<br>
   "d" : 4<br>
 }<br>
<br>
</code></pre>
== removeFields() ==<br>
<br>
_*Description*_ Remove fields of a record by keys.<br>
<br>
Usage:<br>
record removeFields(record target, array names);<br>
<br>
names is an array with one or more string key names, removeFields will remove fields in target record if its key appears in names.<br>
<br>
_*Parameters*_ (2 inputs)<br>
Input Types: ( arg0, required: schema any),( arg1, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; removeFields({a:1,b:2},["a"]);<br>
 {<br>
  "b":2<br>
 }<br>
<br>
</code></pre>
== renameFields() ==<br>
<br>
_*Description*_ Replace the key of the target record with newName only if whose key equals with oldName.<br>
Usage:<br>
record renameFields(record target, record {oldName : newName , ...} );<br>
<br>
_*Parameters*_ (2 inputs)<br>
Input Types: ( arg0, required: schema any),( arg1, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; renameFields({a:1,b:2},{"a":"AAA" , "b":'BBB'});<br>
 {<br>
  "AAA": 1,<br>
  "BBB": 2<br>
 }<br>
<br>
</code></pre>
== replaceFields() ==<br>
<br>
_*Description*_ Replace fields in oldRec with fields in newRec only if the field name exists in oldRec.<br>
Unlike remap, this only replaces existing fields.<br>
<br>
Usage:<br>
<br>
record replaceFields( record old, record new);<br>
<br>
_*Parameters*_ (2 inputs)<br>
Input Types: ( arg0, required: schema any),( arg1, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; replaceFields( { a: 1, b: 2 }, { a: 10, c: 3 } );<br>
 {<br>
   "a": 10,<br>
   "b": 2<br>
 }<br>
<br>
</code></pre>
== values() ==<br>
<br>
_*Description*_ Extract all the values in a record and return as an array.<br>
values($rec) == for $k,$v in $rec return $v == fields($rec)[*][1];<br>
<br>
_*Parameters*_ (1 inputs)<br>
Input Types: ( arg0, required: schema any)<br>
<br>
_*Output*_ schema any<br>
<br>
_*Examples*_<br>
<pre><code>jaql&gt; values({a:1, b:2, c:3});<br>
  [ 1,2,3 ]<br>
<br>
</code></pre>
----