# Introduction #

# Running Jaql in Parallel #

In this section, we show how Jaql queries are evaluated in parallel using
Hadoop's map/reduce. Jaql includes a function that directly wraps Hadoop's
map/reduce, called `mapReduce()`.
It takes a Jaql description of
the map and reduce functions as input, and uses it to run a map/reduce
job in Hadoop.

```
// Write to an HDFS file called 'sample'.
  [
    {x: 0, text: "zero"},
    {x: 1, text: "one"},
    {x: 0, text: "two"},
    {x: 1, text: "three"},
    {x: 0, text: "four"},
    {x: 1, text: "five"},
    {x: 0, text: "six"},
    {x: 1, text: "seven"},
    {x: 0, text: "eight"}
  ]
  -> write(hdfs("sample.dat"));
	
  // Run a map/reduce job that counts the number objects
  // for each 'x' value.
  mapReduce( 
    { input:  {type: "hdfs", location: "sample.dat"}, 
      output: {type: "hdfs", location: "results.dat"}, 
      map:    fn($v) ( $v -> transform [$.x, 1] ),
      reduce: fn($x, $v) ( $v -> aggregate into {x: $x, num: count($)} )
    });
	
  // Read the results...
  read(hdfs("results.dat"));
	
  // result...
  [
    {x: 0, num: 5},
    {x: 1, num: 4}
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
namely that Jaql is a higher-order language.
This allows function definitions to be assigned to variables
and later evaluated. In mapReduce(), the map and reduce phases are specified
using Jaql functions. Each mapper and reducer that is spawned for a Jaql job
uses a Jaql interpreter to evaluate the respective jaql map and reduce function.

The mapReduce() is useful for programmers who want to run a map/reduce job
over JSON data and exploit the expressive power of Jaql yet free themselves
from all the little details required to actually set up and run a map/reduce job. However, we typically write queries using the higher level operators (e.g., `for, group, sort, ...`) and let the Jaql rewriter transform the declarative queries into (possiblly multiple) mapReduce() calls. A simple example is Query 1-- a scan (e.g., `for`) is readily translated into a map-only job as follows:

```
  // Query 1. Return the publisher and title of each book.
  read(hdfs("books"))
  -> transform {$.publisher, $.title};


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

All of the other example queries listed here are rewritten into mapReduce() function calls and evaluated as map-reduce jobs. For example, `group` is translated to a mapReduce() that includes both a map and reduce function. Similarly, `join` is translated to `group`, which is then translated to mapReduce(). If the output of a `for` loop is `grouped`, both are translated into a single mapReduce(). For grouping, if additional information about the aggregate function is provided, Jaql can automatically rewrite queries so that they can take advantage of map/reduce's `combine`. Please refer to the discussion on [aggregate functions](Functions.md) to see how you can define your own, plug them into Jaql, and how they're evaluated in parallel.