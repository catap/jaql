# Introduction #

In this document, we introduce Jaql, a query language for
JavaScript Object Notation or [JSON](http://www.json.org/).
Although Jaql has been designed specifically for JSON,
we have tried to borrow some of the best features of
[SQL](http://en.wikipedia.org/wiki/SQL), [XQuery](http://www.w3.org/TR/xquery/), LISP, and
[PigLatin](http://wiki.apache.org/pig/PigLatin).
Our high-level design objectives include:

  * Semi-structured analytics: easy manipulation and analysis of JSON data
  * Parallelism: Jaql queries that process large amounts of data must be able to take advantage of scaled-out architectures
  * Extensibility: users must be able to easily extend Jaql

We begin with an example of JSON data, then go on to several
examples using the data to illustrate key features of Jaql
and show how it can be used to process
JSON data in parallel using Hadoop's map/reduce framework.
Along the way, we will refer to Jaql's [core operators](LanguageCore.md),
[builtin functions](Builtin_functions.md) and show how Jaql's [function](Functions.md)
and [IO](IO.md) libraries can be extended. To get started with the examples below,
you can [download](http://jaql.googlecode.com/files/jaql-0.5.1_12_07_2010.tgz) the most recent release of Jaql and fire up a Jaql interpreter following these [instructions](Running.md).

_Note:_ Jaql is still in early development,
so beware that it is likely to change over the next few months.
The future development plans are outlined in the [roadmap](RoadMap.md).

# A JSON Example #

Let's start off with an example of an application log:

```
%cat log.json
[
  { from: 101, 
    to: [102],
    ts: 1243361567,
    msg: "Hello, world!" 
  },
  { from: 201, 
    to: [20, 81, 94],
    ts: 1243361567,
    msg: "Hello, world! was interesting, but lets start a new topic please" 
  },
  { from: 81, 
    to: [201, 94, 40],
    ts: 1243361567,
    msg: "Agreed, this topic is not for Joe, but more suitable for Ann" 
  },
  { from: 40, 
    to: [201, 81, 94],
    ts: 1243361567,
    msg: "Thanks for including me on this topic about nothing... reminds me of a Seinfeld episode." 
  },
  { from: 20, 
    to: [81, 201, 94],
    ts: 1243361567,
    msg: "Where did the topic go.. hopefully its more than about nothing." 
  }  
]
```

and its associated user data:

```
% cat user.json
[
  { id: 20,
    name: "Joe Smith",
    zip: 95120
  },
  { id: 40,
    name: "Ann Jones",
    zip: 94114
  },
  { id: 101,
    name: "Alicia Fox",
    zip: 95008
  },
  { id: 201,
    name: "Mike James",
    zip: 94114
  },
  { id: 102,
    name: "Adam Baker",
    zip: 94114
  },
  { id: 81,
    name: "Beth Charles",
    zip: 95008
  },
  { id: 94,
    name: "Charles Dodd",
    zip: 95120
  },
  { id: 103,
    name: "Dan Epstein",
    zip: 95008
  }
]
```

Both data sets are assumed to be stored in separate, local files. Each file consists
of an _array_, delimited by brackets '[ ]', of JSON objects (or records), delimited by braces '{ }'. Objects contain name:value pairs or _members_, where the
value can be an atomic type or a nested value.

_Note:_ while Jaql can consume strict JSON, it can also consume minor variants of JSON. In the examples, for the sake of clarity, we omit double-quotes around field names when writing Jaql queries. Jaql's output, however, will quote field names and thus produces valid JSON.

# The Jaql Query Language #

Jaql is a functional query language that provides users with a simple,
declarative syntax to do things like filter, join, and group JSON data.
Jaql also allows user-defined functions to be written and used in expressions.
Let's begin with some examples using the log and user data presented earlier.
Our first task is to load the locally stored JSON data into Hadoop's file system, HDFS
(We store data in HDFS since it can processed in parallel using map-reduce).

```
  // load log data
  read(file("log.json")) -> write(hdfs("log"));

  // load user data
  read(file("user.json")) -> write(hdfs("user"));
```

**read** is an example of a _source_ whereas **write** is an example of a _sink_. In the example, we've constructed a simple pipe to read from a local file and write to an
hdfs file. For a more comprehensive discussion on how sources and sinks can be parameterized and extended, refer to [IO](IO.md).

Our first query illustrates simple filtering and projection:

```
  //
  // Bind to variable
  $log  = read(hdfs("log"));
  $user = read(hdfs("user"));

  //
  // Query 1: filter and transform
  $log
  -> filter $.from == 101
  -> transform { mandatory: $.msg };

  // result ...
  [
    {
      "mandatory": "Hello, world!"
    }
  ]
```

First we assign a variable to both the log and user data in hdfs.
Then, we start a new Jaql pipe with the log data. A pipe expects an
array as input and conceptually streams the array's values to its consumer.
In Query 1, the first consumer is a **filter** operator that outputs only those
values for which the predicate evaluates to true. In this case, the predicate
tests the **from** field of each object. The **$** is an implicitly defined variable
that references the _current_ array value. Note that the **.** used in **$.from** assumes
that each array value is an object and not some other type of JSON value.

The second consumer is a **transform** operator that takes as input a JSON value
and outputs a JSON value. In this case, a new object is constructed to project the
**msg** field. The sink for Query1 writes to the screen. For more details on Jaql's core language features, refer to [LanguageCore](LanguageCore.md).

While Query 1 is reasonably straightforward, it is not reusable. For every combination of
source, sink, and id (e.g., from), a new Jaql pipe must be defined. Jaql supports functions in the language in order to increase re-usability. Consider defining Query 1 as a function, then calling it:

```
  //
  // Define Query 1 as a function
  $introMessage = 
  fn($input, $id) (
    $input
    -> filter $.from == $id
    -> transform { mandatory: $.msg }
  );

  // Call Query1
  read(hdfs("log")) 
  -> $introMessage(101);

  // result ...
  [
    {
      "mandatory": "Hello, world!"
    }
  ]
```

The first statement defines the variable **$introMessage** as a function of two arguments, namely an **$input** and an **$id** to use for the filter. The second statement shows how to call the newly defined function in the context of a pipe. Log data is used as a source and is implicitly bound to the first agument of the $introMessage function ($input). Using functions allows us to parameterize Query1's filter, use any source that may store log data, and use any sink to consume its output. The following example uses an hdfs sink instead of the screen:

```
  read(hdfs("log"))
  -> $introMessage(101)
  -> write(hdfs("greeting"));
  
  read(hdfs("greeting"));

  // result ...
  [
    {
      "mandatory": "Hello, world!"
    }
  ]
```

The remaining examples are organized around the task of aggregating particular word mentions in the log messages by the sender's zip-code. We start with simple **word-count**, then describe several add-ons such as filtering by a list of words, aggregating the filtered word-counts by sender, and finally correlating senders with their zip-codes to aggregate by zip-code.

In Query 2, we show how to compute **word-count**, the canonical map-reduce example, on the log **msg** field. First we introduce the **expand** operators which will be used to create a single array of strings, and distinguish it from the **transform** operator that we saw earlier:

```
  $example = [ 
    ["first", "example"],
    ["second", "example"]
  ];
  $example -> transform $;
  // results ...
  [
    [
      "first",
      "example"
    ],
    [
      "second",
      "example"
    ]
  ]

  $example -> expand $;
  // results ...
  [
    "first",
    "example",
    "second",
    "example"
  ]
```

**$example** is an array of array of strings, exactly what tokenization will produce for each **msg**. The **transform** operators takes each array child as input, in this case a nested array, and places a single JSON value back into the output array. This leaves us with a nested array of strings whereas word-count operates on a single array of strings. The **expand** operator in contrast expects an array as input, in this case the nested array, and places each of the nested array's children into the output array, thus producing a single array of strings. The following shows how **expand** is used in conjunction with a simple tokenizer that is implemented as the user-defined function (UDF)**splitArr**:

```

  // register the splitArr function
  splitArr = builtin("com.acme.extensions.expr.SplitIterExpr$Descriptor");
  splitArr("something simple with five words", " ");

  // results ...
  [
    "something",
    "simple",
    "with",
    "five",
    "words"
  ]

  // Query 2: word count across all messages
  $log
  -> expand splitArr($.msg, " ")
  -> group by $word = $
      into { $word, num: count($) };

  // results ...
  [
    {
      "num": 1,
      "word": "Agreed,"
    },
    {
      "num": 1,
      "word": "Ann"
    },
    {
      "num": 2,
      "word": "Hello,"
    },
    {
      "num": 1,
      "word": "Joe,"
    },
    {
      "num": 1,
      "word": "Seinfeld"
    },
    ...
  ]
```

The UDF is first registered by associating a name with its implementing class. More details on extending Jaql with UDF's and other functions can be found in [Functions](Functions.md). Following, we exercise **splitArr** on a sample string using a simple delimiter. Finally, we implement word-count by reading the log data, tokenizing each **$.msg**, expanding the tokens into the output array, grouping on word, computing a **count** per word, and outputing an object for each word and its associated count.

Jaql evaluates this query by translating to map-reduce. The read, tokenization, and expansion are performed in the map step and the grouping and count is performed in the reduce step. Since count is an algebraic function, Jaql pushes its computation into the map step and performs a sum in the reduce step, i.e., it takes advantage of map-reduce's combiners where applicable. For more details on how Jaql exploits map-reduce, refer to [PlanLanguage](PlanLanguage.md).

Query 3 slightly modifies Query 2 by adding a filter on the words that are returned:

```
  $inList = ["topic", "Seinfeld"];

  // Query 3: word-count for specific words
  $log
  -> expand (splitArr($.msg, " ") -> filter $ in $inList )
  -> group by $word = $
      into { $word, num: count($) };
  
  // results ...
  [
    {
      "num": 1,
      "word": "Seinfeld"
    },
    {
      "num": 4,
      "word": "topic"
    }
  ]
```

This example illustrates a **sub-pipe**, that is a pipe that is nested under another pipe, in this case, the outer pipe. Tokenization produces an array so is used as the source of the sub-pipe, which simply filters the words according to a simple in-list of words.

Query 4 extends Query 3 by computing word-count per message sender, instead of a global word-count.

```
  //
  // Query 4: word-count for specific words per sender
  $log
  -> expand each $l
       ( splitArr($l.msg, " ") -> filter $ in $inList
                               -> transform { $l.from, word: $ } )
  -> group by $uword = $
       into { $uword.from, $uword.word, num: count($) };
  
  // results ...
  [
    {
      "from": 20,
      "num": 1,
      "word": "topic"
    },
    {
      "from": 40,
      "num": 1,
      "word": "Seinfeld"
    },
    {
      "from": 40,
      "num": 1,
      "word": "topic"
    },
    {
      "from": 81,
      "num": 1,
      "word": "topic"
    },
    {
      "from": 201,
      "num": 1,
      "word": "topic"
    }
  ]
```

The sub-pipe is extended to include a transform that associates the sender id with each filtered word. Note that the sender id is in the JSON object on the outer pipe but **splitArr** "loses" this association by only examining **msg**. As a result, we need to explicitly construct an association of **from** from the outer object with each word in order to later group by **from**. The **each $l** syntax for **expand** saves the outer object so that it can be referenced in the sub-pipe. This is similar to an iteration variable one is familar with in **for-loops**.

Once word-count is defined per user, we can associate zip code information by joining with the user data, as shown in Query 5.

```
  $senderWordCount = 
  $log
  -> expand each $l
       ( splitArr($l.msg, " ") -> filter $ in $inList
                               -> transform { $l.from, word: $ } )
  -> group by $uword = $
       into { $uword.from, $uword.word, num: count($) };

  // Query 5: segment by zip which requires a join with the user data
  join $senderWordCount, $user
  where $senderWordCount.from == $user.id
    into { $senderWordCount.*, $user.zip }
  -> group by $g = {$.zip, $.word}
       into { $g.zip, $g.word, num: sum($[*].num) };    
  
  // results ...
  [
    {
      "num": 1,
      "word": "Seinfeld",
      "zip": 94114
    },
    {
      "num": 2,
      "word": "topic",
      "zip": 94114
    },
    {
      "num": 1,
      "word": "topic",
      "zip": 95008
    },
    {
      "num": 1,
      "word": "topic",
      "zip": 95120
    }
  ]
```

Query 5 joins the senderWordCount result with user information on id, groups on zip and word, then calculates the total number of times a word is mentioned in a given zip.