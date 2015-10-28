# Introduction #

Jaql is a functional language that is specialized for processing large collections of semi-structured data. The DataModel describes the values, as well as the schema constraints that are supported by Jaql. Every type has a textual representation that is based on JSON. There are many other representations available for (de)serializing values to and from byte streams (see [IO](IO.md)). The textual representation is used as a convenient way to show results to the user in the shell and it is used to construct new values:

```
  // 1. print the result of the function 'range'
  jaql> range(1,3);
  [ 1,2,3 ]

  // 2. make a new array of records
  jaql> [ { a: 1 }, { a: 2 }, { a: 3 } ];
  [ { a: 1 }, { a: 2 }, { a: 3 } ]

  // 3. do the same by creating a record for each input element
  jaql> range(1,3) -> transform { a: $ };
  [ { a: 1 }, { a: 2 }, { a: 3 } ]
```

The example above also shows how to call a function (`range`)
and how to operate on collections, which are interchangeably referred to as arrays (`transform`). For a comprehensive list of functions
that Jaql supports, see [builtins](Builtin_functions.md) (and [experimental builtins](Builtin_functions_experimental.md)). For a comprehensive list of operators that are specifically designed to process large arrays, please see LanguageCore. If further extensions are required, Jaql can easily be
extended with Jaql [Functions](Functions.md), Java functions, or arbitrary [executables](Builtin_functions#externalfn().md). Such extensions can be bundled into separate Jaql scripts called [modules](Modules.md) which can be re-used.

In this document, we review Jaql's top-level statements and expressions.

# Top-level Statements #

  * Import
  * Assignment
  * Expression
  * Explain
  * Quit

# Import #
The `import` statement is used for loading modules into the current session's context. Following the `import` statement, all of the imported module's variables can be used. See [modules](Modules.md) for more information.

# Assignment #

Jaql allows variables to be declared and assigned to either expressions or values. If a variable is assigned to an expression, there is no guarantee as to whether that expression has been evaluated (lazy evaluation or views). If a variable is assigned to a value, that value is materialized and stored in the session process's memory. Note that value assignment is only supported as a top-level statement; they are not permitted within function blocks, for example. Here are some examples:

```
  // assign a to the 'read' expression. no data is actually read.
  a = read(del("very_big_file"));

  // assign b to the value 1.
  b := 1;
```

If a variable is assigned to another variable, e.g., `x = y;`, `x` is assigned to the value or expression that `y` references.
For example:

```
  x = 1;
  y = x;
  x = 2;
  y; // prints out 1
  x; // prints out 2
```

In addition, Jaql supports `extern` variables that can be undefined. If an undefined variable is used, an error is thrown.

```
  extern x := 1;
```

Finally, types can be specified for variables:

```
  x: string = "foo";

  x: long := 1;
```

# Expression #
Every statement that is evaluable, thus produces a value, is an
expression. Examples include [IO](IO.md) functions (e.g., `read,write`),
[builtin](Builtin_functions.md) functions, and [core](LanguageCore.md) operators that can scalably manipulate large arrays. In this section, we review all of the other expressions that are packaged with Jaql.

## Constructors ##
All expressions that create values are referred to as constructors.
For example, the literal `"Hello, world!"` creates a string. Refer to the DataModel sections for more examples. Here, we review the non-trivial, complex value constructors for records and arrays.

### Record ###

Record values are constructed using braces {}. Literal records in JSON notation may be used.
The quotes for field names may be omitted if the field name looks like an identifier, i.e. does not contain spaces or symbols.

Instead of literal field names and field values, nested subexpressions may be used. If a field name or field name expression is followed by ?, the field is only added to the record if its value is not null.

For convenience, a number of syntactic shortcuts for common constructs are provided.

  * If an in-scope variable is used in place of a field definition, a field is added to the record with the variable name as field name, and the variable value as field value.
  * If a [path](Path_Expressions.md) is used as a field definition, the resulting field will have the name of the final step of the path.
  * A path expression that returns a record and is followed by `.*` will cause all fields of that record  to be copied to the constructed record.
  * If the : in one or more field definitions is followed by `flatten`, an array of records will be produced that contains one record for each element of the cross product of the `flatten`ed values. A record expression like this ` { ..., f1: flatten e1, f2 : flatten e2, ... , fn: flatten en,...` is equivalent to ` for( v1 in e1, v2 in e2, ..., vn in en) {..., f1: v1, f2: v2, ..., fn: vn,...} `.

```
// literal record
{ x: "value", y: 12, "field with spaces" : 34.2}
// record with subexpressions for field values
basename="test";
{ filename: basename+".txt", size: count(somevariable)};
// record with subexpressions for field names
{ ("a"+"field"):  null, (basename) : "hi" }
// omit null fields (yields {required: null} as a result)
{ required: null, optional?: null}
// variable as field (yields { basename: "test"}
{basename} 
// path expressions
x={a: "value", b: "value"};
{ x.a } // copy field a from x
{x.*} // copy all fields from x
```

### Array ###
Array values are constructed using braces `[]`. Each element of the array is an expression, much as the field values for records are expressions.

## Arithmetic Expressions ##
Jaql includes special syntax for several commonly used arithmetic operators
(`+,-,*,/`).

```
  1+1;

  (1+2)/3;
```

## Boolean Expressions ##
Jaql includes special syntax for constructing boolean expressions and testing for null values. Note that three-level logic is used to incorporate null values into boolean expressions.

```
  x = true;
  y = false;
  z = null;

  x and not y;
  x or y;
  isnull z;
  not isnull z;
```

Jaql supports value-based (in)equality tests as well as an `in` predicate for testing inclusion:

```
  "foo" == "foo";
  "foo" in [ "foo", "bar" ];
  "foo" != "bar";
  3 < 4;
  3 <= 2;
  8 > 3 >= 1;
```


## Path Expressions ##
Jaql includes a special syntax for manipulating records and arrays:

```
  r = { a: 1, b: 2, c: 3 };
  a = [ 1,2,3 ];
  ar = [ { a: 1, b: 2 }, { a: 10, b: 20 } ];

  // record projection
  //
  r.a; // returns 1

  r{.a,.b} // returns {a:1, b:2}

  // array element access
  //
  a[1]; // returns 2

  a[1:2]; // return [2,3]

  // projecting from an array
  //
  ar[*].a; // [ 1, 10 ]

  ar[*]{.a}; // [ { a: 1 }, { a: 10 } ]

  // manipulating fields in a record
  // 
  
  // create a new record that has r's fields and a 'd' field
  { r.*, d:4 }; // { a: 1, b: 2, c: 3, d: 4 }

  // create a new record that has r's field, except 'b', and a 'd' field
  { r{* - .b}, d: 4 }; // { a: 1, c: 3, d: 4 }
```

## Blocks ##
Jaql supports a block that defines a local scope. A block permits some number of expression assignments, followed by an expression. The return value of the block is the value of that last expression. The block expression is comparable to the `let` expression in XQuery. Note that instead of `;` to terminate assignments, a `,` is used. In addition, Jaql does respect the order in which the assignments are specified.

```
  // top-level block. The local 'a' will override any global 'a'
  a = 10;
  (
    a = 1,
    b = a + 2,
    b + 1
   );
   // returns 4

  // a nested block used to filter a nested array
  test = [ { a: 1, b: [ 1,2,3 ] }, { a: 2, b: [ 2,3,4 ] } ];
  test -> transform each r (
            f = r.b -> filter $ > 2,
            { r{* - .b}, gtTwo: f }
          );
  // returns [ { a: 1, gtTwo: [3] }, { a: 2, gtTwo: [3,4] } ]
```

## If-Then-Else ##
For basic control flow, Jaql includes `if, else`:

```
  x = 2;
  if( x > 2 ) "hi" else "bye";
  // returns "bye"
```

## Pipe and Function ##

Jaql includes a special operator `->`, referred to as a pipe
symbol since it is intended to convey that data flows from the left-hand-side to the right-hand-side. For example:

```
  jaql> range(1,3);
  [1,2,3]

  // the range function flows its values into the transform operator
  jaql> range(1,3) -> transform $ + 1;
  [2,3,4]
```

The `->` symbol is also used to syntactically convert function composition into a form that appears closer to a data flow. With a `->`, functions that produce data for other functions appear to
the left.

```
  // function 1
  f1 = fn(x,y) x + y;
  f2 = fn(z,zz) z + zz + 10;

  // classic function composition
  f2(f1(2,3),1);
  // returns 16

  // using ->, f1 looks like its called first and produces data for f2
  f1(2,3) -> f2(1);
  //returns 16
```

Note that `->` binds the output of its left-hand-side to the first function parameter on the right-hand-side. Thus, even though `f2` was declared to have two parameters, it appears to only have one, since its first parameter was implicitly bound.

## Partitioned Array Expressions ##

See [LanguageCore](LanguageCore.md).

# Explain #
If any expression is preceded by `explain`, Jaql prints out a lower level plan that results from automatic rewrites. Using `explain`, one can determine, for example, if a map-reduce based plan was found.

```
  // explain an expression 
  //
  explain read(hdfs("test")) -> transform $ + 1;
  // the output below shows a mapReduce job which is a map-only job.
  (
  $fd_0 = system::mapReduce({ ("input"):(system::const({
    "type": "hdfs",
    "location": "test"
  })), ("map"):(fn(schema [ * ] $mapIn) ($mapIn
-> transform each $ (($)+(1))
-> transform each $fv ([null, $fv]))), ("schema"):(system::const({
    "key": schema null,
    "value": schema long | double | decfloat | null
  })), ("output"):(system::HadoopTemp(schema=schema long | double | decfloat | null)) }),
  system::read($fd_0)
)
;

```

# Quit #
Exit the interpreter.