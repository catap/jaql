Jaql's types and schema language.

# Data Model #

Jaql's data model is based on JavaScript Object Notation or [JSON](http://www.json.org/).
When using literal values in the language (e.g., 42 or "a string" as used in the expressions x = 42 and y == "a string"), the values are specified as JSON. In addition, Jaql has extended JSON with several commonly needed data types. Thus, Jaql accepts valid JSON data but it may not produce valid JSON when non-JSON types are used.

Jaql also includes a powerful schema language to describe data. It includes ideas from [JSON Schema](http://json-schema.org/), [XML schema](http://www.w3.org/XML/Schema), and [RELAX NG](http://www.relaxng.org/), but tailored to Jaql's syntax. Jaql schema is used as a constraint on data and to improve efficiency where applicable.

Jaql's [types](#Types.md) are first described, followed by its [schema language](#Schema_Language.md).


# Types #
  * [Complex Types](#Complex_Types.md)
    * [array](#array.md)
    * [record](#record.md)
  * [Atomic Types](#Atomic_Types.md)
    * [null](#null.md)
    * [boolean](#boolean.md)
    * [string](#string.md)
    * [Numeric Types](#Numeric_Types.md)
      * [long](#long.md)
      * [double](#double.md)
      * [decfloat](#decfloat.md)
    * [binary](#binary.md)
    * [date](#date.md)
    * [schematype](#schematype.md)
    * [function](#function.md)
    * [comparator](#comparator.md)


## Complex Types ##
### array ###
An array is a list of values. It corresponds to JSON's array type.

Examples:
```
  // empty array
  []

  // array with three longs
  [ 1, 2, 3 ]

  // array with mixed atomic types
  [ 1, "a", 3 ]

  // array with nested, complex data
  [ 1, ["a", "b"], [["a", "b"]], ["a", ["b", ["c"]]], {name: "value"}, 2 ]
```

### record ###
A record is a mapping from names to values. It corresponds to JSON's object type.

Field names must be non-null strings.

Examples:
```
  // record with one field, whose name is "aName" and whose value is "val"
  { "aName": "val" }

  // jaql permits names to be specified with the double-quotes
  { aName: "val" }

  // a record with mixed atomic types
  { a: "val", b: 5 }

  // a record with a complex type for one of its fields
  { a: [1,2,3], b: "val" }
```

## Atomic Types ##
### null ###
Just like SQL as well as JSON, jaql's data model includes `null`.

Examples:
```
  // the null value
  null

  // the null value used within a record
  { a: null, b: 1 }
```

### boolean ###
The literal values for the boolean type are `true, false`. This is the same
as in JSON.

Examples:
```
  // the boolean value for TRUE
  true

  // the boolean value for FALSE
  false

  // an array with two boolean values included
  [ 1, true, 3, false, 4 ]
```

### string ###
Strings are specified much the same way as in JSON. The only exception is that jaql's
parser permits single quotes, in addition to the double quotes that are specified by
the JSON standard.

Examples:
```
  "some string"

  'some string'

  "some string with an \\n embedded newline"

  "some string with an embedded \\u1234 unicode char"
```
### Numeric Types ###
The numeric types that are supported include `long`, `double` and `decfloat` (e.g., Decimal). The `decfloat` type corresponds to JSON's numeric type whereas `long` and `double` are explicitly supported for performance and convenience.

### long ###
A 64-bit signed integer. If a number can be of type long, then it will be represented as a long by default.

Examples:
```
  1;

  -1;

  104;
```
### double ###
A 64-bit base-2 floating point value. If a number can be of type double, and is not a long, then it will be represented as a double by default. A number can be coerced to be a double by using a 'd' suffix.

Examples:
```
  1.0;

  3.5;

  3d;

  100e-2;
```
### decfloat ###
A 128-bit base-10 floating point value. A number can be specified to be a decimal only if suffixed by 'm'. The current implementation of decimal handling has lead us to use longs and doubles where possible.

Examples:
```
  1.0m;

  3.5m;

  3dm;

  100e-2m;
```
### binary ###
Binary values are represented as hexadecimal strings and constructed with the `hex` constructor. Note that binary values are provided as a convenience and are not directly supported by JSON.

Examples:
```
  hex('F0A0DDEE');
```
### date ###
Date values are represented using the following string format:
`"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"`. If an alternative format is needed,
the format can be specified as the second argument to `date`.

Examples:
```
  date('2001-07-04T12:08:56.000Z');
```
## schematype ##
Schemata are represented in Jaql with the `schema` type. The basic
pattern to follow is `schema <schema expression>`. What follows are
several simple examples of schema values; the section on [schema language](#Schema_Language.md) discusses `<schema expression>` options in more detail:

Examples:
```
  schema null;

  schema boolean;

  schema long|date;

  schema [ long, boolean * ];

  schema { a: long, b: null };

  schema schematype;
```
## function ##
Functions are part of Jaql's data types. They are specified using the following
pattern: `fn( <param>* ) <body>` where `<param>` is ` [schema] name[=val] `.
That is, a `param` can have an optional schema and an optional default
value associated. The `<body>` is defined by any Jaql expression. Since functions are simply values, they can be assigned to variables which can be invoked.

Examples:
```
  fn() 1+2; // creates a function value

  (fn() 1+2 )(); // invokes an anonymous function

  x = fn() 1+2; // creates a function and assigns it to the variable x

  x(); // invokes a function that returns 3

  y = fn(a,b) a + b;

  y(3,5);

  y = fn(schema long a, schema long b) a + b; // specify parameter schema

  y = fn(a=1, b=1) a + b; // specify default values

  y(); // invocation will use default values

  y(2); // bind 2 to a and use b's default value (yields 3)

  y(2,3); // override the default values

  y(b=2, a=3); // use the parameter name to explicitly bind to parameter value

  y = fn(schema long a=1, schema long b=1) a + b; // combine schemas and default values
```
## comparator ##
A comparator is similar to a function except that it is used specifically to construct a comparator. In particular, it is used by `sort` and `top`. The built-in function `topN` explicitly exposes comparators. For the most part, however, comparators are used for Jaql built-in and core operator implementations.

A comparator is specified as follows: `cmp( <param> ) [ <body> asc | desc ]`. Essentially, a comparator specifies how a single value (`param`) is to be transformed (`<body>`) and compared against other transformed values. The `asc` and `desc`
keywords determine whether the comparator can be used to sort in ascending or descending order.

Examples:
```
  cmp(x) [ x desc ]; // compares x values in descending order

  cmp(x) [ x.someField asc ]; // assumes x is a record and compares values associated with someField in ascending order
```

# Schema Language #
Jaql's schema language specifies the type for values. The type can be precise (e.g., x is a `long`), it can be entirely open (e.g., x is `any` type), or it can be partially specified (e.g., x is a ` { a: long, b: any, * } `). The example for partial specification can be read as: "x is a record with at least two fields, a and b, plus potentially other fields. Field a is a long and field b is of any type".

Schemas are used as constraints on the data and to optimize how data is processed and stored. In general, Jaql will have more opportunities for optimization when more detailed schema information is provided. However, there are many cases, in particular when exploring new data sets, where partial or no schema specification is more convenient.

The expression `schema <schema expression>` constructs a schema value. The `<schema expression>` is defined as follows:
```
  <schema expression> ::= <basic> '?'? ('|' <schema expression>)*
  <basic> ::= <atom> | <array> | <record> | 'nonull' | 'any'
  <atom>  ::= 'string' | 'double' | 'null' | ...
  <array> ::= '[' ( <schema expression (',' <schema expression>)* '...'?)? ']'
  <record> ::= '{' ( <field> (',' <field>)*)? '}'
  <field> ::= (<name> '?'? | '*') (':' <schema expression>)?
```

Essentially, a schema value is the OR of one or more schema values (e.g., ` long|string`. The `?` is used as a short-hand for the null schema.
So, `long?` really translates to `long|null`. In addition, some schema values support value-based constraints (e.g., `long(5)`. Below, we describe Jaql's schema types in detail:

  * [array](#Array_Schema.md)
  * [record](#Record_Schema.md)
  * [null](#Null_Schema.md)
  * [nonnull](#Nonnull_Schema.md)
  * [any](#Any_Schema.md)
  * [boolean](#Boolean_Schema.md)
  * [string](#String_Schema.md)
  * [long](#Long_Schema.md)
  * [double](#Double_Schema.md)
  * [decfloat](#Decfloat_Schema.md)
  * [binary](#Binary_Schema.md)
  * [date](#Date_Schema.md)
  * [schematype](#Schematype.md)
  * [function](#Function_Schema.md)

## Array Schema ##
An array can be described by constraining the types of its elements and their cardinality.
When an array's length is fixed, its called a **closed** array. Otherwise, the array's length is unbounded, in which case its referred to as an **open** array.

Example:
```
  schema []; // describes only the empty array

  schema [*]; // describes any array

  schema [ long(value=1), string(value="a"), boolean ]; // describes a closed array, in this case a triple, whose elements are constrained by the given types

  schema [ long, boolean * ]; // an open array where the first element is a long and the remaining elements, if present, must be booleans

  schema [ long, boolean, * ]; // and open array where the first element is a long, the second is a boolean, and the remaining elements are of any type
```
## Record Schema ##
A record is described by constraining its fields, which in turn are constrained on their name and type. Like arrays, there are **open** and **closed** records where the fields are either fixed or unbounded, respectively. In addition, fields can be specified to be optional (e.g., ` { a? } `).

Example:
```
  schema {}; // the empty record

  schema { a }; // a closed record with a field called "a" of any type

  schema { a? }; // the "a" field is optional

  schema { a: long }; // the "a" field must be of type long

  schema { a: long, b: null, c: nonnull }; // a closed record with 3 fields

  schema { * }; // an open record with any number of fields

  schema { a: long, * }; // an open record that must contain an "a" field of type long

  schema { a: long, *: long }; // a closed record whose second field can be named anything
```
## Null Schema ##
Matches only the null value.

Example:
```
  schema null;
```

## Nonnull Schema ##
Matches any type except for null.

Example:
```
  schema nonnull;
```

## Any Schema ##
Matches any type, including null.

Example:
```
  schema any;
```
## Boolean Schema ##
A value can be constrained to be of type boolean and it can also have its value
constrained to either true or false.

Example:
```
  schema boolean;

  schema boolean(true);
```
## String Schema ##
A string value can be constrained by its length or a specific value.

Example:
```
  schema string;
  
  schema string(5); // matches any string of length 5

  schema string(value="baab");
```
## Long Schema ##

Example:
```
  schema long;

  schema long(5);
```
## Double Schema ##

Example:
```
  schema double;

  schema double(5d);
```
## Decfloat Schema ##

Example:
```
  schema decfloat;

  schema decfloat(5m);
```
## Binary Schema ##
Like strings, binary values can be constrained by their length or a specific value.

Example:
```
  schema binary;

  schema binary(5);

  schema binary(value=hex('001122'));
```
## Date Schema ##

Example:
```
  schema date;

  schema date(date('2000-01-01T12:00:00Z'));
```
## Schematype Schema ##

Example:
```
  schema schematype;

  schema schematype(value=schema long);
```
## Function Schema ##

Example:
```
  schema function;
```

Examples:
```

```

Finally, schema values can be used in Jaql scripts as constraints on the data.