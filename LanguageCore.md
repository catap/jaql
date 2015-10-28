# Introduction #
Jaql is built from several core expressions that are designed to operate on large arrays through parallelization. All of the core expressions can operate on nested arrays, but its the large, top-level arrays that are in particular targeted for scalable processing.

  * [Filter](#Filter.md)
  * [Transform](#Transform.md)
  * [Expand](#Expand.md)
  * [Group](#Group.md)
  * [Join](#Join.md)
  * [Union](#Union.md)
  * [Tee](#Tee.md)
  * [Sort](#Sort.md)
  * [Top](#Top.md)

# Filter #
The `filter` expression filters away elements from its input array. It takes as input an array of elements of type `T` and outputs an array of the same type, retaining those elements for which a predicate evaluates to `true`. It is Jaql's equivalent to SQL's `WHERE` clause.

## Syntax ##
```
  A -> filter <predicate> ;
```

  * input: A of type `[ T ]` (e.g., array of type `T`)
  * output: A' of type `[ T ]`, `count(A') <= count(A)`
  * `<predicate>`: expression that returns a `boolean`
  * `filter` binds a default iteration variable, `$`, that is bound to each element of the input. The type of `$` is `T` and it is often used as input to `<predicate>`.
  * `<predicate>` is often expressed using standard boolean operators such as `==, !=, >, >=, <, <=, not, and, or`.

As with most of the core expressions, the default iteration variable `$` can be renamed as follows:

```
  A -> filter each <var> <predicate> ;
```

  * `<var>` is now the iteration variable, replacing the role of `$`

## Example ##
```
jaql> employees = [
 {name: "Jon Doe", income: 20000, mgr: false},
 {name: "Vince Wayne", income: 32500, mgr: false},
 {name: "Jane Dean", income: 72000, mgr: true},
 {name: "Alex Smith", income: 25000, mgr: false}
];

// use $ as the iteration variable
jaql> employees -> filter $.mgr or $.income > 30000;
[
  {
    "income": 32500,
    "mgr": false,
    "name": "Vince Wayne"
  },
  {
    "income": 72000,
    "mgr": true,
    "name": "Jane Dean"
  }
]

// use 'emp' as the iteration variable
jaql> employees -> filter each emp emp.mgr or emp.income > 30000;
[
  {
    "income": 32500,
    "mgr": false,
    "name": "Vince Wayne"
  },
  {
    "income": 72000,
    "mgr": true,
    "name": "Jane Dean"
  }
]

// Use parens to separate the variable declaration from the <predicate>
// to make the statement easier to read
employees -> filter each emp (emp.mgr or emp.income > 30000);
```


# Transform #
The `transform` expression transforms each element of its input array. It takes as input an array of type `T1` and outputs an array of the same size of type `T2`. The `transform` expression is Jaql's syntax for **map** or SQL's `SELECT` clause.

## Syntax ##
```
  A -> transform <expr> ;
```

  * input: A of type `[ T1 ]` (e.g., array of type `T1`)
  * output: A' of type `[ T2 ]`, `count(A') == count(A)`
  * `<expr>`: expression that returns type `T2`
  * `transform` binds a default iteration variable, `$`, that is bound to each element of the input. The type of `$` is `T1` and it is often used as input to `<expr>`.
  * `<expr>` is often expressed using type constructors such as record `{ ... }` and array `[ ... ]` constructors.

```
  A -> transform each <var> <expr> ;
```

  * `<var>` is now the iteration variable, replacing the role of `$`

The input will be processed item by item. The variable $ will be bound to each item the input, so you can access values from records with `$.key` or from arrays with `$[n]`. When copying a value from a record, you can omit the key. It will automatically be copied to the result.

## Example ##
```
jaql> recs = [
  {a: 1, b: 4},
  {a: 2, b: 5},
  {a: -1, b: 4}
];

// use $ as the iteration variable. Construct an output record of type { sum: long }
// from each input record, of type { a: long, b: long }
jaql> recs -> transform {sum: $.a + $.b};
[
  {
    "sum": 5
  },
  {
    "sum": 7
  },
  {
    "sum": 3
  }
]

// use 'r' as the iteration variable
jaql> recs -> transform each r {sum: r.a + r.b};
[
  {
    "sum": 5
  },
  {
    "sum": 7
  },
  {
    "sum": 3
  }
]
```

# Expand #
The `expand` expression flattens nested arrays. It takes as input an array of nested arrays `[ [ T ] ]` and produces an output array `[ T ]`, by promoting the elements of each nested array to the top-level output array. In addition, `expand` can be used in a manner similar to `transform`, whereby an expression is applied to each nested array. Note that the expression must return an array. For such use, `expand` takes as input `[ [ T1 ] ]` and outputs `[ T2 ]`.

## Syntax ##
```
  A -> expand ;
```
  * input: A of type `[ [ T ] ]` (e.g., array of nested arrays of type `[ T ]`)
  * output: A' of type `[ T ]`

```
  A -> expand <expr>
```
  * input: A of type `[ [ T1 ] ]` (e.g., array of nested arrays of type `[ T1 ]`)
  * output: A' of type `[ T2 ]`
  * `<expr>`: expression that returns type `[ T2 ]`
  * `expand` binds a default iteration variable, `$`, that is bound to each element of the input. The type of `$` is `[ T1 ]` and it is often used as input to `<expr>`.
  * a common `<expr>` used with `expand` is `unroll`, which repeats the parent of a nested array for each element of the nested array.

```
  A -> expand each <var> <expr>
```

  * `<var>` is now the iteration variable, replacing the role of `$`

## Example ##
```
jaql> nestedData = [
  [3,65,8,72],
  [5,98,2,65]
];

// flatten the nested array
jaql> nestedData -> expand;
[
  3,
  65,
  8,
  72,
  5,
  98,
  2,
  65
]

// flatten and transform the nested data
// the first $ is bound to each nested array
// the second $ is bound to each element of each nested array
jaql> nestedData -> expand ($ -> transform $ * 2);
[
  6,
  130,
  16,
  144,
  10,
  196,
  4,
  130
]

// repeat the expression above, but with explicitly declared iteration variables
jaql> nestedData -> expand each arr (arr -> transform each n (n * 2));
[
  6,
  130,
  16,
  144,
  10,
  196,
  4,
  130

// alternatively, expand composes with transform
jaql> nestedData -> expand -> transform $ * 2;
[
  6,
  130,
  16,
  144,
  10,
  196,
  4,
  130
]

// example of projecting and expanding arrays that are nested in records
jaql> moviesOwned = [
  {name:"Jon Doe", movie_ids:[3,65,8,72]}, 
  {name:"Jane Dean", movie_ids:[5,98,2]}
];

jaql> moviesOwned -> expand $.movie_ids;
[
  3,
  65,
  8,
  72,
  5,
  98,
  2
]

// example of unroll (multiply each parent by the arity of its nested child array)
moviesOwned -> expand unroll $.movie_ids;
[
  {
    "movie_ids": 3,
    "name": "Jon Doe"
  },
  {
    "movie_ids": 65,
    "name": "Jon Doe"
  },
  {
    "movie_ids": 8,
    "name": "Jon Doe"
  },
  {
    "movie_ids": 72,
    "name": "Jon Doe"
  },
  {
    "movie_ids": 5,
    "name": "Jane Dean"
  },
  {
    "movie_ids": 98,
    "name": "Jane Dean"
  },
  {
    "movie_ids": 2,
    "name": "Jane Dean"
  }
]
```

# Group #

The `group` expression groups one or more input arrays on a grouping key and creates an array
with one item for each group. The expression transforming a group into a result item may apply aggregate functions such as sum() or count().

> Jaql's `group` expression is similar to SQL's `GROUP BY` clause when specified for a single input array. When multiple input arrays are specified, `group` is similar to PigLatin's co-group operator.

When evaluated by a MapReduce job, a `group` expression's grouping key is extracted in the map phase and the per-group expression is evaluated in the reduce phase.

If the per-group expression uses aggregate functions that are algebraic, they are evaluated using the map, combine, and reduce phases.
The combine phase computes partial aggregates which allows more computation to take place in the mapper processes, potentially reducing network traffic when transferring intermediate data between mapper and reducer processes.

## Syntax ##

Single input, create single, global aggregate:
```
  A -> group into <aggrExpr> ;
```

  * input: A of type `[ T1 ]` (e.g., array of type `T1`)
  * output: A' of type [T2 ](.md)
  * `group` binds the entire input to `$` and places it in scope for `<aggrExpr>`
  * `<aggrExpr>` produces an output of type `T2`

Single input, partition into groups, and apply a function group:
```
  A -> group by <groupKeyVar> = <groupExpr> into <aggrExpr> ;
```

  * `<groupKeyVar>` is a variable that references the grouping key for a group. It is in scope for `<aggrExpr>`.
  * `<groupExpr>` is an expression that extracts a grouping key from each element in `A`. As with the other core operators, the `group` expression defines `$` as the iteration variable, binding it to each element of `A`. Thus, `$` is in scope for `<groupExpr>`.
  * `group` creates arrays of elements, `[ T ]` that are associated with each group.
  * `<aggrExpr>` is invoked per group. Its outputs a value of type `T2`.
  * `group` binds `$` per group and places it in scope for `<aggrExpr>`. In addition, `<groupKeyVar>` is in scope for `<aggrExpr>`. Thus, `<aggrExpr>` has access to a given
group's key and group values.

Single input, but rename all default variables:
```
  A -> group each <elementVar> by <groupKeyVar> <groupExpr> as <groupVar> into <aggrExpr> ;
```

  * The re-definition of `$` for the grouping and aggregate expressions allows for terser expressions but can be confusing to read.
  * `group` expressions allows both the iteration variable and the group variable to renamed as `<elementVar>` and `<groupVar>`, respectively.

Multiple inputs:
```
  group A1 by <groupKeyVar> = <groupExpr1> as <groupVar1>,
        A2 by <groupKeyVar> = <groupExpr2> as <groupVar2>,
        ...
        An by <groupKeyVar> = <groupExprN> as <groupVarN>
  into <aggrExpr> ;
```

  * The piping syntax is not supported when grouping multiple inputs.
  * `<groupKeyVar>` must be identical for each input. `<groupExpr>`'s may differ per input.
  * The group variables, `<groupVar>`'s must be defined and unique per input.
  * The `<groupKeyVar>` and all `<groupVar>`'s are all in scope for `<aggrExpr>`.

For `<aggrExpr>`, it is most common to combine value constructors, such as records and array constructors, with aggregate functions. Refer to [aggregates](http://code.google.com/p/jaql/wiki/Builtin_functions#agg) for a list of supported aggregate functions.

## Example ##
```
jaql> employees = [
  {id:1, dept: 1, income:12000},
  {id:2, dept: 1, income:13000},
  {id:3, dept: 2, income:15000},
  {id:4, dept: 1, income:10000},
  {id:5, dept: 3, income:8000},
  {id:6, dept: 2, income:5000},
  {id:7, dept: 1, income:24000}
];

// Compute an aggregate over all employee recs. All recs are bound to $. 
jaql> employees -> group into count($);
[
  7
]

// Compute an aggregate per department. 
// Note the two uses of $ for both the iteration and group variables.
// The $[*].income expression projects the income field from group's records, 
// producing an array of longs.
// Note that as a short-hand, the first output field (d) has its name constructed from
// the variable name.
jaql> employees -> group by d = $.dept 
                   into {d, total: sum($[*].income)};
[
  {
    "d": 2,
    "total": 20000
  },
  {
    "d": 1,
    "total": 59000
  },
  {
    "d": 3,
    "total": 8000
  }
]

// Repeat the query above with variable renamings for iteration and group variables
jaql> employees -> group each emp by d = emp.dept as deptEmps
                   into {d, total: sum(deptEmps[*].income)};
[
  {
    "d": 2,
    "total": 20000
  },
  {
    "d": 1,
    "total": 59000
  },
  {
    "d": 3,
    "total": 8000
  }
]

jaql> depts = [
  {did: 1, name: "development"},
  {did: 2, name: "marketing"},
  {did: 3, name: "sales"}
];

// Example that shows off multiple inputs. Note that co-group is more general than join.
// A join would enumerate the cross-product between emps and deps that share the same
// dept id. For co-group, direct access to the matching emps and deps, per dept are
// available to the aggregate expression. In this case, the aggregate expression
// constructs a single record per group, providing the department id, the name (a 1:n
// relationship is assumed, which is why it makes sense to look only at the first dept
// record), the list of matching emp ids, and the count of emps per department.
jaql> group employees by g = $.dept as es,
            depts     by g = $.did  as ds
      into { dept: g, deptName: ds[0].name, emps: es[*].id, numEmps: count(es) };
[
  {
    "dept": 2,
    "deptName": "marketing",
    "emps": [3,6],
    "numEmps": 2
  },
  {
    "dept": 1,
    "deptName": "development",
    "emps": [1,2,4,7],
    "numEmps": 4
  },
  {
    "dept": 3,
    "deptName": "sales",
    "emps": [5],
    "numEmps": 1
  }
]
```

# Join #

The `join` expression is used to express a join between two or more input arrays.
The join condition between two inputs is assumed to be an equi-join. When joining
more than two inputs, the join condition is assumed to be a conjunction of equi-joins
where any two inputs are connected by a path of multiple equi-joins. Jaql's `join` is similar to SQL's `JOIN`. It too supports multiple types of joins, including natural, left-outer, right-outer, and outer joins.

## Syntax ##
```
  join preserve? A1,
       preserve? A2,
       ...
       preserve? AN
  where <joinExpr>
  into  <joinOut> ;
```

  * input: `A1` of type `[ T1 ]`, `A2` of type `[ T1 ]`, ...
  * output: [TM ](.md) where TM is generated by `<joinOut>`.
  * The variable `Ai` is by default bound to each element of input `Ai`.
  * The variables `A1, ..., AN` are in scope for `<joinExpr>`and `<joinOut>`.
  * `<joinExpr>` is a conjunction of equality expressions between `Ai` and `Aj`, where `i != j`. In addition for any `i`, `j`, `i != j`, a path must exist between `i` and `j`. Consider a graph G where nodes are input variables and edges are specified by conjunctions and equality expressions. For example, if `x == y and y == z`, then there is a path from `x` to `z`.
  * if there are no `preserve`'s specified for any of the inputs, then the `join` is defined to be a natural join.
  * if `preserve` is specified on a given input, then all of its values will appear, regardless of whether or not another input has matching values. Using `preserve`, you can achieve the same semantics as SQL's various OUTER JOIN options.

```
  join preserve? <var1> in A1,
       preserve? <var2> in A2,
       ...
       preserve? <varN> in AN
  where <joinExpr>
  into  <joinOut> ;
```

  * The syntax above allows the iteration variable to be renamed.
  * As before, `<var1>, ..., <varN>` are in scope for `<joinExpr>`and `<joinOut>`.

## Example ##
```
jaql> users = [
  {name: "Jon Doe", password: "asdf1234", id: 1},
  {name: "Jane Doe", password: "qwertyui", id: 2},
  {name: "Max Mustermann", password: "q1w2e3r4", id: 3}
];

jaql> pages = [
  {userid: 1, url:"code.google.com/p/jaql/"},
  {userid: 2, url:"www.cnn.com"},
  {userid: 1, url:"java.sun.com/javase/6/docs/api/"}
];

// join users and pages on user id. For output, project the user's name 
// and all page fields
jaql> join users, 
           pages 
      where users.id == pages.userid 
      into {users.name, pages.*};
[
  {
    "name": "Jane Doe",
    "url": "www.cnn.com",
    "userid": 2
  },
  {
    "name": "Jon Doe",
    "url": "code.google.com/p/jaql/",
    "userid": 1
  },
  {
    "name": "Jon Doe",
    "url": "java.sun.com/javase/6/docs/api/",
    "userid": 1
  }
]

// repeat the example above, but rename all iteration variables
jaql> join u in users, 
           p in pages 
      where u.id == p.userid 
      into {u.name, p.*};
[
  {
    "name": "Jane Doe",
    "url": "www.cnn.com",
    "userid": 2
  },
  {
    "name": "Jon Doe",
    "url": "code.google.com/p/jaql/",
    "userid": 1
  },
  {
    "name": "Jon Doe",
    "url": "java.sun.com/javase/6/docs/api/",
    "userid": 1
  }
]

// left-outer join, retaining all users
jaql> join preserve u in users, 
                    p in pages 
      where u.id == p.userid 
      into {u.name, p.*};
[
  {
    "name": "Jane Doe",
    "url": "www.cnn.com",
    "userid": 2
  },
  {
    "name": "Jon Doe",
    "url": "code.google.com/p/jaql/",
    "userid": 1
  },
  {
    "name": "Jon Doe",
    "url": "java.sun.com/javase/6/docs/api/",
    "userid": 1
  },
  {
    "name": "Max Mustermann"
  }
]
```

# Union #
Please refer to [union](http://code.google.com/p/jaql/wiki/Builtin_functions#union%28%29).

# Tee #
Please refer to [tee](http://code.google.com/p/jaql/wiki/Builtin_functions#tee%28%29).

# Sort #

The `sort` expression re-orders an input array according to a specified order.

## Syntax ##

```
A -> sort by [<expr desc|asc>, *];`
```

  * input: `A` is an array of type `T`
  * output: `A'` is also an array of type `T`
  * `[<expr desc|asc>, *]` constructs a comparator used to sort `A`.
  * `sort` by default defines `$` to be the iteration variable which is in scope for each comparator expression.
  * by default, `asc` is used for order the input array in ascending order

## Example ##
```
jaql> nums = [ 2, 1, 3 ];

// sort an array of longs
jaql> nums -> sort by [ $ ];
[
  1,
  2,
  3
]

// same expression but override the iteration variable
jaql> nums -> sort each n by [ n ];
[
  1,
  2,
  3
]

// sort in descending order
jaql> nums -> sort by [ $ desc ];
[
  3,
  2,
  1
]

jaql> test = [[2,2,"first"],[1,2,"second"],[2,1,"third"],[1,1,"fourth"]];

// sort that uses a complex comparator, where each comparator expression projects values // from the input
jaql> test -> sort by [$[0], $[1] desc];
[
  [
    1,
    2,
    "second"
  ],
  [
    1,
    1,
    "fourth"
  ],
  [
    2,
    2,
    "first"
  ],
  [
    2,
    1,
    "third"
  ]
]
```

# Top #

The `top` expression selects the first `k` elements of its input. If a comparator is provided, the output is semantically equivalent to sorting the input, then selecting the first `k` elements.

Note: the current implementation of `top`, when a comparator is specified, uses `sort`. A more efficient implementation is provided by [topN](http://code.google.com/p/jaql/wiki/Builtin_functions#topN%28%29).

## Syntax ##
```
  A -> top <k>
```

  * input: `A` is an array of type `T`
  * output: the first `<k>` elements from `A'`, which is also an array of type `T`

```
  A -> top <k> by [<expr desc|asc>, *];
```

  * `[<expr desc|asc>, *]` constructs a comparator used to sort `A`.
  * `top` by default defines `$` to be the iteration variable which is in scope for each comparator expression.
  * by default, `asc` is used for order the input array in ascending order


## Examples ##
```
jaql> nums = [1,2,3];

// select the first two elements from nums, regardless of how nums is ordered.
jaql> nums -> top 2;
[
  2,
  1
]

// specify an ordering from the input from which to select 2 elements.
jaql> nums -> top 2 by [ $ desc ];
[
  3,
  2
]
```