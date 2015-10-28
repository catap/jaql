
# system #
## batch() ##

> _**Description**_ batch( [T](T.md) A , long n ) returns [[T](T.md)]

> Takes an array A and groups it arbitrarily into blocks of size <= n.
> Typically the last every block but the last block has size n, but
> batch can be run in parallel and could produce more small blocks.

> Example:

> range(1,10) -> batch(3);
> ==> [[1,2,3](.md), [4,5,6], [7,8,9], [10](10.md) ]

> _**Parameters**_ (2 inputs)
> Input Types: `( arg0, required: schema any),( arg1, required: schema any)`

> _**Output**_ `schema any`

## R() ##

> _**Description**_ A function allowing invocation of R from within Jaql.

> R(fn, args=[arg1, ..., item argN](item.md),
> inSchema=[arg1, ...,schema argN](schema.md), outSchema=null,
> init, initInline=true, binary=false, flexible=false)

> A single R process is forked per RFn instance (i.e., call site in the query).
> The R process is forked and the init script/string is passed to R only on the
> first invocation.

> To configure R, add -DR.home=<path to R> and -DR.args=<args to R> to the
> VM arguments.

> // TODO: need jaql.conf

> _**Parameters**_ (1 - 8 inputs)
> Input Types: `( fn, required: schema string),( args = null: schema [ * ]?),( inSchema = null: schema [ * ]?),( outSchema = null: schema schematype?),( init = null: schema string?),( initInline = true: schema boolean),( binary = false: schema boolean),( flexible = false: schema boolean)`

> _**Output**_ `schema any`


---

# pragma #
## const() ##

> _**Description**_ This is a pragma function to force const evaluation.

> _**Parameters**_ (1 inputs)
> Input Types: `( arg0, required: schema any)`

> _**Output**_ `schema any`

## inline() ##

> _**Description**_ This is a pragma function to force let/global inlining.

> _**Parameters**_ (1 inputs)
> Input Types: `( arg0, required: schema any)`

> _**Output**_ `schema any`

## unrollLoop() ##

> _**Description**_ A pragma to encourage loop unrolling.

> _**Parameters**_ (1 inputs)
> Input Types: `( arg0, required: schema any)`

> _**Output**_ `schema any`


---

# del #
## jsonToDel() ##

> _**Description**_ A function for converting JSON to CSV. It is called as follows:
> <p>
<blockquote><pre><code>jsonToDel({schema: '...', delimiter: '...', quoted: '...', escape: '...'})</code></pre> .</blockquote>

<blockquote><i><b>Parameters</b></i> (1 - 2 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any),( arg1 = null: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<hr />
<h1>core</h1>
<h2>daisyChain()</h2>

<blockquote><i><b>Description</b></i> Calls the composition of a set of single argument functions.</blockquote>

<blockquote>daisyChain(T0 input, [f1, f2, ..., fn]) returns Tn</blockquote>

<blockquote>where:<br>
<blockquote>f1(T0) returns T1,<br>
f2(T1) returns T2,<br>
fn(Tn) returns Tn</blockquote></blockquote>

<blockquote>A compose function that returns a function is easily created from this one:<br>
<blockquote>compose = fn(fns) fn(input) daisyChain(input, fns)</blockquote></blockquote>

<blockquote><i><b>Parameters</b></i> (2 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any),( arg1, required: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>diamondTag()</h2>

<blockquote><i><b>Description</b></i> This function is used internally during the rewriting of tee().<br>
</blockquote><blockquote>It is not intended for general use.</blockquote>

<blockquote>e -> tagDiamond( f0, ..., fn )</blockquote>

<blockquote>Exactly the same as:<br>
<blockquote>e -> expand union( [$] -> f0() -> transform [0,$], ...,<br>
<blockquote>[$] -> fn() -> transform [n,$] )</blockquote></blockquote></blockquote>

<blockquote><i><b>Parameters</b></i> (2 - 3 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any),( arg1, required: schema any),( arg2 = null: schema any)...</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>expectException()</h2>

<blockquote><i><b>Description</b></i> This function is used by tests to mask expected exceptions.</blockquote>

<blockquote><i><b>Parameters</b></i> (1 - 2 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any),( arg1 = null: schema any)...</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>groupCombine()</h2>

<blockquote><i><b>Description</b></i> groupCombine(input $X, initialFn, partialFn, finalFn) => $Y<br>
<blockquote>initialFn = fn($k,$X) e1 => $P<br>
partialFn = fn($k,$P) => $P<br>
finalFn = fn($k,$P) => $Y</blockquote></blockquote>

<blockquote><i><b>Parameters</b></i> (4 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any),( arg1, required: schema any),( arg2, required: schema any),( arg3, required: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>jump()</h2>

<blockquote><i><b>Description</b></i> jump(i, e0, ..., en) return one of e0 to en based on i.<br>
</blockquote><blockquote>i must be exactly one of 0...n<br>
Like 'if', it should only evaluate one of e0 to en.</blockquote>

<blockquote>Exactly the same as:<br>
<blockquote>if( i == 0 ) e0<br>
else if( i == 1 ) e1<br>
...<br>
else if( i == n ) en<br>
else raise error</blockquote></blockquote>

<blockquote><i><b>Parameters</b></i> (2 - 3 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any),( arg1, required: schema any),( arg2 = null: schema any)...</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>perf()</h2>

<blockquote><i><b>Description</b></i></blockquote>

<blockquote><i><b>Parameters</b></i> (1 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>retag()</h2>

<blockquote><i><b>Description</b></i> This function is used internally during the rewriting of tee().<br>
</blockquote><blockquote>It is not intended for general use.</blockquote>

<blockquote>e -> retag( f1, ..., fn )</blockquote>

<blockquote>Exactly the same as:<br>
<blockquote>e -> expand ( jump($<a href='0.md'>0</a>, f1, ..., fn)( [$<a href='1.md'>1</a>] ) -> transform[i,$<a href='0.md'>0</a>] )</blockquote></blockquote>

<blockquote><i><b>Parameters</b></i> (2 - 3 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any),( arg1, required: schema any),( arg2 = null: schema any)...</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>skipUntil()</h2>

<blockquote><i><b>Description</b></i> Skip the first elements of input (in order) until the predicate is true,<br>
</blockquote><blockquote>return all elements after the test fires.<br>
If inclusive is true (the default) then return the element that triggered<br>
the condition.  Otherwise, exclude it.</blockquote>

<blockquote>input: [T...]? -> skipUntil( when: fn(T): bool, inclusive:bool = true ): [T...]</blockquote>

<blockquote><i><b>Parameters</b></i> (2 - 3 inputs)<br>
</blockquote><blockquote>Input Types: <code>( input, required: schema [ * ]?),( when, required: schema function),( inclusive = true: schema boolean?)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema [ * ]</code></blockquote>

<h2>streamSwitch()</h2>

<blockquote><i><b>Description</b></i> e0 -> streamSwitch( f0, ..., fn )<br>
</blockquote><blockquote><h3></h3>
( x = e0,<br>
<blockquote>union( x -> filter $<a href='0.md'>0</a> == 0 -> transform $<a href='1.md'>1</a> -> f0(),<br>
<blockquote>...<br>
x -> filter $<a href='0.md'>0</a> == n -> transform $<a href='1.md'>1</a> -> fn() )<br>
</blockquote></blockquote>)</blockquote>

<blockquote>Except that the functions can be called any number of times and in any order.<br>
Something like this:</blockquote>

<blockquote>( x = e0,<br>
<blockquote>union( x -> filter $<a href='0.md'>0</a> == 0 -> transform $<a href='1.md'>1</a> -> batch(n=?) -> expand f0($),<br>
<blockquote>...<br>
x -> filter $<a href='0.md'>0</a> == n -> transform $<a href='1.md'>1</a> -> batch(n=?) -> expand fn($) )<br>
</blockquote></blockquote>)</blockquote>

<blockquote>The actual implementation is to stream into function fi any consecutive rows<br>
with index i.  Something like this:</blockquote>

<blockquote>( x = e0,<br>
<blockquote>x -> tumblingWindow( stop = fn(first,next) first<a href='0.md'>0</a> != next<a href='0.md'>0</a> )<br>
<blockquote>-> expand each p fi( p -> transform $<a href='1.md'>1</a> ) // where i is p<a href='j.md'>j</a><a href='0.md'>0</a> for all j in the window<br>
</blockquote></blockquote>)</blockquote>

<blockquote><i><b>Parameters</b></i> (2 - 3 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any),( arg1, required: schema any),( arg2 = null: schema any)...</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>tag()</h2>

<blockquote><i><b>Description</b></i> This function is used internally during the rewriting of tee().<br>
</blockquote><blockquote>It is not intended for general use.</blockquote>

<blockquote>e -> tag(i)</blockquote>

<blockquote>Exactly the same as:<br>
<blockquote>e -> transform [i,$]</blockquote></blockquote>

<blockquote><i><b>Parameters</b></i> (2 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any),( arg1, required: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>tagFlatten()</h2>

<blockquote><i><b>Description</b></i> This function is used internally during the rewriting of tee().<br>
</blockquote><blockquote>It is not intended for general use.</blockquote>

<blockquote>e -> tagFlatten( int index, int numToExpand )</blockquote>

<blockquote>Exactly the same as:<br>
<blockquote>e -> transform each x (<br>
<blockquote>i = x<a href='0.md'>0</a>,<br>
v = x<a href='1.md'>1</a>,<br>
if( i < index ) then x<br>
else if( i > index ) then <a href='i.md'>+ numToExpand-1, v</a>
else ( assert(0 <= v<a href='0.md'>0</a> < numToExpand), <a href='.md'>v[0</a> + index, v<a href='1.md'>1</a> ] ))</blockquote></blockquote></blockquote>

<blockquote>Example:<br>
<blockquote><a href='.md'>[0,a</a>, [1,[0,b]], [1,[1,c]], [2,d] ] -> tagFlatten( 1, 2 )<br>
</blockquote><blockquote><h2></h2>
<blockquote><a href='.md'>[0,a</a>, [1,b], [2,c], [3,d] ]</blockquote></blockquote></blockquote>

<blockquote><i><b>Parameters</b></i> (3 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any),( arg1, required: schema any),( arg2, required: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>tagSplit()</h2>

<blockquote><i><b>Description</b></i> This function is used internally during the rewriting of tee().<br>
</blockquote><blockquote>It is not intended for general use.</blockquote>

<blockquote>e -> tagSplit( f0, ..., fn )</blockquote>

<blockquote>Exactly the same as:<br>
<blockquote>( X = e,<br>
<blockquote>X -> filter $<a href='0.md'>0</a> == 0 -> f0() -> transform $<a href='1.md'>1</a>, ...<br>
X -> filter $<a href='0.md'>0</a> == n -> fn() -> transform $<a href='1.md'>1</a> )</blockquote></blockquote></blockquote>

<blockquote>Also the same as:<br>
<blockquote>( e -> write( composite( [t0, ..., tn] ) ),<br>
<blockquote>read(t0) -> f0(), ...<br>
read(tn) -> fn() )</blockquote></blockquote></blockquote>

<blockquote><i><b>Parameters</b></i> (2 - 3 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any),( arg1, required: schema any),( arg2 = null: schema any)...</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>until()</h2>

<blockquote><i><b>Description</b></i> Return the first elements of input (in order) until the predicate is true.<br>
</blockquote><blockquote>If inclusive is true (the default) then include the element that triggered<br>
the condition.  Otherwise, exclude it.</blockquote>

<blockquote>input: [T...]? -> until( when: fn(T): bool, inclusive:bool = true ): [T...]</blockquote>

<blockquote><i><b>Parameters</b></i> (2 - 3 inputs)<br>
</blockquote><blockquote>Input Types: <code>( input, required: schema [ * ]?),( when, required: schema function),( inclusive = true: schema boolean?)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema [ * ]</code></blockquote>

<hr />
<h1>hadoop</h1>
<h2>buildModel()</h2>

<blockquote><i><b>Description</b></i> Build a data mining model in parallel.</blockquote>

<blockquote>buildModel(<br>
{ input: fd,<br>
<blockquote>output: fd,  // TODO: this could be eliminated, but required now and gets model<br>
init: fn() -> model,<br>
partial: fn($part,$model) -> pmodel, // $part is array of input items<br>
combine: fn($pmodels,$model) -> model, // $pmodels is array of partial models<br>
done: fn($oldModel, $newModel) -> bool<br>
</blockquote><blockquote>})<br>
</blockquote>-> model</blockquote>

<blockquote><i><b>Parameters</b></i> (1 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>chainedMap()</h2>

<blockquote><i><b>Description</b></i> Run a function <b>sequentially</b> but piecemeal over an input array.</blockquote>

<blockquote>chainedMap(<br>
{ input: fd,<br>
<blockquote>output: fd,  // TODO: this could be eliminated, but required now and gets state<br>
init: state,<br>
map: fn(part,state) -> state, // part is array of input items<br>
schema?: state schema<br>
</blockquote><blockquote>})<br>
</blockquote>-> state</blockquote>

<blockquote><i><b>Parameters</b></i> (1 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<hr />
<h1>io</h1>
<h2>arrayRead()</h2>

<blockquote><i><b>Description</b></i></blockquote>

<blockquote><i><b>Parameters</b></i> (1 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>expandFD()</h2>

<blockquote><i><b>Description</b></i></blockquote>

<blockquote><i><b>Parameters</b></i> (1 - 2 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any),( arg1 = null: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>http()</h2>

<blockquote><i><b>Description</b></i> An expression that constructs an I/O descriptor for local file access.</blockquote>

<blockquote><i><b>Parameters</b></i> (1 - 3 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any),( arg1 = null: schema any),( arg2 = null: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>httpGet()</h2>

<blockquote><i><b>Description</b></i></blockquote>

<blockquote><i><b>Parameters</b></i> (1 - 3 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any),( arg1 = null: schema any),( arg2 = null: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<hr />
<h1>array</h1>
<h2>append()</h2>

<blockquote><i><b>Description</b></i> append($a, $b, ...) ==> unnest <a href='.md'>$a, $b, ... </a> NOT when $a or $b are<br>
</blockquote><blockquote>non-array (and non-null), but that's probably an improvement. NOT when $a or<br>
$b are null, but the change to unnest to remove nulls will fix that should<br>
append(null, null) be null? it would break any unnest definition... Push<br>
unnest into ListExpr?</blockquote>

<blockquote><i><b>Parameters</b></i> (1 - 2 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any),( arg1 = null: schema any)...</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>columnwise()</h2>

<blockquote><i><b>Description</b></i></blockquote>

<blockquote><i><b>Parameters</b></i> (1 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>rowwise()</h2>

<blockquote><i><b>Description</b></i></blockquote>

<blockquote><i><b>Parameters</b></i> (1 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>runningCombine()</h2>

<blockquote><i><b>Description</b></i></blockquote>

<blockquote><i><b>Parameters</b></i> (4 inputs)<br>
</blockquote><blockquote>Input Types: <code>( input, required: schema [ * ]?),( init, required: schema any),( add, required: schema function),( into, required: schema function?)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema [ * ]</code></blockquote>

<hr />
<h1>index</h1>
<h2>buildJIndex()</h2>

<blockquote><i><b>Description</b></i></blockquote>

<blockquote><i><b>Parameters</b></i> (2 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any),( arg1, required: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>probeJIndex()</h2>

<blockquote><i><b>Description</b></i></blockquote>

<blockquote><i><b>Parameters</b></i> (2 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any),( arg1, required: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>sharedHashtableN()</h2>

<blockquote><i><b>Description</b></i> sharedHashtableN(<br>
<blockquote><a href='Key.md'>Key</a> probeKeys,<br>
string buildUrl, // "hash://host:port/tableid",<br>
fn() returns <a href='.md'>[Key,Value</a> ] buildFn,<br>
schema [Key, Value] buildSchema ) // TODO: should be inferred from buildFn OR template params<br>
</blockquote><blockquote>returns [Key,Value]</blockquote></blockquote>


<blockquote>The file represented by fd must have [key,value2] pairs.<br>
The [key,value2] pairs are loaded into a hash table<br>
If the fd is same from call to call, the table is not reloaded.<br>
<blockquote>// TODO: cache multiple tables? perhaps with weak references<br>
// TODO: use hadoop's distributed cache?</blockquote></blockquote>

<blockquote>It is generally assumed that the file is assessible wherever this<br>
function is evaluated.  If it is automatically parallelized, the<br>
file better be available from every node (eg, in hdfs).</blockquote>

<blockquote>Throws an exception if the file contains duplicate keys</blockquote>

<blockquote>If the probe key does not exist in the hashtable, null is returned.</blockquote>

<blockquote><i><b>Parameters</b></i> (4 - 10 inputs)<br>
</blockquote><blockquote>Input Types: {{{( data, required: schema [<br>
<blockquote><a href='.md'>* </a>? <b><br>
</blockquote><blockquote>]?),( url, required: schema <a href='.md'>* </a>?),( buildFn, required: schema <a href='.md'>* </a>?),( buildSchema, required: schema <a href='.md'>* </a>?),( age = -1: schema long),( lease = 0: schema long),( serverStart = true: schema boolean),( serverTimeout = 300000: schema long),( serverMemory = "500M": schema string),( serverThread = false: schema boolean)}}}</blockquote></blockquote></b>

<blockquote><i><b>Output</b></i> {{{schema [<br>
<blockquote>[<br>
<blockquote>any,<br>
any<br>
</blockquote>] <b><br>
</blockquote><blockquote>]}}}</blockquote></blockquote></b>

<hr />
<h1>schema</h1>
<h2>assert()</h2>

<blockquote><i><b>Description</b></i> Returns its first argument and adds the schema information given in the second argument<br>
</blockquote><blockquote>without validation. Use carefully!</blockquote>

<blockquote><i><b>Parameters</b></i> (2 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any),( arg1, required: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>dataGuide()</h2>

<blockquote><i><b>Description</b></i> Usage: dataGuide(any value) returns <a href='string.md'>string</a></blockquote>

<blockquote>Return a string that represents each unique path in the value.<br>
For records:<br>
<blockquote>yield ""<br>
for each field:value in record:<br>
<blockquote>yield "." + field + dataGuide(value)<br>
</blockquote></blockquote>For arrays:<br>
<blockquote>yield "<a href='.md'>.md</a>"<br>
for each value in array<br>
<blockquote>yield "<a href='.md'>.md</a>" + dataGuide(value)<br>
</blockquote></blockquote>For atomic types:<br>
<blockquote>yield ":" + the type name, eg, ":string", ":null"</blockquote></blockquote>

<blockquote><i><b>Parameters</b></i> (1 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>elementsOf()</h2>

<blockquote><i><b>Description</b></i> elementsOf(schema): if schema is (potentially) an array schema, return the schema of its elements (if any)</blockquote>

<blockquote><i><b>Parameters</b></i> (1 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>fieldsOf()</h2>

<blockquote><i><b>Description</b></i> elementsOf(schema): if schema is (potentially) an record schema, return a table describing its known fields:<br>
<blockquote>[{ name: string, schema: schema, index: long }...]</blockquote></blockquote>

<blockquote><i><b>Parameters</b></i> (1 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>isNullable()</h2>

<blockquote><i><b>Description</b></i> isNullable(schema): true if schema might match null, false otherwise</blockquote>

<blockquote><i><b>Parameters</b></i> (1 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>schemaof()</h2>

<blockquote><i><b>Description</b></i></blockquote>

<blockquote><i><b>Parameters</b></i> (1 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>sqlTypeCode()</h2>

<blockquote><i><b>Description</b></i> sqlTypeCode(schema): return the sql type code for schema</blockquote>

<blockquote><i><b>Parameters</b></i> (1 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<hr />
<h1>internal</h1>
<h2>exprtree()</h2>

<blockquote><i><b>Description</b></i> An internal method that can be used to print the internal tree of expressions in JSON format.</blockquote>

<blockquote><i><b>Parameters</b></i> (1 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>hash()</h2>

<blockquote><i><b>Description</b></i> An internal method that can be used to print the internal tree of expressions in JSON format.</blockquote>

<blockquote><i><b>Parameters</b></i> (1 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>longHash()</h2>

<blockquote><i><b>Description</b></i> An internal method that can be used to print the internal tree of expressions in JSON format.</blockquote>

<blockquote><i><b>Parameters</b></i> (1 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<hr />
<h1>nil</h1>
<h2>emptyOnNull()</h2>

<blockquote><i><b>Description</b></i> emptyOnNull(e) == firstNonNull(e, <a href='.md'>.md</a>)</blockquote>

<blockquote><i><b>Parameters</b></i> (1 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>firstNonNull()</h2>

<blockquote><i><b>Description</b></i></blockquote>

<blockquote><i><b>Parameters</b></i> (0 - 1 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0 = null: schema any)...</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>nullElementOnEmpty()</h2>

<blockquote><i><b>Description</b></i></blockquote>

<blockquote><i><b>Parameters</b></i> (1 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>nullOnEmpty()</h2>

<blockquote><i><b>Description</b></i></blockquote>

<blockquote><i><b>Parameters</b></i> (1 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>onEmpty()</h2>

<blockquote><i><b>Description</b></i></blockquote>

<blockquote><i><b>Parameters</b></i> (2 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any),( arg1, required: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<hr />
<h1>db</h1>
<h2>jdbc()</h2>

<blockquote><i><b>Description</b></i></blockquote>

<blockquote><i><b>Parameters</b></i> (1 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<hr />
<h1>catalog</h1>
<h2>catalogInsert()</h2>

<blockquote><i><b>Description</b></i> An expression to insert an entry into catalog. It is an error if an entry<br>
</blockquote><blockquote>with the same key already exists.</blockquote>

<blockquote><i><b>Parameters</b></i> (1 - 2 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any),( arg1 = null: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>catalogLookup()</h2>

<blockquote><i><b>Description</b></i> An expression to read the entry identified by a key in catalog.</blockquote>

<blockquote><i><b>Parameters</b></i> (1 - 2 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any),( arg1 = null: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>catalogUpdate()</h2>

<blockquote><i><b>Description</b></i> An expression to update catalog entry. It can only add records or fields. No<br>
</blockquote><blockquote>old records or fields will be overwritten.</blockquote>

<blockquote><i><b>Parameters</b></i> (1 - 2 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any),( arg1 = null: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>updateComment()</h2>

<blockquote><i><b>Description</b></i> An expression that updates comment field of entry in catalog.</blockquote>

<blockquote><i><b>Parameters</b></i> (1 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<hr />
<h1>agg</h1>
<h2>combine()</h2>

<blockquote><i><b>Description</b></i></blockquote>

<blockquote><i><b>Parameters</b></i> (2 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any),( arg1, required: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>covStats()</h2>

<blockquote><i><b>Description</b></i> covStats(array x) = sum <a href='1.md'>x1 x2 ... xn</a> <b><a href='1.md'>x1 x2 ... xn</a>^T<br>
<blockquote>= [ count   sum(x1)    sum(x2)    ... sum(xn)    ,<br>
<blockquote>sum(x1*x1) sum(x1*x2) ... sum(x1*xn) ,<br>
<blockquote>sum(x2*x2) ... sum(x2*xn) ,<br>
</blockquote></blockquote><blockquote>...                                          ,<br>
<blockquote>sum(xn*xn) ]</blockquote></blockquote></blockquote></blockquote></b>

<blockquote><i><b>Parameters</b></i> (1 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>expSmooth()</h2>

<blockquote><i><b>Description</b></i> Perform exponential smoothing on a sequence of numbers:<br>
<blockquote>s<a href='0.md'>0</a> = x<a href='0.md'>0</a>
s<a href='i.md'>i</a> = a <b>x<a href='i.md'>i</a> + (1-a)</b> s[i-1]<br>
</blockquote></blockquote><blockquote>The numbers are cast to a double and the result is always a double.</blockquote>

<blockquote><i><b>Parameters</b></i> (2 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any),( arg1, required: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>icebergCubeInMemory()</h2>

<blockquote><i><b>Description</b></i></blockquote>

<blockquote><i><b>Parameters</b></i> (3 - 4 inputs)<br>
</blockquote><blockquote>Input Types: {{{( input, required: schema <a href='.md'>* </a>?),( columns, required: schema [<br>
<blockquote>string <b><br>
</blockquote><blockquote>]),( perGroupFn, required: schema function),( minSupport = 1: schema long | double | decfloat)}}}</blockquote></blockquote></b>

<blockquote><i><b>Output</b></i> {{{schema [<br>
<blockquote><a href='.md'>* </a> <b><br>
</blockquote><blockquote>]}}}</blockquote></blockquote></b>

<h2>inferElementSchema()</h2>

<blockquote><i><b>Description</b></i> Infer a schema that describes all of the elements.</blockquote>

<blockquote><i><b>Parameters</b></i> (1 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>vectorSum()</h2>

<blockquote><i><b>Description</b></i> vectorSum(array x) = [sum(x1), sum(x2), ..., sum(xn)]</blockquote>

<blockquote><i><b>Parameters</b></i> (1 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<hr />
<h1>span</h1>
<h2>span()</h2>

<blockquote><i><b>Description</b></i></blockquote>

<blockquote><i><b>Parameters</b></i> (2 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any),( arg1, required: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>span_begin()</h2>

<blockquote><i><b>Description</b></i></blockquote>

<blockquote><i><b>Parameters</b></i> (1 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>span_contains()</h2>

<blockquote><i><b>Description</b></i></blockquote>

<blockquote><i><b>Parameters</b></i> (2 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any),( arg1, required: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>span_end()</h2>

<blockquote><i><b>Description</b></i></blockquote>

<blockquote><i><b>Parameters</b></i> (1 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>span_extract()</h2>

<blockquote><i><b>Description</b></i> span_extract("some big string", span(2,4))<br>
</blockquote><blockquote>"me"</blockquote>

<blockquote>Current implementation uses SubJsonString. If the caller modifies the input string at<br>
some later point, the result of this call will be invalid.</blockquote>

<blockquote><i><b>Parameters</b></i> (2 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any),( arg1, required: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>span_overlaps()</h2>

<blockquote><i><b>Description</b></i></blockquote>

<blockquote><i><b>Parameters</b></i> (2 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any),( arg1, required: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>span_select()</h2>

<blockquote><i><b>Description</b></i></blockquote>

<blockquote><i><b>Parameters</b></i> (2 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any),( arg1, required: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>tokenize()</h2>

<blockquote><i><b>Description</b></i></blockquote>

<blockquote><i><b>Parameters</b></i> (1 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<hr />
<h1>module</h1>
<h2>examples()</h2>

<blockquote><i><b>Description</b></i></blockquote>

<blockquote><i><b>Parameters</b></i> (1 - 2 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any),( arg1 = null: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>listExports()</h2>

<blockquote><i><b>Description</b></i></blockquote>

<blockquote><i><b>Parameters</b></i> (1 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>test()</h2>

<blockquote><i><b>Description</b></i></blockquote>

<blockquote><i><b>Parameters</b></i> (1 - 2 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any),( arg1 = null: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<hr />
<h1>function</h1>
<h2>addClassPath()</h2>

<blockquote><i><b>Description</b></i> Add jars to the classpath</blockquote>

<blockquote><i><b>Parameters</b></i> (1 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<h2>fencePush()</h2>

<blockquote><i><b>Description</b></i> evaluate a function in a separate process<br>
</blockquote><blockquote>Usage:<br>
T2 fencePush( T1 e,  T2 fn(T1 x) );</blockquote>

<blockquote>The fencePush function applies the function argument to e to produce the output.<br>
In particular, the fencePush function is evaluated in a separate process.<br>
In contrast to fence, where all of the input is consumed, fencePush is designed<br>
to be pushed one value at a time (e.g., as in the case of transform). For such<br>
cases, the fencePush process will be re-used between calls.<br>
A common use of fencePush is to shield the Jaql interpreter from user-defined<br>
functions that exhaust memory, for example.</blockquote>

<blockquote><i><b>Parameters</b></i> (2 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any),( arg1, required: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<blockquote><i><b>Examples</b></i>
<pre><code>jaql&gt; [1,2,3] -&gt; write(hdfs("test"));<br>
<br>
jaql&gt; read(hdfs("test")) -&gt; transform fencePush( $, fn(i) i + 1 );<br>
 [2,3,4]<br>
<br>
</code></pre>
<hr />
<h1>net</h1>
<h2>jaqlGet()</h2></blockquote>

<blockquote><i><b>Description</b></i></blockquote>

<blockquote><i><b>Parameters</b></i> (1 - 2 inputs)<br>
</blockquote><blockquote>Input Types: <code>( arg0, required: schema any),( arg1 = null: schema any)</code></blockquote>

<blockquote><i><b>Output</b></i> <code>schema any</code></blockquote>

<hr />