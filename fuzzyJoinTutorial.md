

# Introduction #
The tutorial will give a brief overview how a complex data flow can be modelled in Jaql. The task we consider is how to implement a _fuzzy join_ of two collections on a key where we must use a predicate based on similarity rather than a typical Boolean predicate (e.g., equality or range). This arises in many scenarios where we’d like to combine multiple data sets (e.g., data integration), the different data sets refer to the same object, but their respective keys may not exactly match. Typical examples of such keys are person names or addresses; in this tutorial we focus on movie titles. In particular, we were interested to join the data set from the Netflix competition with IMDB movies. While both data sets refer to movies, they use different keys so we must use movie title as the join key. However, there are many small differences amongst the titles so a join using an equality predicate does not find all movies in common to both data sets. To get better results both sets need to be joined with a fuzzy join based on string similarity. The implementation of that fuzzy join is the focus of this tutorial.


## Example with simple test data ##
Following is a small example that shows the different results between different joins to find matching titles. The data sets are represented in JSON.

Data:
```
$test_a = [
{xyz: 1, title: "Standing in Front of Silverman"},
{xyz: 3, title: "Protecting Delilh"},
{xyz: 5, title: "Discovering Mrs. Wrong"},
{xyz: 7, title: "The Case of Old River"}
];

$test_b = [
{abc: 992, title: "Standing In Front Of Silverman"},
{abc: 996, title: "Protecting Delilah"},
{abc: 9910, title: "Discovering Mrs. Wrong"},
{abc: 9914, title: "Case of Old River, The"},
{abc: 9999, title: "No match for you"}
];
```

Result of join using an equality predicate:
```
jaql> $equalityJoin($test_a, $test_b, “xyz, “abc”, “title”, “title”);
[ { "abc": 9910,
    "title": "Discovering Mrs. Wrong",
    "xyz": 5
} ]
```

Result of join using an equality predicate after titles have been normalized (simple cleaning):
```
jaql> $norm_test_a = $normalize($test_a, “title”);
jaql> $norm_test_b = $normalize($test_b, “title”);
jaql> $equalityJoin($norm_test_a, $norm_test_b, “xyz, “abc”, 
 “title”, “title”);
[ { "abc": 9910,
    "title": "DISCOVERING MRS WRONG",
    "xyz": 5
  },
  { "abc": 992,
    "title": "STANDING IN FRONT OF SILVERMAN",
    "xyz": 1
} ]
```

Result of fuzzy join using Jaccard Similarity after titles have been normalized (title displayed is from $test\_a):
```
jaql> $fuzzyJoin($norm_test_a, $norm_test_b, “xyz”, “abc”, 
                “title”, “title”, 0.5);
[ { "abc": 9910,
    "title": "DISCOVERING MRS WRONG",
    "xyz": 5
  },
  { "abc": 992,
    "title": "STANDING IN FRONT OF SILVERMAN",
    "xyz": 1
  },
  { “abc": 9914,
    “title”: “PROTECTING DELILH”,
    "xyz": 7
  },
  { "abc": 996,
    “title”: “THE CASE OF OLD RIVER”,
    "xyz": 3
  }
]
```

Next, we provide an overview of the fuzzyJoin function.

# Overview of fuzzyJoin #
The fuzzy string join algorithm implemented in Jaql is based on set similarity over qgrams. It expects two collections as input (A, B) that consists of records, each having a title (titleA, titleB) and a key (keyA, keyB). In addition, a threshold can be provided to eliminate those inputs that have a low similarity score. The output is a mapping of records in A to B.

The fuzzy join is implemented by first converting each input’s titles into q-grams, then conceptually, all pairs of titles from A and B are scored using a similarity function and for each pair, the pair with the top score, if it is above the threshold, is returned. In order to reduce the running time of the algorithm, we filter away input records that do not have a chance of producing a match that is greater than the threshold. The steps are as follows:

  1. **Form Q-grams**: each title is split up into a list of qgrams (in this specific implementation we use 3-grams).
  1. **Invert**: Before the filters can be applied we first invert on q-gram to find their collection frequency so that the prefix filter can work more efficiently. For each title, the list of q-grams is then sorted by q-gram frequency.
  1. **Filter**: The filter tries to prune away those titles that do not need to be checked for similarity, based on a similarity threshold. Each filter is run on each input separately.  The filters used are as follows:
    1. **Size filter**: Given a threshold and the title length, potentially matching titles need to be within a specific size range. So for each title multiple records are created for every size value in this range. That way the title can later be joined with titles from the other set which have a different size but are inside the size range. The filter only needs to be applied to one of the inputs.
    1. **Prefix filter**: Given a threshold and the qgram set size, at most x qgrams are allowed not to match. This filter only keeps the first x+1 qgrams of each title in both sets. This filter is very effective because only the least frequent qgrams are kept. So when there is one match, chances are good that the titles have more qgrams in common.
> To generate a set of candidates for each title, both inputs are joined based on the size and qgram fields. The final output of the filter is a (keyA, keyB) pair with the properties that the titles in the pair are within the size range and have at least one matching qgram.
  * **Final join**: Before running the similarity functions the qgrams information must be added to each key pair record. The qgrams are then compared using a similarity function (Jaccard is used in the example). After calculating similarity the best match for each key from A is selected and this results in the final mapping (keyA, keyB) where keyA is unique and keyB is the best matching title from B.

# Implementation of fuzzyJoin in Jaql #
Following is the implementation of fuzzyJoin in Jaql. It shows the fuzzyJoin function which contains the main flow. For input, it has the two collections and their respective key and title field names. It also accepts a threshold value as described earlier. It returns the best match for the A’s titles in B, subject to the threshold.

```
$fuzzy_join = fn($inA, $inB, $keyA, $keyB, $textA, $textB, $threshold) (
	//Create qgrams for the datasets
	$qA_pre = $gen_qgrams($inA, $keyA, $textA),
	$qB_pre = $gen_qgrams($inB, $keyB, $textB),

	//Generate inverse frequency of qgrams 
	//From smaller dataset is better (assume A is smaller)
	$invmap = $group_by_qgrams($qA_pre),
	$freq = $qgram_freq_map($invmap),

	//Sort qgrams by their global frequency
	$qA = $sort_qgrams_by_freq($qA_pre, $freq),
	$qB =$sort_qgrams_by_freq($qB_pre, $freq),
 

	//Filter
	$filtered = $filter($qA, $qB, $threshold),

	//Calculate similarities
	$sims = $jaccard($filtered, $qA, $qB),
	
	//Generate mapping
	$sims
	-> group by $.a into (
		singleton($[*]->sort by [$.sim desc]->top 1)
	)
	-> transform { $keyA: $.a, $keyB: $.b, $.sim}
	-> filter $.sim >= $threshold
);
```

Note that neither map/reduce nor temporary files are explicitly used in the above flow. Rather Jaql creates a plan that consists of a number of map/reduce jobs. The flow also consists of several user defined functions (UDF). The two most notable ones are:

### qgram ###
The fuzzy join depends on qgrams. This function converts a string to a collection of qgrams by scanning the input with a sliding window of size 3 (configurable). Since a qgram can occur several times in a title, we make them unique by associating with each qgram its ordinal number from the input string (e.g.,  [“the”, 1] for the first occurrence of “the” and [“the”, 2] for the second). The qgram/ordinal pairs are returned as an array.
Example, standalone usage:
```
jaql>	qgram(“abcdabc”);
  [["§§a",1],["§ab",1],["abc",1],["bcd",1],["cda",1],["dab",1],["abc",2],["bc§",1],["c§§",1]]
```

### jaccard ###
Currently, the two main ways to define user defined functions in Jaql is via direct Java implementation or as a Jaql function. Because the jaccard function is performance critical it was implemented in java. The function accepts two arrays as input and calculates the jaccard similarity based on how many different elements are in both arrays together and how many both have in common.
Example, standalone usage:
```
jaql> 	jaccard(qgram("jaccard sim"), qgram("jaccrd sim"));
  0.6666666666666666	
```

# Applying fuzzyJoin to Netflix and IMDB #
Now, we show how to use fuzzyJoin with several real data sets. The main modification to the overall flow is to first clean each data set, then invoke fuzzyJoin on the cleaned datasets. We first describe the input data, then review how the data is read and cleaned, and finally combine the input processing with fuzzyJoin to form the final flow.

## Input Data ##
The input data is provided by Netflix and IMDB as plain text files.

We used the IMDB data available from
[ftp://ftp.fu-berlin.de/pub/misc/movies/database/movies.list.gz](ftp://ftp.fu-berlin.de/pub/misc/movies/database/movies.list.gz)
This file contains line delimited records for movies in the Internet Movie Database. The lines that contain information on movie titles are of the following format:
` [title] ([year]) [additional info]? [year] `
The first field, the movie’s title, is used as key in the fuzzy join. It is followed by a year in parentheses that denotes the release year, and more additional information that is not used in our example. The lines containing the actual data are enclosed in a header and footer that have to be stripped when reading the input data into Jaql. The IMDB data set contains 786.411 titles.
The Netflix dataset we used is no longer available, but will be made available at the Machine Learning Archive at UC Irvine (see also http://www.netflixprize.com/closed). The movie titles we used are contained in the line delimited file movie\_titles.txt. The format is even simpler than IMDB’s format:
` [id], [year], [title] `
and can be read using Jaql’s built-in line input format. This dataset has 17.700 titles.

## Cleaning the data ##
The files (movies.list (IMDB) and movie\_titles.txt (Netflix)) are stored in HDFS. They contain line based records, i.e. each line contains at most one record, but some lines, for example in the header, don’t contain data that is useful for the join. Since Hadoop already provides a facility to read line based files, we only needed to write a Jaql converter (the source for the converter is in the fuzzyJoin package) to transform the text that is returned by the TextInputFormat into JSON records.

The Jaql converter is called for each line that is read by the TextInputFormat and is used to transform from any format into Jaql’s JSON representation. The converter to movies skips header lines (returns null) and converts all other lines to JSON records using regular expressions. Additionally a unique identifier is assigned to each record. Recently, schema has been implemented in Jaql which allows for more efficient processing and storage. The movie converter provides Jaql with schema information by implementing a getSchema() method.

The first step in the flow is to get the text files provided by Netflix and IMDB into “clean” JSON data. Therefore an I/O adapter is registered with Jaql that uses a TextInputformat to read a HDFS file and then converts each of those records using the special converter to get JSON values.

```
registerAdapter({type : 'imdbMovie',
 		 inoptions: {adapter : 'com.ibm.jaql.io.hadoop.DefaultHadoopInputAdapter', 
			format : 'org.apache.hadoop.mapred.TextInputFormat', 
			converter : 'com.ibm.jaql.io.hadoop.converter.FromIMDBMovieConverter',
			configurator : 'com.ibm.jaql.io.hadoop.FileInputConfigurator'}});
```

Then Jaql is able to read the HDFS file using the registered adapter, and in the second step, all null values are filtered out (e.g., from reading the header).

```
$imdb_titles_raw = read({type:'imdbMovie',
	location:'imdb/movies.list'});
//Read in imdb titles
$imdb_titles = $imdb_titles_raw
->filter not isnull($.iid);
```

Finally, the titles are normalized using a UDF. The UDF is first registered under the name cleanTitle and then invoked for each title. The output is further processed after normalizing, since several titles may be identical. They are grouped into one title and only the id of one title is kept.

```
registerFunction("cleanTitle", "com.ibm.jaql.udf.CleanTitle");
$imdb_clean = $normalize($imdb_titles, “title”)
-> group by $title = $.title into { iid: $[0].iid, $title };
```

Reading the netflix data is much simpler, each line of the file is first read in using the lines input format and then split and converted into a record:

```
$net_titles = read(lines($net_path))
		->transform strSplitN($, "," , 3) 
		->transform{ nid: long($[0]), year: $[1], title: $[2]}
		->filter not isnull($.nid);

$net_clean = $normalize($net_titles, “title”)
-> group by $title = $.title into { nid: $[0].nid, $title };
```

The cleanTitle UDF is used to clean both titles inside the $normalize function.

### cleanTitle ###
This function prepares the titles before they are used for the fuzzy join. It removes all non alphanumerical characters, converts them to upper case and eliminates consecutive white space between words.
Example, standalone usage:
```
jaql>	cleanTitle(“Hello : \ 	world”);
“HELLO WORLD”
```
After cleaning there are 529927 unique titles from the IMDB dataset and 17346 unique titles from the Netflix dataset.

# Running the fuzzy join #

All the scripts and files that are necessary to run fuzzyJoin are available in a special package in the download section http://jaql.googlecode.com/files/fuzzyJoin_new.tgz

Fuzzy join uses  new features that are not yet supported by any released version. We tested it with [r273](https://code.google.com/p/jaql/source/detail?r=273) and advise users who want to try the fuzzy join to use that specific revision. A compiled version of the jaql.jar of that version is included in the download package. To use it just replace the original one in the jaql directory.
We also advise to use jaql with an existing hadoop cluster by using the –c (--cluster) option.

To run fuzzyJoin with very simple test data just start jaql with the following command:
```
jaqlshell -c -j util.jar fuzzy_join.jaql fuzzy_join_run_test.jaql 
```
We also included the scripts to run fuzzyJoin on the imdb/netflix data. The data itself is not included in the package. The paths in the command need to adjusted to the location of the files on hdfs
```
jaqlshell --eval '$imdb_path="imdb/movies.list";$net_path="netflix/movie_titles.txt"' -c -j util.jar fuzzy_join.jaql fuzzy_join_run_nextflix.jaql 
```


# How effective was fuzzyJoin? #

We evaluate the effectiveness of fuzzyJoin by (1) comparing with the number of matches found using equality join and (2) visually inspecting the quality of the matches (ideas to automate this are welcome!). Below, we list the number of matches per join method (as well as the improvement over equality join w/ normalized titles):

  * equality join: 10106/17700
  * equality join (normalized titles): 10293/17346
  * fuzzyJoin 0.8 threshold: 10530/17346 (+237)
  * fuzzyJoin 0.6 threshold: 11749/17346 (+1456)

Lowering the threshold increases recall—we were able to find ~20% more matches when compared to equality join. For this data set, we visually observed that the quality of the matches decreased rapidly when using a threshold below 0.6.

The Jaccard similarity function is one of many techniques that have been used to implement fuzzy joins. We tried several others; for example we observed that Levenshtein similarity gave better results with short titles but worse for longer terms. The larger point is that we were able to easily modify our main flow and quickly try different similarity and cleaning function. We encourage you to try out the Jaql flows for yourselves and if you are interested in fuzzy joins, to tweak the flows, tell us what works best, and help us to improve the algorithms, the tutorial, and Jaql.

# Appendix #
## Equality Join Function ##
```
$equalityJoin = fn($test_a, $test_b) (
	join $test_a, $test_b
	where $test_a.title == $test_b.title
	into { $test_a.*, $test_b{*-.title}}
);
```

This function uses a special path expression to select the fields which get copied into the new record. “$test\_b{`*`-.$text\_b}” copies all fields from the record except fort he field .”$text\_b”, where $text\_b is a variable.

## Normalize Function ##
```
$normalize = fn($data, $field) (
	$data->transform {${*-.$field}, $field: cleanTitle($.$field)}	
);
```
A description of the cleanTitle UDF can be found in the “Cleaning the data” section.