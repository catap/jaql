# Introduction #

We've provided several tools to help you try out Jaql.
Below, we list the software requirements, describe how to get the Jaql,
show how to run the Jaql shell, evaluate examples from the documentatation,
and run unit tests.

# Requirements #
  * Java 1.5 or greater
  * [Cygwin](http://www.cygwin.com/) if using MS Windows
  * If running your own, distributed cluster,  [Hadoop 0.17.1](http://archive.apache.org/dist/hadoop/core/hadoop-0.17.1/), [Hadoop 0.18.1](http://archive.apache.org/dist/hadoop/core/hadoop-0.18.1/), [Hadoop 0.18.3](http://archive.apache.org/dist/hadoop/core/hadoop-0.18.3/), and [Hadoop 0.19.0](http://archive.apache.org/dist/hadoop/core/hadoop-0.19.0/)

# Jaql Shell #

The Jaql shell is a very simple way to run Jaql queries.
Currently, it can run with a Hadoop MiniCluster or your own cluster.

To get up and running, you can either download a binary or build from source.

## Using a binary ##

  1. [Download](http://jaql.googlecode.com/files/jaql-0.4_hadoop-0.18.3.tgz) the tar (for Hadoop 0.18.3 clusters). For other versions of Hadoop, download: [Jaql + Hadoop 0.17.1](http://www.jaql.org/release/0.4/jaql-0.4_hadoop-0.17.1.tgz), http://www.jaql.org/release/0.4/jaql-0.4_hadoop-0.18.1.tgz Jaql + Hadoop 0.18.3], or [Jaql + Hadoop 0.19.0](http://www.jaql.org/release/0.4/jaql-0.4_hadoop-0.19.0.tgz)
  1. **untar** jaql-0.3.tgz into DIR

## Using the source ##

Follow these [instructions](Building.md) to build Jaql.

## Running ##

  1. **setup the environment:** export JAQL\_HOME=[DIR](DIR.md)/jaql-0.3
  1. **run it:** from DIR/jaql-0.3, run `./bin/jaql JaqlShell`. You should see the `jaql>` prompt if successful.

_Note:_ If you already have a Hadoop cluster installed, when running jaql in local mode,
be sure to unset Hadoop related environment variables (e.g., HADOOP\_HOME, HADOOP\_CONF\_DIR, ...).

By default, JaqlShell launches a Hadoop MiniCluster. A MiniCluster
runs HDFS, map-reduce, and HBase as multiple threads in a single process.
If you already have a distributed cluster installed and what to try out Jaql,
pass in the `-cluster` flag as follows: `./jaql JaqlShell -cluster`.

JaqlShell supports the following options:

```
-cluster [use an existing cluster, do not launch a MiniCluster]
-jars    [list of comma separated jars to include user defined expressions or data stores]
-dir     [MiniCluster option: the root HDFS directory]
-fmt     [MiniCluster option: format HDFS?]
-n       [MiniCluster options: how many nodes to use]
```

# Examples #

All examples from the documentation can be found in DIR/jaql/docs/examples.txt.
JaqlShell can be used to evaluate the examples either interactively or as a script.
To run interactively, fire up the shell, then cut-and-paste.
To run as a script, pipe in the example file to the shell, i.e.,
`./jaql JaqlShell < docs/examples.txt`.

_Note:_ Queries from the documentation that intentionally produce exceptions were excluded from examples.txt.

When extending Jaql, be sure to specify the jar file that includes your extensions to the shell. For example, if your extension code is in `myextensions.jar`, specify the jar file to the shell as follows: `./jaql JaqlShell -jars myextensions.jar`.

# Unit Tests #

The "tests" directory includes other tests that can be used as additional examples.
The tests are included under DIR/jaql/tests. Each test includes a _Queries_
file (i.e., DIR/jaql/tests/coreQueries.txt) and a _Gold_ file (i.e., DIR/jaql/tests/coreGold.txt)
that lists expected outputs, including when failures are expected.
To run all of the tests, from DIR/jaql, run `ant test`.
The test outputs and logs will be placed in DIR/jaql/build/test/cache and DIR/jaql/build/test, respectively.
To run only one of the tests, from DIR/jaql, run `ant test -Dtest.include="TestCore"` for example. The tests can also be run on a distributed cluster as follows: `ant test-cluster`.