# Introduction #

We've provided several tools to help you try out Jaql.
Below, we list the software requirements, describe how to get the Jaql,
show how to run the Jaql shell, evaluate examples from the documentatation,
and run unit tests.

# Requirements #
  * Java 1.6 or greater
  * [Cygwin](http://www.cygwin.com/) if using MS Windows
  * If running your own, distributed cluster, [Hadoop 0.20.2](http://archive.apache.org/dist/hadoop/core/hadoop-0.20.2/)

# Jaql Shell #

The Jaql shell is a very simple way to run Jaql queries.
Currently, it can run in local mode, your own cluster, or with a Hadoop MiniCluster.

To get up and running, you can either download a binary or build from source. The instructions below show you how to run the Jaql shell from a terminal. Alternatively, you can also [run the Jaql shell from Eclipse](RunningFromEclipse.md).

## Using a binary ##

  1. [Download](http://jaql.googlecode.com/files/jaql-0.5.1_12_07_2010.tgz) the tar.
  1. **untar** jaql-0.5.1\_12\_07\_2010.tgz into DIR

## Using the source ##

Follow these [instructions](Building.md) to build Jaql.

## Running ##

  1. **setup the environment:** export JAQL\_HOME=[DIR](DIR.md)/jaql-0.5.1
  1. **run it:** from DIR/jaql-0.5.1, run `./bin/jaqlshell`. You should see the `jaql>` prompt if successful.

_Note:_ If you already have a Hadoop cluster installed, when running jaql in local mode,
be sure to unset Hadoop related environment variables (e.g., HADOOP\_HOME, HADOOP\_CONF\_DIR, ...).

If you already have a distributed cluster installed and what to try out Jaql,
pass in the `--cluster` flag as follows: `jaqlshell --cluster`.

The MiniCluster and local modes are geared for running jaql in self-contained
manner on a single server. A MiniCluster runs HDFS, and map-reduce as multiple threads in a single process. The local mode uses Hadoop's local mode. We recommend
using Hadoop's local mode since it is easier to use and offers better response time.
To run in local mode, use `jaql -Djaql.mapred.mode=local JaqlShell --cluster`
and to run in MiniCluster mode, use `jaqlshell`.

JaqlShell supports the following options:

```
jaqlshell [options] [<file1> [<file2> ...]]
options
  -h (--help,-?)             print this message
  -j (--jars) <args>         comma-separated list of jar files to include user
                             defined expressions or data stores
  -b (--batch)               run in batch mode (i.e., do not read from stdin)
  -e (--eval) <expr>         evaluate Jaql expression
  Cluster options
    -c (--cluster)           use existing cluster (i.e., do not launch a
                             mini-cluster)
    -d (--hdfs-dir) <arg>    mini-cluster option: root HDFs directory
    -n (--no-nodes) <arg>    mini-cluster option: number of nodes to spawn
  <file> [<file> ...]        list of input files
```

### Know Limitations ###

# Examples #

All examples from the documentation can be found in DIR/jaql-0.5.1/docs/examples.txt.
JaqlShell can be used to evaluate the examples either interactively or as a script.
To run interactively, fire up the shell, then cut-and-paste.
To run as a script, start the shell in batch mode and supply the example file as an argument, i.e.,
`./bin/jaqlshell -b docs/examples.txt`.

_Note:_ Queries from the documentation that intentionally produce exceptions were excluded from examples.txt.

When extending Jaql, be sure to specify the jar file that includes your extensions to the shell. For example, if your extension code is in `myextensions.jar`, specify the jar file to the shell as follows: `./bin/jaqlshell --jars myextensions.jar`.

# Unit Tests #

The "tests" directory includes other tests that can be used as additional examples.
The tests are included under DIR/jaql/tests. Each test includes a _Queries_
file (i.e., DIR/jaql-0.5.1/tests/coreQueries.txt) and a _Gold_ file (i.e., DIR/jaql-0.5.1/tests/coreGold.txt)
that lists expected outputs, including when failures are expected.
To run all of the tests, from DIR/jaql, run `ant test`.
The test outputs and logs will be placed in DIR/jaql/build/test/cache and DIR/jaql-0.5.1/build/test, respectively.
To run only one of the tests, from DIR/jaql-0.5.1, run `ant test -Dtest.include="TestCore"` for example. The tests can also be run on a distributed cluster as follows: `ant test-cluster`.