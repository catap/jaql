There is no shortage of interesting problems left to solve in Jaql,
and we welcome help on it. These include:

  * Enable bindings to other programming languages besides Java
so Jaql clients and functions can be written in, say, Python or JavaScript.

  * Investigate how schema may be specified for JSON data and how it can be exploited
for storage and query processing.

  * There are many ways to improve in query rewriting. Projection push-down is a good place to start.

  * Extend the notion of job tracking, load balancing, and restarting to Jaql queries.

  * How to deal with exceptions both in [IO](IO.md) and the Jaql interpreter.

  * Performance testing and performance improvements.

One of the big things we are working on these days is a drastic simplification to the query syntax. Please check-out our [slides](http://jaql.googlecode.com/files/Jaql-Pipes-Intro.ppt) that we presented at the Hadoop User Group on 10/16/2008 ... all feedback greatly appreciated and welcome!