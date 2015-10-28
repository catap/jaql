# Build #

The build Jaql from the sources, first check-out the project
and from the project's root directory, issue:

  1. ant jar

To run the interpreter, please follow these [instructions](Running.md).

# Using a different version of Hadoop #

The default version of Hadoop used by Jaql is 0.20.1. However, Jaql supports
multiple Hadoop versions (and dependent version of HBase). To build Jaql for
another version, use the `hadoop.version` ant property. For example, to build
for version 0.18.3, you use ant as follows:

  1. ant jar -Dhadoop.version="0.18.3"

We currently support Hadoop versions 0.18.3, and 0.20.1.

# Eclipse Setup #
The source code is a common Eclipse java project. It can be imported into
Eclipse. It needs JDK 1.6 to compile. SUN JDK is preferred since hadoop prefers
SUN JDK. `jaql/.classpath` contains all the Java libraries from multiple
versions of hadoop and hbase. And `jaql/.project` contains stuff related to
antlr and JavaCC. Eclipse `Content Assist` does not work with this `.classpath`
file.  `jaql/eclipse` directory contains `.classpath` file working with a
specific version of hadoop and `.project` without antlr and JavaCC stuff. These
`.classpath` and `.project` are preferred if you wan to work with only a
specific version of hadoop and don't want to use antlr and JavaCC plugin.