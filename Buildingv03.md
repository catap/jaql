# Build #

The build Jaql from the sources, first check-out the project
and from the project's root directory, issue:

  1. ant jar

To run the interpreter, please follow these [instructions](Running.md).

# Using a different version of Hadoop #

The default version of Hadoop used by Jaql is 0.18.1. However, Jaql supports
multiple Hadoop versions (and dependent version of HBase). To build Jaql for
another version, use the `hadoop.version` ant property. For example, to build
for version 0.19.0, you use ant as follows:

  1. ant jar -Dhadoop.version="0.19.0"

We currently support Hadoop versions 0.17.1, 0.18.1, 0.18.3, and 0.19.0.