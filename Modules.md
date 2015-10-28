# Introduction #

Jaql's **modules** allow users to package related Jaql functions and resources (such as Java jars) so that they are easily re-usable by others.
The features that Jaql's modules enable include:

  * Namespaces: functions defined in a module are placed in their own namespace to avoid name clashes (e.g., variables).
  * Packages: organize modules using directories.
  * Jar management: associate jars with one or more modules

Next, we describe Jaql's modules in more detail:

  * [Module Search Path](#Search_Path.md)
  * [Example of the Simplest Module](#Simple_Module.md)
  * [Importing a Module](#Import.md)
  * [Packages](#Packages.md)

# Search Path #

The module search path is a string containing file system paths that are delimited by `:`. The way to specify the search path to the jaql interpreter is as follows:
```
  -jp "projects/jaql/myModules:scratch/myOtherModules"
```

The module search path is used to look up module and package files during **import**.

# Simple Module #

Consider the following directory layout:
```
  /home/joeuser/modules/simple.jaql
```

If we cat `simple.jaql`, we see a simple variable assignment:
```
  greeting = "Hello, World!";
```

To use the module, if jaql is launched from the `/home/joeuser` directory, we use the following command line:
```
  jaql -jp "modules"
```

Then, from the interpreter, we can import the module, which creates a new namespace, and use the variables that are defined in `simple`:
```
jaql> import simple;

jaql> simple::greeting;
"Hello, World!"
```

In practice, such a module would be filled with related functions, registered udf's, global variables, etc. In addition, modules can import other modules (so long as no cycles are introduced). Often, we use a single, top-level driver script that simply includes top-level modules and calls very their high-level functions.

# Import #

Several options are available when importing a module to control how the module's variables interact with the current namespace. The current namespace is defined to be the namespace of the script that issuing the import call (e.g., a script or the interpreter).

```
  import simple1;

  // reference variables as
  simple1::greeting;
```

```
  import simple1 (greeting);

  // promotes only greeting into the current namespace
  greeting;
```

```
  // promotes all variables in simple1 into current namespace
  import simple1 (*);
```

```
  // alias for simple1
  import simple1 as x;

  x::greeting;
```

In addition, the import can specify a module that is contained within packages. Suppose that `simple1` is nested in the package `test`, e.g., `/home/joeuser/modules/test/simple1`. Then the import would look like this:
```
  import test::simple1;
```

# Packages #

Jaql packages correspond to directories (similar to Python's package design). A package can include other packages, modules, and initialization statements. For example, consider the following directory layout:
```
  /home/joeuser/modules/p1/_p1.jaql
  /home/joeuser/modules/p1/simple1.jaql
  /home/joeuser/modules/p1/p2/simple2.jaql
  /home/joeuser/modules/p3.jaql
  /home/joeuser/modules/p3/simple3.jaql
```

Four modules are defined in three packages. The `p1` package includes a module (`p1::simple1`) and a package that includes a module (`p1::p2::simple2`). In addition, `p1` includes some initialization statements (`_p1.jaql`) that are evaluated before any sub-module is imported. The convention is to name the initialization file by prepending the package name with a `_`. The most common use of the initialization file is to register jars that are needed by module udf's. The way that jars are registered is as follows:
```
  addClassPath( "<path1>:<path2>:...:<pathN>" );
```

The path is a `:` delimited string of jar paths relative to the package directory. Often, a package is used to factor out all of the java jars that are needed by package modules.

The `p3.jaql` module shows an example of a module and package that are named the same. If one issues an `import p3`, the `p3.jaql` file will be loaded. If one issues an `import p3::simple3`, then only `p3` will be loaded. If the `p3` package includes initialization, then these statements will only be evaluated for `simple3`, not the `p3` module.