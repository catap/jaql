# Introduction #

It is often convenient to run Jaql from within a development environment such as Eclipse or Emacs. With Eclipse, for example, one can:

  * run Jaql's command-line interpreter
  * use Eclipse's text editor to author scripts
  * cut-and-paste easily between the script and the interpreter
  * use Eclipse's Java debugger to step through your user defined Java functions (or Jaql code, if needed)

In addition, testing out Java programs that call out to Jaql is fairly straight forward. As expected, one can run Jaql in local mode or accessing a remote Hadoop cluster if needed. Below, we outline the steps needed to run Jaql running from within Eclipse.

## Step 1: JaqlShell **Run** Panel ##
We assume that Jaql has been checked-out and imported as a project into Eclipse. Jaql comes with a .launch directory that populates the list of applications that are available for running. This list can be reached via the **Run -> Run Configurations** menu. Note the **JaqlShell** entry on the left-hand side of the screen shot below:

![http://docs.google.com/uc?id=0B2Y36GqYe50LNzVjY2M0Y2ItY2Y2ZS00ZGY2LWJlMGQtZTBiYjYxODA2ZjIz&hl=en&format=endswith.jpg](http://docs.google.com/uc?id=0B2Y36GqYe50LNzVjY2M0Y2ItY2Y2ZS00ZGY2LWJlMGQtZTBiYjYxODA2ZjIz&hl=en&format=endswith.jpg)

## Step 2: Configure arguments ##

Next, configure arguments for JaqlShell and the JVM. The first screen-shot shows how to run in **local** mode, whereas the next screen-shot shows how to run in **cluster** mode.

![http://docs.google.com/uc?id=0B2Y36GqYe50LOThiYmIzZGUtNTAyMy00ZmM4LWI2OTUtNWRjYzQxNjJjZjg0&nonsense=something_that_ends_with.jpg](http://docs.google.com/uc?id=0B2Y36GqYe50LOThiYmIzZGUtNTAyMy00ZmM4LWI2OTUtNWRjYzQxNjJjZjg0&nonsense=something_that_ends_with.jpg)

![http://docs.google.com/uc?id=0B2Y36GqYe50LNDYzNWE1YTktMTNmZi00MGJkLWI5NjEtN2Y1Y2RmZjc4MzIw&nonsense=something_that_ends_with.jpg](http://docs.google.com/uc?id=0B2Y36GqYe50LNDYzNWE1YTktMTNmZi00MGJkLWI5NjEtN2Y1Y2RmZjc4MzIw&nonsense=something_that_ends_with.jpg)

## Step 3: configure classpath ##
The classpath must be modified to first include Jaql's **conf** directory as well as the **jaql.jar**. The conf directory includes various logging and IO settings and the jaql.jar is needed for Jaql's use of MapReduce. If jaql.jar is not present, then it can be built using **ant jar**. The following screen-shot shows the classpath that is configured for local mode. The screen-shot afterwards shows the classpath that is configured to access a remote Hadoop cluster. Note Hadoop's conf directory (which includes files that state where Hadoop's NameNode, JobTracker, etc. are located) appears after Jaql's conf directory:

![http://docs.google.com/uc?id=0B2Y36GqYe50LMDQ1Yjg3NjctOTU3MS00Mzk4LWFkODEtMzlmNjc4MGE5Y2Ri&nonsense=something_that_ends_with.jpg](http://docs.google.com/uc?id=0B2Y36GqYe50LMDQ1Yjg3NjctOTU3MS00Mzk4LWFkODEtMzlmNjc4MGE5Y2Ri&nonsense=something_that_ends_with.jpg)

![http://docs.google.com/uc?id=0B2Y36GqYe50LODBhNTU0NDMtOGJkMC00ZjExLTk5YjUtNTRjNjljYjZmZDkz&nonsense=something_that_ends_with.jpg](http://docs.google.com/uc?id=0B2Y36GqYe50LODBhNTU0NDMtOGJkMC00ZjExLTk5YjUtNTRjNjljYjZmZDkz&nonsense=something_that_ends_with.jpg)

## Run it! ##
With all configured, run Jaql:

![http://docs.google.com/uc?id=0B2Y36GqYe50LNmIyZjcxZjQtYmI3NS00MTcwLTgxYzEtYjIwMzhhZGVmMjlm&nonsense=something_that_ends_with.jpg](http://docs.google.com/uc?id=0B2Y36GqYe50LNmIyZjcxZjQtYmI3NS00MTcwLTgxYzEtYjIwMzhhZGVmMjlm&nonsense=something_that_ends_with.jpg)