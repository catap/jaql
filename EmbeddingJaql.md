# Embedding Jaql #

It is possible to embed Jaql in programs written by different languages, e.g Java and python. Jaql provides very simple api for both java and python programmers, if you have some knowledge of Jaql syntax, you can easily embed Jaql in your program. This document introduces how to setup jaql environment when developing embedding programs, and some examples to demonstrate how one could work with Jaql.

## Embedding Jaql in Java ##

### Setup environment ###
Typically, Jaql runs on a hadoop cluster, which we called cluster model, this requires you have a hadoop cluster (and hbase, if use any hbase related functions) environment, otherwise, you can launch Jaql in local model, which means all the tasks run locally.
We are not going to discuss how to setup a hadoop cluster(hbase) here, below links maybe useful.
  * http://hadoop.apache.org/common/docs/current/cluster_setup.html
  * http://wiki.apache.org/hadoop/Hbase/10Minutes
Let's assume you've installed Hadoop 0.20.1 and hbase 0.20.0. Follow below steps, you can launch Jaql on a running hadoop cluster and connect to a hbase server.

  1. Create a Java project
  1. Add the following jar files into the project's build path
    * jaql.jar
    * hadoop-0.20.1-core.jar
    * log4j-1.2.15.jar
    * commons-logging-1.0.4.jar
    * commons-logging-api-1.0.4.jar
    * hbase-0.20.0-core.jar
    * zookeeper-[r785019](https://code.google.com/p/jaql/source/detail?r=785019)-hbase-1329.jar
  1. Add the following folder into the project's build path
> > conf folder from Jaql source
  1. Add you hadoop conf folder into the project's build path, which includs hadoop configuration files, like core-site.xml, hdfs-site.xml and mapred-site.xml.

Note : Launch Jaql in local model, simply remove hadoop conf folder from project's build path.

### Examples ###
This chapter introduces you several simple examples using Jaql for you to start, more examples can be found in Jaql source.

First you are going to initialize a JaqlQuery instance by its constructor `JaqlQuery()`, then use `setQueryString()` to set input for jaql context, at last use `evaluate()` to evaluate the query statement, this function will return you a JsonValue. Alternatively, `iterator()` can also evaluate the statement but return you a JsonIterator.
Notice that, you can set variable holders in the input parameter, by means of Jaql syntax, it's $varname, then use `setVar()` to set its value at runtime.
You can try out below examples.

#### Write data into HDFS ####
```
public void writePublishers(){
	String PUBLISHERS = "[{name: 'Scholastic', country: 'USA'}, "
        + "{name: 'Grosset', country: 'UK'}, "
        + "{name: 'Writers Publishing House', country: 'China'}]";
	String LOCATION = "publishers";
	try{
		JaqlQuery q = new JaqlQuery();
		q.setQueryString("$publishers -> write(hdfs($location));");	
		q.setArray("$publishers", PUBLISHERS); 
		q.setVar("$location", LOCATION);
		q.evaluate();
		q.close();
	}catch(Exception ex){
		ex.printStackTrace();
	}
}
```

#### Read data from HDFS, and retrieve JsonValue data ####
```
public static void readData(){
	String pName="Scholastic";
	String location = "publishers";
	try{
	        JaqlQuery jaql = new JaqlQuery();
		jaql.setQueryString("read(hdfs($location)) -> filter $.name == $name -> transform $.country;");
		jaql.setVar("$name", pName);
		jaql.setVar("$location", location);
		JsonValue jv = jaql.evaluate();
		System.out.println(jv); // USA
		jaql.close();
	    }catch(Exception ex){
		ex.printStackTrace();
	}
}
```

#### Define / evaluate jaql functions ####
```
public void sampleFunction() {
	try {
		JaqlQuery jaql = new JaqlQuery("samplefn=fn(a,b)(a+b);");
		FunctionArgs args = new FunctionArgs();
		args.setArgument("a", 1200); // set argument by name
		args.setArgument("b", 1200);
                //alternatively，set argument by sequence
                //args.setArguments(1200,1200);
		JsonValue v = jaql.evaluate("samplefn", args); // got 2400
		jaql.close();
	} catch (Exception ex) {
		fail(ex.getMessage());
	}
}
```

#### Import modules ####
```
public void importModule(){
	try{
                String BASE_DIR = "YOUR_JAQL_HOME";
		JaqlQuery q = new JaqlQuery();
		String path = BASE_DIR+"/src/test/com/ibm/jaql/modules";
		File p = new File (path);
		if(!p.isDirectory()){
			return;
		}
		q.setModuleSeachPath(path);
		q.setQueryString("import fuzzy_join;");
		q.evaluate();
		q.setQueryString("fuzzy_join::cleanTitle('XY');");
		JsonValue v = q.evaluate(); // XY
		q.close();
	}catch(Exception ex){
		ex.printStackTrace();
	}
}
```

#### Multiple statements in one query ####
```
public void batchQuery(){
	try{
		JaqlQuery q = new JaqlQuery();
		q.setQueryString("1+1;2+2;3+3;");
		int[] expected = {2,4,6};
		int i = 0;
		while(q.moveNextQuery()){
			JsonIterator it = q.currentQuery(); 
			while(it.moveNext()){
				JsonValue v = it.current(); // 2 - 4 - 6
				assertEquals(new JsonLong(expected[i]), (JsonLong)v); 
				}
				i++;
			}
			q.close();
		}catch(Exception ex){
			fail(ex.getMessage());
		}
	}
```


---


## Embedding Jaql in Python ##

### Introduction ###
PyJaql is an effort to enable python programmers work with Jaql, it's achieved not through re-implementing Jaql in python, but rather through bridging jaql and python by the using of [jpype](http://jpype.sourceforge.net/), it provides a few simple and clean functions for python programmers to take the advantage of jaql's capability. This document introduces you how to setup pyJaql environment and few examples for you to start with.
### Ubuntu Installer ###
This chapter introduces you pyJaql installation and configuration steps.
#### Python ####
By default, [python](http://www.python.org/) is installed on ubuntu. If not, please read guide introduced by python official site to install. The recommended version is python 2.6.
#### jpype ####
[Download](http://sourceforge.net/projects/jpype/files/) source from sourceforge, the latest version of jpype 0.5.4.1 is recommended.
  1. Make sure you have all of the necessary tools for compiling C code installed:
```
  sudo apt-get install build-essential
```
  1. Make sure you have python2.6-dev package installed:
```
  sudo apt-get install python2.6-dev
```
  1. Extract jpype package and enter into jpype dir, run the following command:
```
  sudo python setup.py install
```
  1. Put the newly installed jpype into your PYTHONPATH by putting the following into you /etc/environment , or other system wide variable:
```
  PYTHONPATH=$PYTHONPATH:/usr/local/lib/python2.6/dist-packages
```
  1. Check your install by running the following commands:
```
  $ python
  [GCC 4.2.3 (Ubuntu 4.2.3-2ubuntu7)] on linux2
  Type "help", "copyright", "credits" or "license" for more information.
  >>> import jpype
  >>>
```
If you don't get an ImportError, you're done!
#### pyJaql ####
  1. Download pyJaql installer, namely pyJaql-0.1.tar.gz. Extract this package and enter into pyJaql dir, run following command:
```
   sudo python setup.py install
```
  1. Open python command line, and check your install by below command:
```
   import pyJaql
```
If you don't get an ImportError, the installation is done.

### Usage ###
#### Launch Jaql in different models ####
There are two models to run jaql, one is "local" model, in this situation, pyJaql will launch a mini-cluster on your local machine; On the other hand, if you want to luanch pyJaql on an existing hadoop cluster, use "hadoop-cluster" model.
  * Launch Jaql in local model
> > Unset hadoop related environment variables, eg HADOOP\_HOME, HADOOP\_CONF\_DIR
  * Launch Jaql in hadoop-cluster model
> > _This model requires a running hadoop cluster environment._
> > Set required environment variables, HADOOP\_HOME and HADOOP\_CONF\_DIR. Make sure HADOOP\_HOME points to the right path
```
  export HADOOP_HOME= "yourHadoopHome"
  export HADOOP_CONF_DIR= "yourHadoopHome/conf"
```

#### pyJaql's HelloWorld ####
Basically, there are several steps to get started First, import PyJaql module

```
 from pyJaql import *
```

Second, initialize jaql engine

```
 jaql=Jaql()
```

Third, execute a jaql expression, one can execute any valid jaql expression by using execute(), which will return a python generator.

```
 it=jaql.execute("jaql expression",{'variable name':'variable value',...})
```

Then iterate the result set, and manipulate the return data as you like, depending on the output of jaql expresion, there are 3 kinds of data type of the iterating result, list, dict or primitive types(str,int,etc), there are kinds of ways to manipulate these data easily and conveniently.

```
for item in it:
	# manipulate item as a list, dict or primitive type
	# which depends on the return data format
	# possible ways:
	#1. item type is dict: use item["key"] to retrieve specific key value.
	#2. item type is list: use item[index] to retrieve list element
	#3. item type is python primitive type: directly manipulate item
```
An alternative way to do evaluation is jaql.evaluate(). The usage of evaluate() is very same with execute(), but different in how to retrieve data from jaql, by which means, evaluate() maintain whole result, sometimes a very big block of data, in memory, it cost much more space but consuming less time when retrieving data, however, one must consider that, when the result set is very big, evaluate() costs much more time, sometimes intolerable, to process and return the data. In comparison, execute() does not return us all data in one time, its strategy is to retrieve specific data only when you iterate the result set. They are designed for satisfying different scenarios.
Following examples will show you ways to embed pyJaql in your python program.

### Examples ###
  * Ex1. write data to Hadoop HDFS
```
#Write the books collection from data below to hadoop hdfs. 
	hdfswrite="[{publisher: 'Scholastic',author: 'J. K. Rowling',title: 'Deathly Hallows',year: 2007},{publisher: 'Scholastic',author: 'J. K. Rowling',title: 'Chamber of Secrets',year: 1999, reviews: [{rating: 10, user: 'joe', review: 'The best ...'},{rating: 6, user: 'mary', review: 'Average ...'}]}] -> write(hdfs('books'));" 
	try: 
		jaql=Jaql() 
		jaql.execute(hdfswrite) 
	except JAQLRuntimeException,err: 
		print err.msg 
		#handle exception
	except JVMException,err: 
		print err.msg
		#handle exception
 	finally: 
		jaql.close()	 
```

  * Ex2. Read data from HDFS, use "filter" to filtrate result and use "transform" to format output data.
```

#Read data from HDFS, use "filter" to filtrate result and use "transform" to format output data.
	try: 
		jaql=Jaql() 
		it=jaql.execute("read(hdfs('books'))->filter $.title=='Chamber of Secrets' and $.year==1999 - > transform {$.title,$.year}" ) 
		for record in it: 
			print record
	except JAQLRuntimeException,err: 
		print err.msg 
		#handle exception
	except JVMException,err: 
		print err.msg 
		#handle exception
	finally: 
		jaql.close() 
```
More examples can be found in docs folder under pyJaql package.

### Known Limitation ###
After calling jaql.close(), it will shutdown JVM, if you attempt calling Jaql() again a JVMException will be raised. This dues to SUN's JVM (by now is 1.6.0\_16) doesn't allow uploading, though JNI API defines a method to destroy JVM named destroyJVM(), it doesn't work1.So, pyJaql shares a unique JVM through program's life cycle, there is no problem if you use several times of jaql=Jaql() to get different jaql engine references, because JVM will only be initialized in the first time, but when you invoke jaql.close(), it's required that to make sure no one will use JVM again<sup>(1)</sup>.

### Reference ###

(1). http://jpype.sourceforge.net/doc/user-guide/userguide.html