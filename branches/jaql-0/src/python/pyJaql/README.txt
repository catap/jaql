Introduction**************************************************************************************************************

PyJaql is an effort to enable python programmers work with Jaql, it's achieved not through re-implementing Jaql in python, 
but rather through bridging jaql and python by using of jpype ( http://jpype.sourceforge.net/ ), it provides a few simple 
and clean functions for python programmers to take the advantage of jaql's capability.

Reference*****************************************************************************************************************

To learn pyJaql API specification, please read docs/pyJaql-0.1-API.pdf
To setup pyJaql, please read docs/PyJaql-0.1-Tutorial.pdf

Installation guide********************************************************************************************************

if you already have "python" and "jpype" installed on your machine, this guide will be simple and helpful for you to start.

1. Unpack pyJaql package and enter into pyJaql dir, run following command:
	sudo python setup.py install

2. (1) launch jaql in "local model"
       Unset "HADOOP_HOME" and "HADOOP_CONF_DIR" environment variables
   (2) launch jaql in "hadoop-cluster model"
	   Set "HADOOP_HOME" and "HADOOP_CONF_DIR" point to right hadoop path. Run exist hadoop clusters.

See more, please read documents in "pyJaql/docs".
