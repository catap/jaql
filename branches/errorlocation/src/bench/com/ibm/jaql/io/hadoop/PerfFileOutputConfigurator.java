package com.ibm.jaql.io.hadoop;

import org.apache.hadoop.mapred.JobConf;

import com.ibm.jaql.io.hadoop.FileInputConfigurator;
import com.ibm.jaql.io.hadoop.HadoopSerializationTemp;
import com.ibm.jaql.io.hadoop.JsonHolderTempKey;
import com.ibm.jaql.io.hadoop.JsonHolderTempValue;

public class PerfFileOutputConfigurator extends FileOutputConfigurator {
	  @Override
	  protected void registerSerializers(JobConf conf)
	  {
	    conf.setOutputKeyClass(JsonHolderPerfKey.class);
	    conf.setOutputValueClass(JsonHolderPerfValue.class);
	    HadoopSerializationPerf.register(conf);
	  }  
}
