package com.ibm.jaql.io.hadoop;

import org.apache.hadoop.mapred.JobConf;

import com.ibm.jaql.io.hadoop.FileInputConfigurator;
import com.ibm.jaql.io.hadoop.HadoopSerializationTemp;

public class PerfFileInputConfigurator extends FileInputConfigurator {
	  @Override
	  protected void registerSerializers(JobConf conf)
	  {
	    HadoopSerializationPerf.register(conf);
	  }
}
