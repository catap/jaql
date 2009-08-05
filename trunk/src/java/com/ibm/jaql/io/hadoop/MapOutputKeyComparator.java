package com.ibm.jaql.io.hadoop;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import com.ibm.jaql.json.util.DefaultJsonComparator;

public class MapOutputKeyComparator extends DefaultJsonComparator implements Configurable
{
  private Configuration conf;
  
  public MapOutputKeyComparator()
  {
    super();
    serializer = null; // set in setConf
  }
  
  @Override
  public Configuration getConf()
  {
    return conf;
  }

  @Override
  public void setConf(Configuration conf)
  {
    this.conf = conf;
    HadoopSerializationMapOutput hs = new HadoopSerializationMapOutput();
    hs.setConf(conf);
    serializer = hs.keySerializer();
  }  
}
