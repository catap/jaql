package com.ibm.jaql.io.hadoop;

import org.apache.hadoop.mapred.JobConf;

import com.ibm.jaql.io.serialization.binary.BinaryFullSerializer;

/** Wrapper for Jaql's default serialization. */
public class HadoopSerializationDefault extends HadoopSerialization
{
  @Override
  public boolean accept(Class<?> c)
  {
    return c.equals(JsonHolderDefault.class);
  }

  @Override
  public org.apache.hadoop.io.serializer.Deserializer<JsonHolder> 
      getDeserializer(Class<JsonHolder> c)
  {
    return new HadoopDeserializerDefault(BinaryFullSerializer.getDefault()); 
  }

  @Override
  public org.apache.hadoop.io.serializer.Serializer<JsonHolder> 
      getSerializer(Class<JsonHolder> c)
  {
    return new HadoopSerializer(BinaryFullSerializer.getDefault()); 
  }
  
  /** Register this class as an additional serializer in the provided <code>conf</code>. */
  public static void register(JobConf conf) {
    register(conf, HadoopSerializationDefault.class);
  }
  
  /** Default deserializer. */
  private  static class HadoopDeserializerDefault extends AbstractHadoopDeserializer<JsonHolderDefault>
  {
    public HadoopDeserializerDefault(BinaryFullSerializer serializer) {
      super(serializer);
    }
  
    public JsonHolderDefault newHolder()
    {
      return new JsonHolderDefault();
    }
  }
}
