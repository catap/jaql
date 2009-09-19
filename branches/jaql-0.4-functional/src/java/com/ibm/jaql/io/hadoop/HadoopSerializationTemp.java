package com.ibm.jaql.io.hadoop;

import org.apache.hadoop.mapred.JobConf;

import com.ibm.jaql.io.serialization.binary.BinaryFullSerializer;
import com.ibm.jaql.io.serialization.binary.temp.TempBinaryFullSerializer;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.json.type.JsonString;

/** Wrapper for Jaql's temporary serialization. */
public class HadoopSerializationTemp extends HadoopSerialization
{
  @Override
  public boolean accept(Class<?> c)
  {
    return c.equals(JsonHolderTempKey.class) || c.equals(JsonHolderTempValue.class);
  }

  @Override
  public org.apache.hadoop.io.serializer.Deserializer<JsonHolder> 
      getDeserializer(Class<JsonHolder> c)
  {
    if (c.equals(JsonHolderTempKey.class))
    {
      return new HadoopDeserializerTempKey(new TempBinaryFullSerializer(SchemaFactory.nullSchema()));
    }
    else
    {
      Schema schema = getSchema(ConfSetter.CONFINOPTIONS_NAME);
      return new HadoopDeserializerTempValue(new TempBinaryFullSerializer(schema));
    }
  }

  @Override
  public org.apache.hadoop.io.serializer.Serializer<JsonHolder> 
      getSerializer(Class<JsonHolder> c)
  {
    if (c.equals(JsonHolderTempKey.class))
    {
      return new HadoopSerializer(new TempBinaryFullSerializer(SchemaFactory.nullSchema()));
    }
    else
    {
      Schema schema = getSchema(ConfSetter.CONFOUTOPTIONS_NAME);
      return new HadoopSerializer(new TempBinaryFullSerializer(schema));
    }
  }
  
  /** Register this class as an additional serializer in the provided <code>conf</code>. */
  public static void register(JobConf conf) {
    register(conf, HadoopSerializationTemp.class);
  }
  
  /** get the schema information from the job conf */
  private Schema getSchema(String key)
  {
    JsonRecord outoptions = (JsonRecord)getValueFromConf(key);
    JsonRecord options = (JsonRecord)outoptions.getRequired(new JsonString("options"));
    Schema schema = ((JsonSchema)options.getRequired(new JsonString("schema"))).get();
    return schema;
  }
  
  /** Deserializer used for keys. */
  private static class HadoopDeserializerTempKey extends AbstractHadoopDeserializer<JsonHolderTempKey>
  {
    public HadoopDeserializerTempKey(BinaryFullSerializer serializer) {
      super(serializer);
    }
    
    public JsonHolderTempKey newHolder()
    {
      return new JsonHolderTempKey();
    }
  }

  /** Deserializer used for values. */
  private static class HadoopDeserializerTempValue extends AbstractHadoopDeserializer<JsonHolderTempValue>
  {
    public HadoopDeserializerTempValue(BinaryFullSerializer serializer) {
      super(serializer);
    }
    
    public JsonHolderTempValue newHolder()
    {
      return new JsonHolderTempValue();
    }
  }
}
