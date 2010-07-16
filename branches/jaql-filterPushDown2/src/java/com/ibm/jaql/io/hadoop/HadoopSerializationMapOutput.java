package com.ibm.jaql.io.hadoop;

import org.apache.hadoop.mapred.JobConf;

import com.ibm.jaql.io.serialization.binary.BinaryFullSerializer;
import com.ibm.jaql.io.serialization.binary.temp.TempBinaryFullSerializer;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.lang.expr.hadoop.MapReduceBaseExpr;
import com.ibm.jaql.lang.util.JaqlUtil;

/** Wrapper for Jaql's serialization of map outputs (when there is an reducer). */
public class HadoopSerializationMapOutput extends HadoopSerialization
{
  @Override
  public boolean accept(Class<?> c)
  {
    return c.equals(JsonHolderMapOutputKey.class) || c.equals(JsonHolderMapOutputValue.class);
  }

  @Override
  public org.apache.hadoop.io.serializer.Deserializer<JsonHolder> 
      getDeserializer(Class<JsonHolder> c)
  {
    if (c.equals(JsonHolderMapOutputKey.class))
    {
      return new HadoopDeserializerMapOutputKey(keySerializer());
    }
    else
    {
      return new HadoopDeserializerMapOutputValue(valueSerializer());
    }
  }

  @Override
  public org.apache.hadoop.io.serializer.Serializer<JsonHolder> 
      getSerializer(Class<JsonHolder> c)
  {
    if (c.equals(JsonHolderMapOutputKey.class))
    {
      return new HadoopSerializer(keySerializer());
    }
    else 
    {
      return new HadoopSerializer(valueSerializer());
    }
  }
  
  BinaryFullSerializer keySerializer()
  {
    return new TempBinaryFullSerializer(getSchema(true));
  }
  
  BinaryFullSerializer valueSerializer()
  {
    return new TempBinaryFullSerializer(getSchema(false));
  }
  
  /** Register this class as an additional serializer in the provided <code>conf</code>. */
  public static void register(JobConf conf) {
    register(conf, HadoopSerializationMapOutput.class);
  }
  
  /** get the schema information from the job conf */
  private Schema getSchema(boolean key)
  {
    // get the schemata
    JsonRecord schemaOptions = (JsonRecord)getValueFromConf(MapReduceBaseExpr.SCHEMA_NAME);
    Schema keySchema, valueSchema;
    if (schemaOptions != null)
    {
      keySchema = ((JsonSchema)JaqlUtil.enforceNonNull(schemaOptions.getRequired(new JsonString("key")))).get();
      valueSchema = ((JsonSchema)JaqlUtil.enforceNonNull(schemaOptions.getRequired(new JsonString("value")))).get();
    }
    else
    {
      keySchema = SchemaFactory.anySchema();
      valueSchema = SchemaFactory.anySchema();
    }
    if (key) 
    {
      return keySchema;
    }
    else
    {
      return valueSchema;
    }    
  }
  
  /** Deserializer used for keys. */
  private static class HadoopDeserializerMapOutputKey extends AbstractHadoopDeserializer<JsonHolderMapOutputKey>
  {
    public HadoopDeserializerMapOutputKey(BinaryFullSerializer serializer) {
      super(serializer);
    }
    
    public JsonHolderMapOutputKey newHolder()
    {
      return new JsonHolderMapOutputKey();
    }
  }

  /** Deserializer used for values. */
  private static class HadoopDeserializerMapOutputValue extends AbstractHadoopDeserializer<JsonHolderMapOutputValue>
  {
    public HadoopDeserializerMapOutputValue(BinaryFullSerializer serializer) {
      super(serializer);
    }
    
    public JsonHolderMapOutputValue newHolder()
    {
      return new JsonHolderMapOutputValue();
    }
  }
}
