package com.ibm.jaql.io.hadoop;

import org.apache.hadoop.mapred.JobConf;

import com.ibm.jaql.io.serialization.binary.BinaryFullSerializer;
import com.ibm.jaql.io.serialization.binary.perf.PerfBinaryFullNullSerializer;
import com.ibm.jaql.io.serialization.binary.perf.PerfBinaryFullSerializer;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.json.type.JsonString;

public class HadoopSerializationPerf extends HadoopSerialization {
	
	  @Override
	  public boolean accept(Class<?> c)
	  {
	    return c.equals(JsonHolderPerfKey.class) || c.equals(JsonHolderPerfValue.class);
	  }

	  @Override
	  public org.apache.hadoop.io.serializer.Deserializer<JsonHolder> 
	      getDeserializer(Class<JsonHolder> c)
	  {
	    if (c.equals(JsonHolderPerfKey.class))
	    {
	      return new HadoopDeserializerPerfKey(new PerfBinaryFullNullSerializer(SchemaFactory.nullSchema()));
	    }
	    else
	    {
	      Schema schema = getSchema(ConfSetter.CONFINOPTIONS_NAME);
	      return new HadoopDeserializerPerfValue(new PerfBinaryFullSerializer(schema));
	    }
	  }

	  @Override
	  public org.apache.hadoop.io.serializer.Serializer<JsonHolder> 
	      getSerializer(Class<JsonHolder> c)
	  {
	    if (c.equals(JsonHolderPerfKey.class))
	    {
	      return new HadoopSerializer(new PerfBinaryFullNullSerializer(SchemaFactory.nullSchema()));
	    }
	    else
	    {
	      Schema schema = getSchema(ConfSetter.CONFOUTOPTIONS_NAME);
	      return new HadoopSerializer(new PerfBinaryFullSerializer(schema));
	    }
	  }
	  
	  /** Register this class as an additional serializer in the provided <code>conf</code>. */
	  public static void register(JobConf conf) {
	    register(conf, HadoopSerializationPerf.class);
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
	  private static class HadoopDeserializerPerfKey extends AbstractHadoopDeserializer<JsonHolderPerfKey>
	  {
	    public HadoopDeserializerPerfKey(BinaryFullSerializer serializer) {
	      super(serializer);
	    }
	    
	    public JsonHolderPerfKey newHolder()
	    {
	      return new JsonHolderPerfKey();
	    }
	  }

	  /** Deserializer used for values. */
	  private static class HadoopDeserializerPerfValue extends AbstractHadoopDeserializer<JsonHolderPerfValue>
	  {
	    public HadoopDeserializerPerfValue(BinaryFullSerializer serializer) {
	      super(serializer);
	    }
	    
	    public JsonHolderPerfValue newHolder()
	    {
	      return new JsonHolderPerfValue();
	    }
	  }

}
