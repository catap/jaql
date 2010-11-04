package com.ibm.jaql.io.hadoop;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.JobConf;

import com.ibm.jaql.io.serialization.binary.BinaryFullSerializer;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.parser.JaqlLexer;
import com.ibm.jaql.lang.parser.JaqlParser;
import com.ibm.jaql.lang.util.JaqlUtil;
import com.ibm.jaql.util.UnsynchronizedBufferedOutputStream;

/** Superclass for wrappers that make our serializers available to Hadoop. */
public abstract class HadoopSerialization
extends Configured 
implements org.apache.hadoop.io.serializer.Serialization<JsonHolder> 
{
  
  // -- org.apache.hadoop.io.serializer.Serialization interface -----------------------------------
  
  @Override
  public abstract boolean accept(Class<?> c);

  @Override
  public abstract org.apache.hadoop.io.serializer.Deserializer<JsonHolder> getDeserializer(Class<JsonHolder> c);

  @Override
  public abstract org.apache.hadoop.io.serializer.Serializer<JsonHolder> getSerializer(Class<JsonHolder> c);


  // -- utility methods ---------------------------------------------------------------------------

  /** Register this class as an additional serializer in the provided <code>conf</code>. */
  protected static void register(JobConf conf, Class<?> clazz) {
    String serializations = conf.get("io.serializations");
    if (serializations==null) {
      serializations="";
    } else {
      serializations+=",";
    }
    conf.set("io.serializations", serializations + clazz.getName());
  }

  protected JsonValue getValueFromConf(String key)
  {
    String text = getConf().get(key);
    JsonValue value = null;
    if (text != null)
    {
      try 
      {
        JaqlLexer lexer = new JaqlLexer(new StringReader(text));
        JaqlParser parser = new JaqlParser(lexer);
        Expr expr = parser.parse();
        value = JaqlUtil.enforceNonNull(expr.compileTimeEval());
      } catch (Exception e)
      {
        // value stays null 
      }
    }
    return value;
  }
  // -- wrapper classes ---------------------------------------------------------------------------
  
  /** Wrapper for writing. Makes use of an internal buffer because the <code>OutputStream</code> 
   * provided by Hadoop performs poorly when a large number of small elements are written to it.
   * (In our case, these small elements are encoding ids and field lengths, for example.) */
  protected static class HadoopSerializer
  implements org.apache.hadoop.io.serializer.Serializer<JsonHolder> {
    BinaryFullSerializer serializer;
    // BufferedOutputStream out;    // would work as well but is synchronized
    UnsynchronizedBufferedOutputStream out;   
    DataOutputStream dataOut;
    
    public HadoopSerializer(BinaryFullSerializer serializer) {
      this.serializer = serializer;
    }

    @Override
    public void open(OutputStream out) throws IOException
    {
      this.out = new UnsynchronizedBufferedOutputStream(out);
      this.dataOut = new DataOutputStream(this.out);      
    }

    @Override
    public void serialize(JsonHolder t) throws IOException
    {
      serializer.write(dataOut, t.value);
      out.flushBuf(); // necessary; otherwise Hadoop crashes
    }
    
    @Override
    public void close() throws IOException
    {
      out.close();      
    }
  }
  
  /** Wrapper for reading. */
  protected static abstract class AbstractHadoopDeserializer<T extends JsonHolder>
  implements org.apache.hadoop.io.serializer.Deserializer<JsonHolder> {
    BinaryFullSerializer serializer;
    DataInputStream in;
    
    public AbstractHadoopDeserializer(BinaryFullSerializer serializer) {
      this.serializer = serializer;
    }

    @Override
    public void open(InputStream in) throws IOException
    {
      this.in = new DataInputStream(in);      
    }

    @Override
    public JsonHolder deserialize(JsonHolder t) throws IOException
    {
      if (t==null) {
        t = newHolder();
      }
      t.value = serializer.read(in, t.value);
      return t;
    }

    @Override
    public void close() throws IOException
    {
      in.close();      
    }
    
    public abstract T newHolder();    
  }
  
//  /** Default deserializer. */
//  protected static class HadoopDeserializer extends AbstractHadoopDeserializer<JsonHolder>
//  {
//    public HadoopDeserializer(BinaryFullSerializer serializer) {
//      super(serializer);
//    }
//    
//    public JsonHolder newHolder()
//    {
//      return new JsonHolderMapOutputKey();
//    }
//  }
  
//  /** Deserializer used for map output keys. */
//  static class HadoopDeserializerKey extends AbstractHadoopDeserializer<JsonHolderMapOutputKey>
//  {
//    public HadoopDeserializerKey(BinaryFullSerializer serializer) {
//      super(serializer);
//    }
//    
//    public JsonHolderMapOutputKey newHolder()
//    {
//      return new JsonHolderMapOutputKey();
//    }
//  }
//
//  /** Deserializer used for map output values. */
//  static class HadoopDeserializerValue extends AbstractHadoopDeserializer<JsonHolderMapOutputValue>
//  {
//    public HadoopDeserializerValue(BinaryFullSerializer serializer) {
//      super(serializer);
//    }
//    
//    public JsonHolderMapOutputValue newHolder()
//    {
//      return new JsonHolderMapOutputValue();
//    }
//  }

  
  // -- helper classes ----------------------------------------------------------------------------
  
}
