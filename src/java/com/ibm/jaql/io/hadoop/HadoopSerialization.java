package com.ibm.jaql.io.hadoop;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.JobConf;

import com.ibm.jaql.io.serialization.Serializer;
import com.ibm.jaql.io.serialization.binary.BinaryFullSerializer;
import com.ibm.jaql.io.serialization.binary.temp.TempBinaryFullSerializer;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.hadoop.MapReduceBaseExpr;
import com.ibm.jaql.lang.parser.JaqlLexer;
import com.ibm.jaql.lang.parser.JaqlParser;
import com.ibm.jaql.lang.util.JaqlUtil;

/** Wrapper class to make our serializers available to Hadoop. Currently the {@link Serializer}
 * is hard-coded; future versions will read it from the job configuration. */
public class HadoopSerialization
extends Configured 
implements org.apache.hadoop.io.serializer.Serialization<JsonHolder> 
{
  
  // -- org.apache.hadoop.io.serializer.Serialization interface -----------------------------------
  
  @Override
  public boolean accept(Class<?> c)
  {
    return c.equals(JsonHolder.class) 
        || c.equals(JsonHolderMapOutputKey.class) 
        || c.equals(JsonHolderMapOutputValue.class);
  }

  @Override
  public org.apache.hadoop.io.serializer.Deserializer<JsonHolder> 
      getDeserializer(Class<JsonHolder> c)
  {
    if (c.equals(JsonHolder.class))
    {
      return new HadoopDeserializer(getInternalSerializer(c)); 
    }
    else if (c.equals(JsonHolderMapOutputKey.class))
    {
      return new HadoopDeserializerKey(getInternalSerializer(c));
    }
    else if (c.equals(JsonHolderMapOutputValue.class))
    {
      return new HadoopDeserializerValue(getInternalSerializer(c));
    }
    throw new IllegalArgumentException("no deserializer defined for class " + c);
  }

  @Override
  public org.apache.hadoop.io.serializer.Serializer<JsonHolder> 
      getSerializer(Class<JsonHolder> c)
  {
    return new HadoopSerializer(getInternalSerializer(c)); 
  }

  public BinaryFullSerializer getInternalSerializer(Class<? extends JsonHolder> c)
  {
    if (c.equals(JsonHolder.class))
    {
      return BinaryFullSerializer.getDefault();
    }

    // get the schema argument
    String schemaText = getConf().get(MapReduceBaseExpr.SCHEMA_NAME);
    JsonRecord schemaRecord = null;
    if (schemaText != null)
    {
      JsonValue t = null;
      try 
      {
        JaqlLexer lexer = new JaqlLexer(new StringReader(schemaText));
        JaqlParser parser = new JaqlParser(lexer);
        Expr expr = parser.parse();
        t = JaqlUtil.enforceNonNull(expr.eval(Env.getCompileTimeContext()));
      } catch (Exception e)
      {
        // schema stays null 
      }
      schemaRecord = (JsonRecord)t; // intentional class cast exception
    }

    // get the schemata
    Schema keySchema, valueSchema;
    if (schemaRecord != null)
    {
      keySchema = ((JsonSchema)JaqlUtil.enforceNonNull(schemaRecord.getRequired(new JsonString("key")))).get();
      valueSchema = ((JsonSchema)JaqlUtil.enforceNonNull(schemaRecord.getRequired(new JsonString("value")))).get();
    }
    else
    {
      keySchema = SchemaFactory.anyOrNullSchema();
      valueSchema = SchemaFactory.anyOrNullSchema();
    }
    
    // get the right serializer
    if (c.equals(JsonHolderMapOutputKey.class))
    {
      return new TempBinaryFullSerializer(keySchema);
    }
    else if (c.equals(JsonHolderMapOutputValue.class))
    {
      return new TempBinaryFullSerializer(valueSchema);
    }
    throw new IllegalArgumentException("no serializer defined for class " + c);
  }
  
  // -- utility methods ---------------------------------------------------------------------------

  /** Register this class as an additional serializer in the provided <code>conf</code>. */
  public static void register(JobConf conf) {
    String serializations = conf.get("io.serializations");
    if (serializations==null) {
      serializations="";
    } else {
      serializations+=",";
    }
    conf.set("io.serializations", serializations + HadoopSerialization.class.getName());
  }

  
  // -- wrapper classes ---------------------------------------------------------------------------
  
  /** Wrapper for writing. Makes use of an internal buffer because the <code>OutputStream</code> 
   * provided by Hadoop performs poorly when a large number of small elements are written to it.
   * (In our case, these small elements are encoding ids and field lengths, for example.) */
  public static class HadoopSerializer 
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
  static abstract class AbstractHadoopDeserializer<T extends JsonHolder>
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
  
  /** Default deserializer. */
  static class HadoopDeserializer extends AbstractHadoopDeserializer<JsonHolder>
  {
    public HadoopDeserializer(BinaryFullSerializer serializer) {
      super(serializer);
    }
    
    public JsonHolder newHolder()
    {
      return new JsonHolderMapOutputKey();
    }
  }
  
  /** Deserializer used for map output keys. */
  static class HadoopDeserializerKey extends AbstractHadoopDeserializer<JsonHolderMapOutputKey>
  {
    public HadoopDeserializerKey(BinaryFullSerializer serializer) {
      super(serializer);
    }
    
    public JsonHolderMapOutputKey newHolder()
    {
      return new JsonHolderMapOutputKey();
    }
  }

  /** Deserializer used for map output values. */
  static class HadoopDeserializerValue extends AbstractHadoopDeserializer<JsonHolderMapOutputValue>
  {
    public HadoopDeserializerValue(BinaryFullSerializer serializer) {
      super(serializer);
    }
    
    public JsonHolderMapOutputValue newHolder()
    {
      return new JsonHolderMapOutputValue();
    }
  }

  
  // -- helper classes ----------------------------------------------------------------------------
  
  /** Like {@link BufferedOutputStream} but without synchronization. */
  public static class UnsynchronizedBufferedOutputStream extends OutputStream
  {
    static final int BUF_SIZE = 65768;  
    byte[] buf = new byte[BUF_SIZE];
    int count = 0;
    OutputStream out;

    public UnsynchronizedBufferedOutputStream(OutputStream out) {
      this.out = out;
    }

    @Override
    public void write(int b) throws IOException
    {
      if (count == buf.length)
      {
        flushBuf();
      }
      buf[count++] = (byte) b;
    }

    @Override
    public void write(byte b[], int off, int len) throws IOException
    {
      // does not fit
      if (count + len > buf.length)
      {
        flushBuf(); // could be optimized but probably not worth it
        if (len >= buf.length)
        {
          out.write(b, off, len);
          return;
        }
      }

      // fits
      System.arraycopy(b, off, buf, count, len);
      count += len;
    }

    /** Flushes the internal buffer but does not flush the underlying output stream */
    public void flushBuf() throws IOException
    {
      if (count > 0)
      {
        try
        {
          out.write(buf, 0, count);
        } catch (IOException e)
        {
          throw new RuntimeException(e);
        }
        count = 0;
      }
    }

    @Override
    public void flush() throws IOException {
      flushBuf();
      out.flush();
    }
    
    @Override
    public void close() throws IOException {
      flushBuf();
      out.close();      
    }
  }  
}
