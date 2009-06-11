package com.ibm.jaql.io.hadoop;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.mapred.JobConf;

import com.ibm.jaql.io.serialization.FullSerializer;
import com.ibm.jaql.io.serialization.Serializer;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JValue;

/** Wrapper class to make our serializers available to Hadoop. Currently the {@link Serializer}
 * is hard-coded; future versions will read it from the job configuration. */
public class HadoopSerialization implements org.apache.hadoop.io.serializer.Serialization<Item> {
  
  // -- org.apache.hadoop.io.serializer.Serialization interface -----------------------------------
  
  @Override
  public boolean accept(Class<?> c)
  {
    return c.equals(Item.class);
  }

  @Override
  public org.apache.hadoop.io.serializer.Deserializer<Item> getDeserializer(Class<Item> c)
  {
    // TODO: make parametrizable
    return new HadoopDeserializer(FullSerializer.getDefault());
  }

  @Override
  public org.apache.hadoop.io.serializer.Serializer<Item> getSerializer(
      Class<Item> c)
  {
    // TODO: make parametrizable
    return new HadoopSerializer(FullSerializer.getDefault());
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
  public static class HadoopSerializer implements org.apache.hadoop.io.serializer.Serializer<Item> {
    FullSerializer serializer;
    // BufferedOutputStream out;    // would work as well but is synchronized
    UnsynchronizedBufferedOutputStream out;   
    DataOutputStream dataOut;
    
    public HadoopSerializer(FullSerializer serializer) {
      this.serializer = serializer;
    }

    @Override
    public void open(OutputStream out) throws IOException
    {
      this.out = new UnsynchronizedBufferedOutputStream(out);
      this.dataOut = new DataOutputStream(this.out);      
    }

    @Override
    public void serialize(Item t) throws IOException
    {
      serializer.write(dataOut, t.get());
      out.flushBuf(); // necessary; otherwise Hadoop crashes
    }
    
    @Override
    public void close() throws IOException
    {
      out.close();      
    }
  }
  
  /** Wrapper for reading. */
  public static class HadoopDeserializer implements org.apache.hadoop.io.serializer.Deserializer<Item> {
    FullSerializer serializer;
    DataInputStream in;
    
    public HadoopDeserializer(FullSerializer serializer) {
      this.serializer = serializer;
    }

    @Override
    public void open(InputStream in) throws IOException
    {
      this.in = new DataInputStream(in);      
    }

    @Override
    public Item deserialize(Item t) throws IOException
    {
      if (t==null) {
        t = new Item();
      }
      JValue v = t.get();
      v = serializer.read(in, v);
      t.set(v);
      return t;
    }

    @Override
    public void close() throws IOException
    {
      in.close();      
    }
  }
  

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
