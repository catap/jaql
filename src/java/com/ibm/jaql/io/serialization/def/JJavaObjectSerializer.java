package com.ibm.jaql.io.serialization.def;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;

import org.apache.hadoop.io.Writable;

import com.ibm.jaql.io.serialization.BasicSerializer;
import com.ibm.jaql.json.type.JJavaObject;
import com.ibm.jaql.json.type.JValue;

public class JJavaObjectSerializer extends BasicSerializer<JJavaObject>
{
  @Override
  public JJavaObject newInstance()
  {
    return new JJavaObject();
  }

  @Override
  public JJavaObject read(DataInput in, JValue target) throws IOException
  {
    JJavaObject t;
    if (target == null || !(target instanceof JJavaObject)) {
      t = new JJavaObject();
    } else {
      t = (JJavaObject)target;      
    }
    
    String name = in.readUTF();
    Writable o = t.getInternalValue();
    if (o == null || !o.getClass().getName().equals(name))
    {
      try
      {
        Class<?> clazz = Class.forName(name);
        o = (Writable) clazz.newInstance();
        t.set(o);
      }
      catch (Exception e)
      {
        throw new UndeclaredThrowableException(e);
      }          
    }
    o.readFields(in);
    
    return t;
  }
  
  @Override
  public void write(DataOutput out, JJavaObject value) throws IOException
  {
    Writable o = value.getInternalValue();
    out.writeUTF(o.getClass().getName());
    o.write(out);
  }
  
  
  //TODO: efficient implementation of compare, skip, and copy
}
