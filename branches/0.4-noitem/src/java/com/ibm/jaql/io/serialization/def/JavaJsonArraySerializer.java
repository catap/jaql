package com.ibm.jaql.io.serialization.def;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.ibm.jaql.io.serialization.BasicSerializer;
import com.ibm.jaql.json.meta.MetaArray;
import com.ibm.jaql.json.type.JavaJsonArray;
import com.ibm.jaql.json.type.JsonValue;

public class JavaJsonArraySerializer extends BasicSerializer<JavaJsonArray>
{

  @Override
  public JavaJsonArray newInstance()
  {
    return new JavaJsonArray();
  }

  
  @Override
  public JavaJsonArray read(DataInput in, JsonValue target) throws IOException
  {
    if (target == null || !(target instanceof JavaJsonArray)) {
      target = new JavaJsonArray();      
    }
    JavaJsonArray t = (JavaJsonArray)target;
    
    String className = in.readUTF(); // TODO: would like to compress out the class name...
    t.setClass(className);
    MetaArray metaArray = t.getMetaArray();
    Object value = metaArray.read(in, t.getValue());
    t.setValue(value);
    return t;
  }

  @Override
  public void write(DataOutput out, JavaJsonArray value) throws IOException
  {
    MetaArray metaArray = value.getMetaArray();
    out.writeUTF(metaArray.getClazz().getName()); // TODO: would like to compress out the class name...
    metaArray.write(out, value.getValue());
  }
  
  
  //TODO: efficient implementation of compare, skip, and copy
}
