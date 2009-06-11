/*
 * Copyright (C) IBM Corp. 2008.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.ibm.jaql.io.serialization.binary.def;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;

import org.apache.hadoop.io.Writable;

import com.ibm.jaql.io.serialization.binary.BinaryBasicSerializer;
import com.ibm.jaql.json.type.JsonJavaObject;
import com.ibm.jaql.json.type.JsonValue;

public class JsonJavaObjectSerializer extends BinaryBasicSerializer<JsonJavaObject>
{
  @Override
  public JsonJavaObject newInstance()
  {
    return new JsonJavaObject();
  }

  @Override
  public JsonJavaObject read(DataInput in, JsonValue target) throws IOException
  {
    JsonJavaObject t;
    if (target == null || !(target instanceof JsonJavaObject)) {
      t = new JsonJavaObject();
    } else {
      t = (JsonJavaObject)target;      
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
  public void write(DataOutput out, JsonJavaObject value) throws IOException
  {
    Writable o = value.getInternalValue();
    out.writeUTF(o.getClass().getName());
    o.write(out);
  }
  
  
  //TODO: efficient implementation of compare, skip, and copy
}
