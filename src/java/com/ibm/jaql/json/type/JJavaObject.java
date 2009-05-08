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
package com.ibm.jaql.json.type;

import java.io.DataOutputStream;
import java.lang.reflect.UndeclaredThrowableException;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Writable;

import com.ibm.jaql.util.RandomAccessBuffer;

/**
 * 
 */
public class JJavaObject extends JAtom
{
  protected Writable value; // TODO: move away from Writables along with hadoop

  /**
   * 
   */
  public JJavaObject()
  {
  }

  /**
   * @param value
   */
  public JJavaObject(Writable value)
  {
    this.value = value;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#clone()
   */
  @Override
  public JJavaObject clone()
  {
    try
    {
      JJavaObject ji = new JJavaObject();
      ji.setCopy(this);
      return ji;
    }
    catch (Exception e)
    {
      throw new UndeclaredThrowableException(e);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#toJSON()
   */
  @Override
  public String toJSON()
  {
    throw new RuntimeException("JJavaObject cannot be converted to JSON");
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#toString()
   */
  @Override
  public String toString()
  {
    return value.toString();
  }

 

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#compareTo(java.lang.Object)
   */
  @SuppressWarnings("unchecked")
  @Override
  public int compareTo(Object x)
  {
    JJavaObject ji = (JJavaObject) x;
    if (value.getClass() == ji.value.getClass())
    {
      return ((Comparable<Object>) value).compareTo(ji.value);
    }
    return value.getClass().getName().compareTo(ji.getClass().getName());
  }

  public void set(Writable o) {
    this.value = o;
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#copy(com.ibm.jaql.json.type.JValue)
   */
  @Override
  public void setCopy(JValue jvalue) throws Exception
  {
    JJavaObject ji = (JJavaObject) jvalue;
    if (value == null || value.getClass() != ji.value.getClass())
    {
      value = ji.value.getClass().newInstance();
    }

    RandomAccessBuffer copyBuffer = new RandomAccessBuffer();
    DataOutputStream copyOutput = new DataOutputStream(copyBuffer);
    ji.value.write(copyOutput);
    copyOutput.flush();

    DataInputBuffer copyInput = new DataInputBuffer(); // TODO: cache
    copyInput.reset(copyBuffer.getBuffer(), 0, copyBuffer.size());
    value.readFields(copyInput);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#getEncoding()
   */
  @Override
  public Item.Encoding getEncoding()
  {
    return Item.Encoding.JAVAOBJECT_CLASSNAME;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#longHashCode()
   */
  @Override
  public long longHashCode()
  {
    long h = value.getClass().getName().hashCode();
    h = (h << 32) | value.hashCode();
    return h;
  }
  
  public Writable getInternalValue() {
    return value;
  }
}
