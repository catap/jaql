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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * 
 */
public class JBool extends JAtom
{
  public final static Item trueItem  = new Item(new JBool(true));
  public final static Item falseItem = new Item(new JBool(false));

  /**
   * @param tf
   * @return
   */
  public static Item make(boolean tf)
  {
    return tf ? trueItem : falseItem;
  }

  public boolean value;

  /**
   * 
   */
  public JBool()
  {
  }

  /**
   * @param value
   */
  public JBool(boolean value)
  {
    this.value = value;
  }

  /**
   * @return
   */
  public boolean getValue()
  {
    return value;
  }

  /**
   * @param value
   */
  public void setValue(boolean value)
  {
    this.value = value;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#getEncoding()
   */
  public Item.Encoding getEncoding()
  {
    return Item.Encoding.BOOLEAN;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#compareTo(java.lang.Object)
   */
  public int compareTo(Object x)
  {
    //    int c = Util.typeCompare(this, (Writable)x);
    //    if( c != 0 )
    //    {
    //      return c;
    //    }
    boolean value2 = ((JBool) x).value;
    return (value == value2) ? 0 : (value2 ? -1 : +1);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#longHashCode()
   */
  @Override
  public long longHashCode()
  {
    return value ? 1 : 0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#readFields(java.io.DataInput)
   */
  @Override
  public void readFields(DataInput in) throws IOException
  {
    value = (in.readUnsignedByte() == 1);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#write(java.io.DataOutput)
   */
  @Override
  public void write(DataOutput out) throws IOException
  {
    out.writeByte(value ? 1 : 2);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#copy(com.ibm.jaql.json.type.JValue)
   */
  @Override
  public void copy(JValue jvalue) throws Exception
  {
    JBool b = (JBool) jvalue;
    value = b.value;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#toJSON()
   */
  @Override
  public String toJSON()
  {
    return value ? "true" : "false";
  }
}
