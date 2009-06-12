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
import java.math.BigDecimal;

/**
 * 
 */
public class JDouble extends JNumeric
{
  public double value;

  /**
   * 
   */
  public JDouble()
  {
  }

  /**
   * @param value
   */
  public JDouble(double value)
  {
    this.value = value;
  }

  /**
   * @param str
   */
  public JDouble(String str)
  {
    value = Double.parseDouble(str);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JNumeric#decimalValue()
   */
  @Override
  public BigDecimal decimalValue()
  {
    return new BigDecimal(value);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JNumeric#doubleValue()
   */
  @Override
  public double doubleValue()
  {
    return value;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JNumeric#intValue()
   */
  @Override
  public int intValue()
  {
    return (int) value;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JNumeric#intValueExact()
   */
  @Override
  public int intValueExact()
  {
    int x = (int) value;
    if (x != value) // TODO: is this the best way to determine exactness?
    {
      throw new ArithmeticException("cannot convert to int: " + value);
    }
    return x;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JNumeric#longValue()
   */
  @Override
  public long longValue()
  {
    return (long) value;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JNumeric#longValueExact()
   */
  @Override
  public long longValueExact()
  {
    long x = (long) value;
    if (x != value) // TODO: is this the best way to determine exactness?
    {
      throw new ArithmeticException("cannot convert to long exactly: " + value);
    }
    return x;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JNumeric#negate()
   */
  @Override
  public void negate()
  {
    value = -value;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#toJSON()
   */
  @Override
  public String toJSON()
  {
    return value + "d"; // TODO: flag to disable suffix for JSON compatibility
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#copy(com.ibm.jaql.json.type.JValue)
   */
  @Override
  public void copy(JValue jvalue) throws Exception
  {
    JDouble x = (JDouble) jvalue;
    value = x.value;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#getEncoding()
   */
  @Override
  public Item.Encoding getEncoding()
  {
    return Item.Encoding.DOUBLE;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#readFields(java.io.DataInput)
   */
  @Override
  public void readFields(DataInput in) throws IOException
  {
    value = in.readDouble();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#write(java.io.DataOutput)
   */
  @Override
  public void write(DataOutput out) throws IOException
  {
    out.writeDouble(value);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#compareTo(java.lang.Object)
   */
  @Override
  public int compareTo(Object obj)
  {
    JDouble that = (JDouble) obj;
    if (this.value < that.value)
    {
      return -1;
    }
    else if (this.value > that.value)
    {
      return +1;
    }
    else
    {
      return 0;
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#longHashCode()
   */
  @Override
  public long longHashCode()
  {
    long x = Double.doubleToLongBits(value);
    return JLong.longHashCode(x);
  }
}
