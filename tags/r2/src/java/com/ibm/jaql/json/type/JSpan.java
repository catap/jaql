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

import com.ibm.jaql.util.BaseUtil;

/**
 * 
 */
public class JSpan extends JAtom
{
  public long begin;
  public long end;

  /**
   * 
   */
  public JSpan()
  {
  }

  /**
   * @param begin
   * @param end
   */
  public JSpan(long begin, long end)
  {
    if (begin < 0)
    {
      throw new IllegalArgumentException("begin out of range:" + begin + "<0");
    }
    if (end < begin)
    {
      throw new IllegalArgumentException("end out of range:" + end + "<"
          + begin);
    }
    this.begin = begin;
    this.end = end;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#getEncoding()
   */
  @Override
  public Item.Encoding getEncoding()
  {
    return Item.Encoding.SPAN;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#compareTo(java.lang.Object)
   */
  @Override
  public int compareTo(Object x)
  {
    JSpan y = (JSpan) x;
    if (begin < y.begin)
    {
      return -1;
    }
    if (begin > y.begin)
    {
      return 1;
    }
    if (end < y.end)
    {
      return -1;
    }
    if (end > y.end)
    {
      return 1;
    }
    return 0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#longHashCode()
   */
  @Override
  public long longHashCode()
  {
    final long g = BaseUtil.GOLDEN_RATIO_64;
    long h = begin * g;
    h = (h | end) * g;
    return h;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#readFields(java.io.DataInput)
   */
  @Override
  public void readFields(DataInput in) throws IOException
  {
    begin = BaseUtil.readVULong(in);
    end = begin + BaseUtil.readVULong(in);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#write(java.io.DataOutput)
   */
  @Override
  public void write(DataOutput out) throws IOException
  {
    BaseUtil.writeVULong(out, begin);
    BaseUtil.writeVULong(out, end - begin);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#toJSON()
   */
  @Override
  public String toJSON()
  {
    return "span(" + begin + "," + end + ")";
  }

  /**
   * @param inText
   * @param outText
   */
  public void getText(JString inText, JString outText)
  {
    outText.copy(inText.getBytes(), (int) begin, (int) (end - begin));
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#copy(com.ibm.jaql.json.type.JValue)
   */
  @Override
  public void copy(JValue jvalue)
  {
    JSpan s = (JSpan) jvalue;
    this.begin = s.begin;
    this.end = s.end;
  }

  /**
   * @param x
   * @param y
   * @return
   */
  public static boolean before(JSpan x, JSpan y)
  {
    return x.end < y.begin;
  }

  /**
   * @param x
   * @param y
   * @return
   */
  public static boolean strictlyContains(JSpan x, JSpan y)
  {
    return x.begin < y.begin && y.end < x.end;
  }

  /**
   * @param x
   * @param y
   * @return
   */
  public static boolean contains(JSpan x, JSpan y)
  {
    return x.begin <= y.begin && y.end <= x.end;
  }

  /**
   * @param x
   * @param y
   * @return
   */
  public static boolean overlaps(JSpan x, JSpan y)
  {
    return x.begin <= y.end && y.begin <= x.end;
  }
}
