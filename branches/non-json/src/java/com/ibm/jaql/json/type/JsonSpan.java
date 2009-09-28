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

import com.ibm.jaql.util.BaseUtil;

/**
 * 
 */
public class JsonSpan extends JsonAtom
{
  public long begin;
  public long end;

  /**
   * 
   */
  public JsonSpan()
  {
  }

  /**
   * @param begin
   * @param end
   */
  public JsonSpan(long begin, long end)
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
  public JsonEncoding getEncoding()
  {
    return JsonEncoding.SPAN;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#compareTo(java.lang.Object)
   */
  @Override
  public int compareTo(Object x)
  {
    JsonSpan y = (JsonSpan) x;
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

  /**
   * @param inText
   * @param outText
   */
  public void getText(JsonString inText, MutableJsonString outText)
  {
    int length = (int)(end-begin);
    byte[] bytes = outText.get();
    outText.ensureCapacity(length);
    inText.writeBytes(bytes, (int)begin, length);
    outText.set(bytes, length);
  }

  /* @see com.ibm.jaql.json.type.JsonValue#getCopy(com.ibm.jaql.json.type.JsonValue) */
  @Override
  public JsonSpan getCopy(JsonValue target) throws Exception
  {
    if (target == this) target = null;
    
    if (target instanceof JsonSpan)
    {
      JsonSpan t = (JsonSpan)target;
      t.begin = this.begin;
      t.end = this.end;
      return t;
    }
    return new JsonSpan(this.begin, this.end);
  }
  
  @Override
  public JsonSpan getImmutableCopy() throws Exception
  {
    // FIXME: copy is not immutable
    return getCopy(null);
  }

  /**
   * @param x
   * @param y
   * @return
   */
  public static boolean before(JsonSpan x, JsonSpan y)
  {
    return x.end < y.begin;
  }

  /**
   * @param x
   * @param y
   * @return
   */
  public static boolean strictlyContains(JsonSpan x, JsonSpan y)
  {
    return x.begin < y.begin && y.end < x.end;
  }

  /**
   * @param x
   * @param y
   * @return
   */
  public static boolean contains(JsonSpan x, JsonSpan y)
  {
    return x.begin <= y.begin && y.end <= x.end;
  }

  /**
   * @param x
   * @param y
   * @return
   */
  public static boolean overlaps(JsonSpan x, JsonSpan y)
  {
    return x.begin <= y.end && y.begin <= x.end;
  }
}
