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
package com.ibm.jaql.lang.util;

import com.ibm.jaql.json.type.JSpan;

/**
 * 
 */
public class SimpleTokenizer
{
  protected byte[] buffer;
  protected int    pos;
  protected int    end;
  protected JSpan  span = new JSpan();

  /**
   * 
   */
  public SimpleTokenizer()
  {
  }

  /**
   * @param buffer
   * @param start
   * @param len
   */
  public SimpleTokenizer(byte[] buffer, int start, int len)
  {
    reset(buffer, start, len);
  }

  /**
   * @param buffer
   * @param start
   * @param len
   */
  public void reset(byte[] buffer, int start, int len)
  {
    this.buffer = buffer;
    this.pos = start;
    this.end = start + len;
  }

  /**
   * Returns the only SpanItem that this will ever return.
   * 
   * @return
   */
  public JSpan getSpan()
  {
    return span;
  }

  /**
   * @return
   */
  public JSpan next()
  {
    while (pos < end && !Character.isLetterOrDigit(buffer[pos]))
    {
      pos++;
    }
    if (pos == end)
    {
      return null;
    }
    span.begin = pos;
    while (pos < end && Character.isLetterOrDigit(buffer[pos]))
    {
      pos++;
    }
    span.end = pos;
    return span;
  }
}
