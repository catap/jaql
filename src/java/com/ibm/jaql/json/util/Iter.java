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
package com.ibm.jaql.json.util;

import java.io.PrintStream;

import com.ibm.jaql.json.type.Item;

/** Iterator over a list of {@link Item}s. 
 * 
 * */
public abstract class Iter
{
  /** Non-null iterator that does not produce elements */
  public static final Iter empty = new EmptyIter(); 
  
  /** Null iterator */
  public static final Iter nil   = new NullIter();

  /**
   * @return
   */
  public boolean isNull()
  {
    return false;
  }

  /** Returns the next item or <tt>null</tt> if no next item exits.
   * 
   * @return
   * @throws Exception
   */
  public abstract Item next() throws Exception;

  /** Skips n-1 items, and returns the n-th ones. Standard implementation performs
   * n calls to {@link Exception#next()}. Returns {@link Item#NIL} if n<0 or the if
   * the iterator does not provide n more items.   * 
   * 
   * @param n
   * @return
   * @throws Exception
   */
  public Item nth(long n) throws Exception
  {
    if (n < 0)
    {
      return Item.NIL;
    }
    long i = 0;
    Item item;
    while ((item = next()) != null)
    {
      if (i == n)
      {
        return item;
      }
      i++;
    }
    return Item.NIL;
  }

  /**
   * @param out
   * @throws Exception
   */
  public final void print(PrintStream out) throws Exception
  {
    this.print(out, 0);
  }

  /**
   * 
   * <no indent> [ <indent+2> value, ... <indent> ]
   * 
   * OR
   * 
   * <no indent> []
   * 
   * @param out
   * @param indent
   * @throws Exception
   */
  public final void print(PrintStream out, int indent) throws Exception
  {
    if (this.isNull())
    {
      out.println("null");
    }
    else
    {
      Item item;
      String sep = "";
      out.print("[");
      indent += 2;
      while ((item = this.next()) != null)
      {
        out.println(sep);
        for (int s = 0; s < indent; s++)
        {
          out.print(' ');
        }
        item.print(out, indent);
        sep = ",";
      }
      if (sep.length() > 0) // if not empty array
      {
        out.println();
        for (int s = 2; s < indent; s++)
          out.print(' ');
      }
      out.print("]");
    }
  }
}
