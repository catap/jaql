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
import com.ibm.jaql.json.type.JLong;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.type.SpillJArray;

/**
 * 
 */
public class JsonUtil
{
  public static char[] hex = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };
  
  /**
   * @param out
   * @param indent
   */
  public static void printSpace(PrintStream out, int indent)
  {
    for (int i = 0; i < indent; i++)
    {
      out.print(' ');
    }
  }

  /**
   * @param text
   * @return
   */
  public static String quote(String text)
  {
    StringBuffer buf = new StringBuffer();
    buf.append("\"");
    for (int i = 0; i < text.length(); i++)
    {
      char c = text.charAt(i);
      switch (c)
      {
        case '\'' :
          buf.append("\\'");
          break;
        case '\"' :
          buf.append("\\\"");
          break;
        case '\\' :
          buf.append("\\\\");
          break;
        case '\b' :
          buf.append("\\b");
          break;
        case '\f' :
          buf.append("\\f");
          break;
        case '\n' :
          buf.append("\\n");
          break;
        case '\r' :
          buf.append("\\r");
          break;
        case '\t' :
          buf.append("\\t");
          break;
        default :
          if (Character.isISOControl(c))
          {
            buf.append("\\u");
            buf.append( hex[ ((c & 0xf000) >>> 12) ] );
            buf.append( hex[ ((c & 0x0f00) >>> 8) ] );
            buf.append( hex[ ((c & 0x00f0) >>> 4) ] );
            buf.append( hex[ (c & 0x000f) ] );
          }
          else
          {
            buf.append(c);
          }
      }
    }
    buf.append("\"");
    return buf.toString();
  }

  /**
   * @param out
   * @param text
   */
  public static void printQuoted(PrintStream out, String text)
  {
    out.print("\"");
    for (int i = 0; i < text.length(); i++)
    {
      char c = text.charAt(i);
      switch (c)
      {
        case '\'' :
          out.print("\\'");
          break;
        case '\"' :
          out.print("\\\"");
          break;
        case '\\' :
          out.print("\\\\");
          break;
        case '\b' :
          out.print("\\b");
          break;
        case '\f' :
          out.print("\\f");
          break;
        case '\n' :
          out.print("\\n");
          break;
        case '\r' :
          out.print("\\r");
          break;
        case '\t' :
          out.print("\\t");
          break;
        default :
          if (Character.isISOControl(c))
          {
            out.print("\\u");
            out.print( hex[ ((c & 0xf000) >>> 12) ] );
            out.print( hex[ ((c & 0x0f00) >>> 8) ] );
            out.print( hex[ ((c & 0x00f0) >>> 4) ] );
            out.print( hex[ (c & 0x000f) ] );
          }
          else
          {
            out.print(c);
          }
      }
    }
    out.print("\"");
  }
  
//  public static String toHex(char c) {
//    StringBuilder sb = new StringBuilder();
//    sb.append("\\u");
//    sb.append( (c & 0xf000) >>> 12).append('|');
//    sb.append( (c & 0x0f00) >>> 8).append('|');
//    sb.append( (c & 0x00f0) >>> 4).append('|');
//    sb.append(c & 0x000f);
//    return sb.toString();
//  }

  /**
   * @param out
   * @param str
   */
  public static void printQuoted(PrintStream out, JString str)
  {
    String s = str.toString(); // TODO: memory; efficient JString to escaped string
    printQuoted(out, s);
  }

  //  public static void print(PrintStream out, JaqlType value, int indent) throws Exception
  //  {
  //    if( value == null )
  //    {
  //      printSpace(out, indent);
  //      out.print("null");
  //    }
  //    else if( value instanceof JAtom )
  //    {
  //      printSpace(out, indent);
  //      ((JAtom)value).print(out);
  //    }
  //    else if( value instanceof JString )
  //    {
  //      printSpace(out, indent);
  //      printQuoted(out, (JString)value);
  //    }
  //    else if( value instanceof JRecord )
  //    {
  //      ((JRecord)value).print(out, indent);
  //    }
  //    else if( value instanceof JArray )
  //    {
  //      print(out, ((JArray)value).iter(), indent);
  //    }
  //    else
  //    {
  //      //out.print("need serialization code for "+value.getClass().getName());
  //      //throw new IOException("need serialization code for "+value.getClass().getName());
  //      printSpace(out, indent);
  //      out.print(value);
  //    }
  //  }

  //  public static void print(PrintStream out, Iter iter, int indent) throws Exception
  //  {
  //    if( iter.isNull() )
  //    {
  //      out.println("null");
  //    }
  //    else
  //    {
  //      Item item;
  //      String sep = "";
  //      printSpace(out, indent);
  //      out.print("[");
  //      indent += 2;
  //      while( (item = iter.next())!= null )
  //      {
  //        out.print(sep);
  //        print(out, item.get(), indent);
  //        sep = ",\n";
  //      }
  //      out.print("]");
  //    }
  //  }

  //  public static String toString(JaqlType w)
  //  {
  //    if( w == null )
  //    {
  //      return null;
  //    }
  //    else if( w instanceof JString )
  //    {
  //      return w.toString();
  //    }
  //    else if( w instanceof JDecimal )
  //    {
  //      // TODO: implement toString on Atom subclasses
  //      return ((JDecimal)w).value.toString();
  //    }
  //    else if( w instanceof JLong )
  //    {
  //      return Long.toString( ((JLong)w).value );
  //    }
  //    else if( w instanceof JDate )
  //    {
  //      return w.toString();
  //    }
  //    else if( w instanceof JBool )
  //    {
  //      return ((JBool)w).getValue() ? "true" : "false";
  //    }
  //    else
  //    {
  //      // TODO: (nearly) all types should be supported.
  //      throw new RuntimeException("can't convert "+w.getClass().getSimpleName()+" to string (yet)");
  //    }
  //  }

  //  public static void toText(JaqlType w, JString out)
  //  {
  //    if( w == null )
  //    {
  //      out.set("");
  //    }
  //    else if( w instanceof JString )
  //    {
  //      out.copy((JString)w);
  //    }
  //    else
  //    {
  //      out.set( toString(w) );
  //    }
  //  }

  private static ItemComparator itemComparator = new ItemComparator();

  /**
   * @param x
   * @param y
   * @return
   * @throws Exception
   */
  public static int deepCompare(SpillJArray x, SpillJArray y) throws Exception
  {
    synchronized (itemComparator)
    {
      return itemComparator.compareSpillArrays(x.getSpillFile().getInput(), y
          .getSpillFile().getInput());
    }
  }

  /**
   * @param iter1
   * @param iter2
   * @return
   * @throws Exception
   */
  public static int deepCompare(Iter iter1, Iter iter2) throws Exception
  {
    Item item1, item2;
    for (item1 = iter1.next(), item2 = iter2.next(); item1 != null
        && item2 != null; item1 = iter1.next(), item2 = iter2.next())
    {
      int c = item1.compareTo(item2);
      if (c != 0)
      {
        return c;
      }
    }
    if (item1 == null)
    {
      if (item2 == null)
      {
        return 0;
      }
      return -1;
    }
    return 1;
  }
}
