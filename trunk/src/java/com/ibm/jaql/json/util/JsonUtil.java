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

import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.SpilledJsonArray;

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

  /**
   * @param out
   * @param str
   */
  public static void printQuoted(PrintStream out, JsonString str)
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

  // TODO: not needed anymore, kept because it is still used
  public static int deepCompare(SpilledJsonArray x, SpilledJsonArray y) throws Exception
  {
    return x.compareTo(y);
  }

  /**
   * @param iter1
   * @param iter2
   * @return
   * @throws Exception
   */
  public static int deepCompare(JsonIterator iter1, JsonIterator iter2) throws Exception
  {
    boolean hasNext1, hasNext2;
    for (hasNext1 = iter1.moveNext(), hasNext2 = iter2.moveNext(); 
         hasNext1 && hasNext2;
         hasNext1 = iter1.moveNext(), hasNext2 = iter2.moveNext())
    {
      JsonValue value1 = iter1.current();
      JsonValue value2 = iter2.current();

      if (value1 == null)
      {
        return value2==null ? 0 : -1;
      }

      int c = value1.compareTo(value2);
      if (c != 0)
      {
        return c;
      }
    }
    
    return !hasNext1 ? (!hasNext2 ? 0 : -1) : 1;
  }
}
