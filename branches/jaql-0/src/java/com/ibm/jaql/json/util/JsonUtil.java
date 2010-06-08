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

import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.SpilledJsonArray;

/**
 * 
 */
public class JsonUtil
{
  public static final char DOUBLE_QUOTE = '\"';
  public static final char BACK_SLASH = '\\';
  
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
   * Quotes the text.
   * 
   * @param text text to be quoted
   * @param escape <code>true</code> to escape the text; otherwise not.
   * @param doubleQuoteEscapeChar character to escape double quote <tt>"</tt>. 
   * @return quoted text
   * @throws NullPointerException If <code>text</code> is <code>null</code>.
   */
  public static String quote(String text, boolean escape, char doubleQuoteEscapeChar)
  {
    StringBuilder buf = new StringBuilder();
    buf.append("\"");
    for (int i = 0; i < text.length(); i++)
    {
      char c = text.charAt(i);
      if (escape) 
      {
        if (c == DOUBLE_QUOTE) 
        {
          buf.append((char) doubleQuoteEscapeChar);
          buf.append("\"");
        } 
        else 
        {
          buf.append(escape(c));
        }
      } 
      else 
      {
        buf.append(c);
      }
    }
    buf.append("\"");
    return buf.toString();
  }


  /**
   * Escapes the character in Java way.
   * 
   * @param c character
   * @return escaped string
   */
  public static String escape(char c) {
    switch (c)
    {
      case '\'' :
        return "\\'";
      case '\\' :
        return "\\\\";
      case '\b' :
        return "\\b";
      case '\f' :
        return "\\f";
      case '\n' :
        return "\\n";
      case '\r' :
        return "\\r";
      case '\t' :
        return "\\t";
      default :
        if (Character.isISOControl(c))
        {
          StringBuilder buf = new StringBuilder();
          buf.append("\\u");
          buf.append( hex[ ((c & 0xf000) >>> 12) ] );
          buf.append( hex[ ((c & 0x0f00) >>> 8) ] );
          buf.append( hex[ ((c & 0x00f0) >>> 4) ] );
          buf.append( hex[ (c & 0x000f) ] );
          return buf.toString();
        }
        else
        {
          return String.valueOf(c);
        }
    }
  }
  
  

  /**
   * Prints the JSON value as a quoted string.
   * 
   * @param out print stream
   * @param value JSON value
   * @param escape <code>ture</code> to escape characters in Java way;
   *          otherwise, not to escape.
   */
  public static void printQuoted(PrintStream out,
                                 JsonValue value,
                                 boolean escape,
                                 char doubleEscapeCharacter)
  {
    if (value != null) 
    {
      String text = value.toString(); // TODO: memory; efficient JString to escaped string
      String quotedStr = quote(text, escape, doubleEscapeCharacter);
      out.print(quotedStr);
    }
  }
  
  public static void printQuotedDel(PrintStream out, JsonValue value, boolean escape) 
  {
    printQuoted(out, value, escape, DOUBLE_QUOTE);
  }
  
  public static void printQuotedJson(PrintStream out, JsonValue value)
  {
    printQuoted(out, value, true, BACK_SLASH);
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
    return com.ibm.jaql.json.type.JsonUtil.compare(x, y);
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
      int c = com.ibm.jaql.json.type.JsonUtil.compare(value1, value2);
      if (c != 0)
      {
        return c;
      }
    }
    
    return !hasNext1 ? (!hasNext2 ? 0 : -1) : 1;
  }

}
