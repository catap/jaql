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
package com.ibm.jaql.io.hadoop;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.parser.JaqlLexer;
import com.ibm.jaql.lang.parser.JaqlParser;
import com.ibm.jaql.lang.util.JaqlUtil;

/** Provides static methods that serializes and deserializes {@link JsonRecord}s and 
 * {@link JsonArray}s to and from the Hadoop configuration file */
public class ConfUtil
{
  /**
   * Write a text serialized form of args to the conf under the given name.
   * 
   * @param conf
   * @param name
   * @param args
   * @throws Exception
   */
  public static void writeConf(Configuration conf, String name, JsonRecord args)
      throws Exception
  {
    if (args == null) return;

    ByteArrayOutputStream bstr = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(bstr);
    JsonUtil.print(out, args);
    out.flush();
    out.close();
    conf.set(name, bstr.toString()); // FIXME: memory and strings...
  }

  /**
   * Read a text serialized form located in the conf under name into a JRecord.
   * 
   * @param conf
   * @param name
   * @return
   * @throws Exception
   */
  public static JsonRecord readConf(Configuration conf, String name) throws Exception
  {
    String jsonTxt = conf.get(name);
    if (jsonTxt == null) return null;
    ByteArrayInputStream input = new ByteArrayInputStream(jsonTxt.getBytes());

    // TODO: cannot use JsonParser because of schema
//    JsonParser parser = new JsonParser(input);
//    JsonValue data = parser.TopVal();
    JaqlLexer lexer = new JaqlLexer(input);
    JaqlParser parser = new JaqlParser(lexer);
    Expr expr = parser.parse();
    JsonValue data = JaqlUtil.enforceNonNull(expr.compileTimeEval());

    return (JsonRecord) data;
  }

  /**
   * @param conf
   * @param name
   * @return
   * @throws Exception
   */
  public static JsonArray readConfArray(Configuration conf, String name)
      throws Exception
  {
    String jsonTxt = conf.get(name);
    if (jsonTxt == null) return null;
    ByteArrayInputStream input = new ByteArrayInputStream(jsonTxt.getBytes());

    // TODO: cannot use JsonParser because of schema
    //    JsonParser parser = new JsonParser(input);
    //    JsonValue data = parser.TopVal();
    JaqlLexer lexer = new JaqlLexer(input);
    JaqlParser parser = new JaqlParser(lexer);
    Expr expr = parser.parse();
    JsonValue data = JaqlUtil.enforceNonNull(expr.compileTimeEval());
    return (JsonArray) data;
  }

  /**
   * @param conf
   * @param name
   * @param data
   * @throws Exception
   */
  public static void writeConfArray(Configuration conf, String name, JsonArray data)
      throws Exception
  {
    if (data == null) return;

    ByteArrayOutputStream bstr = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(bstr);
    JsonArray mdata = (JsonArray) data; // FIXME: don't depend on this...
    JsonUtil.print(out, mdata);
    out.flush();
    out.close();
    conf.set(name, bstr.toString());
  }
  
  /**
   * Write a binary string to a conf 
   */
  public static void writeBinary(Configuration conf, String name, byte[] bytes, int offset, int length)
  {
    StringBuilder s = new StringBuilder(length * 2);
    for(int i = 0 ; i < length ; i++)
    {
      byte b = bytes[i + offset];
      s.append( (char)( ((b >> 4) & 0x0f) + 'a') );
      s.append( (char)( ((b     ) & 0x0f) + 'a') );
    }
    conf.set(name, s.toString());
  }

  /**
   * Read a binary string from a conf 
   */
  public static byte[] readBinary(Configuration conf, String name)
  {
    String s = conf.get(name);
    if( s == null )
    {
      return null;
    }
    byte[] bytes = new byte[s.length()/2];
    for( int i = 0, j = 0 ; i < bytes.length ; i++, j += 2 )
    {
      int c1 = s.charAt(j) - 'a';
      int c2 = s.charAt(j+1) - 'a';
      bytes[i] = (byte)((c1 << 4) | c2);
    }
    return bytes;
  }

  /**
   * Set each key:value from rec in the conf
   */
  public static void setConf(Configuration conf, JsonRecord rec)
  {
    if( rec != null )
    {
      for( Map.Entry<JsonString, JsonValue> e: rec )
      {
        conf.set(e.getKey().toString(), e.getValue().toString());
      }
    }
  }
}
