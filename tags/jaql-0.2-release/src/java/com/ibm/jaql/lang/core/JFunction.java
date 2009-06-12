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
package com.ibm.jaql.lang.core;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.UndeclaredThrowableException;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JAtom;
import com.ibm.jaql.json.type.JValue;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.expr.core.Expr;

/**
 * 
 */
public class JFunction extends JAtom
{
  protected FunctionDef def            = new FunctionDef();
  protected Item[]      capturedValues = Item.NO_ITEMS;

  // call readFields()
  /**
   * 
   */
  public JFunction()
  {
  }

  /**
   * @param fnVar
   * @param params
   * @param body
   * @throws Exception
   */
  public JFunction(Var fnVar, Var[] params, Expr body) throws Exception
  {
    set(fnVar, params, body);
  }

  /**
   * @param fnVar
   * @param params
   * @param body
   * @throws Exception
   */
  public void set(Var fnVar, Var[] params, Expr body) throws Exception
  {
    def.set(fnVar, params, body);
    capturedValues = new Item[def.getNumCaptures()];
  }

  /**
   * @return
   */
  public int getNumParameters()
  {
    return def.getNumParameters();
  }

  /**
   * @param i
   * @return
   */
  public Var param(int i)
  {
    return def.param(i);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#compareTo(java.lang.Object)
   */
  @Override
  public int compareTo(Object x)
  {
    // TODO: how are functions compared?  based on serialized text?
    throw new RuntimeException("functions cannot be compared");
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#getEncoding()
   */
  @Override
  public Item.Encoding getEncoding()
  {
    return Item.Encoding.FUNCTION;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#longHashCode()
   */
  @Override
  public long longHashCode()
  {
    throw new RuntimeException("functions cannot be hashed");
  }

  //  public void setBody(Expr body)
  //  {
  //    def.body = body;
  //    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
  //    PrintStream ps = new PrintStream(outStream);
  //    HashSet<Var> capturedVars = new HashSet<Var>();
  //    try
  //    {
  //      body.decompile(ps, capturedVars);
  //    }
  //    catch( Exception e )
  //    {
  //      throw new UndeclaredThrowableException(e);
  //    }
  //    capturedVars.remove(def.fnVar);
  //    for (Var p : def.params)
  //    {
  //      capturedVars.remove(p);
  //    }
  //    def.bodyText = outStream.toString();
  //    def.capturedVars = capturedVars.toArray(new Var[capturedVars.size()]);
  //    capturedValues = new Item[capturedVars.size()];
  //  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JAtom#print(java.io.PrintStream)
   */
  @Override
  public void print(PrintStream out)
  {
    try
    {
      def.print(out, capturedValues);
    }
    catch (Exception e)
    {
      throw new UndeclaredThrowableException(e);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#readFields(java.io.DataInput)
   */
  @Override
  public void readFields(DataInput in) throws IOException
  {
    try
    {
      def = new FunctionDef(); // TODO: reuse if not shared
      capturedValues = def.read(in, capturedValues);
    }
    catch (IOException ex)
    {
      throw ex;
    }
    catch (Exception ex)
    {
      throw new UndeclaredThrowableException(ex);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#write(java.io.DataOutput)
   */
  @Override
  public void write(DataOutput out) throws IOException
  {
    def.write(out, capturedValues);
  }

  /**
   * @param context
   * @param args
   * @return
   * @throws Exception
   */
  public Item eval(Context context, Item[] args) throws Exception
  {
    return def.eval(context, capturedValues, args);
  }

  /**
   * @param context
   * @param args
   * @return
   * @throws Exception
   */
  public Item eval(Context context, Expr[] args) throws Exception
  {
    return def.eval(context, capturedValues, args);
  }

  /**
   * @param context
   * @param args
   * @return
   * @throws Exception
   */
  public Iter iter(Context context, Item[] args) throws Exception
  {
    return def.iter(context, capturedValues, args);
  }

  /**
   * @param context
   * @param args
   * @return
   * @throws Exception
   */
  public Iter iter(Context context, Expr[] args) throws Exception
  {
    return def.iter(context, capturedValues, args);
  }

  /**
   * @param context
   * @throws Exception
   */
  public void capture(Context context) throws Exception
  {
    def.capture(context, capturedValues);
  }

  /**
   * @return
   */
  public Expr getBody()
  {
    return def.getBody();
  }

  //  public JFunction clone()
  //  {
  //    try
  //    {
  //      JFunction fn = new JFunction();
  //      fn.copy(this);
  //      return fn;
  //    }
  //    catch(Exception e)
  //    {
  //      throw new UndeclaredThrowableException(e);
  //    }
  //  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#copy(com.ibm.jaql.json.type.JValue)
   */
  @Override
  public void copy(JValue jvalue) throws Exception
  {
    JFunction fn = (JFunction) jvalue;
    def = fn.def;
    int n = fn.capturedValues.length;
    if (capturedValues.length != n)
    {
      if (fn.capturedValues.length == 0)
      {
        capturedValues = Item.NO_ITEMS;
      }
      else
      {
        capturedValues = new Item[n];
      }
    }
    for (int i = 0; i < n; i++)
    {
      capturedValues[i] = new Item();
      capturedValues[i].copy(fn.capturedValues[i]);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#toJSON()
   */
  @Override
  public String toJSON()
  {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(baos);
    try
    {
      print(out);
    }
    catch (Exception e)
    {
      throw new UndeclaredThrowableException(e);
    }
    out.flush();
    return baos.toString();
  }
}
