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
import java.io.PrintStream;
import java.io.StringReader;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.HashSet;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JAtom;
import com.ibm.jaql.json.type.JValue;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.expr.core.DefineFunctionExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.parser.JaqlLexer;
import com.ibm.jaql.lang.parser.JaqlParser;

/**
 * 
 */
public class JFunction extends JAtom
{
  protected DefineFunctionExpr fn; // cannot have any captured variables
  protected boolean ownFn; // true if we own the fn, and therefore must init/close it.
  protected String fnText;

  /**
   * 
   */
  public JFunction()
  {
  }

  /**
   * @param params
   * @param body
   * @throws Exception
   */
  public JFunction(DefineFunctionExpr fn, boolean ownFn) throws Exception
  {
    set(fn, ownFn);
  }

  /**
   * @param params
   * @param body
   * @throws Exception
   */
  public void set(DefineFunctionExpr fn, boolean ownFn) throws Exception
  {
    this.fn = fn;
    this.ownFn = ownFn;

    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(outStream);
    HashSet<Var> capturedVars = new HashSet<Var>();
    fn.decompile(ps, capturedVars);
    assert capturedVars.size() == 0;
    this.fnText = outStream.toString();
  }

  public void set(String fnText) throws Exception
  {
    JaqlLexer lexer = new JaqlLexer(new StringReader(fnText)); // TODO: memory
    JaqlParser parser = new JaqlParser(lexer); // TODO: memory

    try
    {
      fn = (DefineFunctionExpr)parser.parse();
    }
    catch(Exception e)
    {
      fn = null;
      fnText = null;
      throw new UndeclaredThrowableException(e);
    }
    fn.annotate();
    ownFn = true;
  }
  
  /**
   * @return
   */
  public int getNumParameters()
  {
    return fn.numParams();
  }

  /**
   * @return
   */
  public DefineFunctionExpr getFunction()
  {
    return fn;
  }

  /**
   * @return
   */
  public Expr getBody()
  {
    return fn.body();
  }
  
  /**
   * @param i
   * @return
   */
  public Var param(int i)
  {
    return fn.param(i).var;
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

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JAtom#print(java.io.PrintStream)
   */
  @Override
  public void print(PrintStream out)
  {
    out.print(fnText);
  }
  
  public void checkArgs(int numArgs)
  {
    int n = fn.numParams();
    if (n != numArgs)
    {
      throw new RuntimeException(
          "wrong number of arguments to function.  Expected " + n + 
          " but given " + numArgs + 
          "\nfunction:\n" + fnText);
    }
  }

  public void setParams(Item[] args, int offset, int length)
  {
    checkArgs(length);
    int p = fn.numParams();
    for(int i = 0 ; i < p ; i++)
    {
      fn.param(i).var.setValue(args[offset + i]);
    }
  }

  public void setParams(Context context, Expr[] args, int offset, int length) throws Exception
  {
    checkArgs(length);
    int p = fn.numParams();
    for(int i = 0 ; i < p ; i++)
    {
      fn.param(i).var.setEval(args[offset + i], context);
    }
  }

  /**
   * Requires:
   *    * all parameters have been set using:
   *        * setParams(...)
   *        * param(i).set...
   * 
   * @param context
   * @return
   * @throws Exception
   */
  public Item eval(Context context) throws Exception
  {
    return fn.body().eval(context);
  }

  /**
   * @param context
   * @param args
   * @return
   * @throws Exception
   */
  public Item eval(Context context, Item[] args) throws Exception
  {
    setParams(args, 0, args.length);
    return eval(context);
  }

  /**
   * @param context
   * @param args
   * @param start index of first args to use
   * @param length number of args to use
   * @return
   * @throws Exception
   */
  public Item eval(Context context, Expr[] args, int offset, int length) throws Exception
  {
    setParams(context, args, offset, length);
    return eval(context);
  }

  /**
   * Requires:
   *    * all parameters have been set using:
   *        * setParams(...)
   *        * param(i).set...
   * 
   * @param context
   * @return
   * @throws Exception
   */
  public Iter iter(Context context) throws Exception
  {
    return fn.body().iter(context);
  }


  /**
   * @param context
   * @param args
   * @return
   * @throws Exception
   */
  public Iter iter(Context context, Item[] args) throws Exception
  {
    setParams(args, 0, args.length);
    return iter(context);
  }

  /**
   * @param context
   * @param args
   * @return
   * @throws Exception
   */
  public Iter iter(Context context, Expr[] args, int start, int length) throws Exception
  {
    setParams(context, args, start, length);
    return iter(context);
  }

  /**
   * 
   * @param context
   * @param args
   * @return
   * @throws Exception
   */
  public Iter iter(Context context, Expr[] args) throws Exception
  {
    return iter(context, args, 0, args.length);
  }

  /**
   * 
   * @param context
   * @param arg0
   * @param arg1
   * @return
   * @throws Exception
   */
  public Iter iter(Context context, Item arg0, Item arg1) throws Exception
  {
    checkArgs(2);
    param(0).setValue(arg0);
    param(0).setValue(arg1);
    return iter(context);
  }

  /**
   * 
   * @param context
   * @param arg0
   * @return
   * @throws Exception
   */
  public Iter iter(Context context, Iter arg0) throws Exception
  {
    checkArgs(1);
    param(0).setIter(arg0);
    return iter(context);
  }

  /**
   * 
   * @param context
   * @param arg0
   * @param arg1
   * @return
   * @throws Exception
   */
  public Iter iter(Context context, Item arg0, Iter arg1) throws Exception
  {
    checkArgs(2);
    param(0).setValue(arg0);
    param(1).setIter(arg1);
    return iter(context);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#copy(com.ibm.jaql.json.type.JValue)
   */
  @Override
  public void setCopy(JValue jvalue) throws Exception
  {
    JFunction f = (JFunction) jvalue;
    this.fn = (DefineFunctionExpr)f.fn.clone(new VarMap());
    this.ownFn = true;
    this.fnText = f.fnText;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#toJSON()
   */
  @Override
  public String toJSON()
  {
    return fnText; // TODO: wrap in constructor?
  }

}
