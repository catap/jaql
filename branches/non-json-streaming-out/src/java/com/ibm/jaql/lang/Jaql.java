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
package com.ibm.jaql.lang;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.Reader;
import java.io.StringReader;

import jline.ConsoleReader;
import jline.ConsoleReaderInputStream;
import antlr.collections.impl.BitSet;

import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.json.util.SingleJsonValueIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.top.AssignExpr;
import com.ibm.jaql.lang.expr.top.ExplainExpr;
import com.ibm.jaql.lang.expr.top.MaterializeExpr;
import com.ibm.jaql.lang.parser.JaqlLexer;
import com.ibm.jaql.lang.parser.JaqlParser;
import com.ibm.jaql.lang.rewrite.RewriteEngine;
import com.ibm.jaql.util.ClassLoaderMgr;
import static com.ibm.jaql.json.type.JsonType.*;

public class Jaql
{
  public static void main(String args[]) throws Exception
  {
    InputStream in;
    if (args.length > 0) {
        in = new FileInputStream(args[0]);
    } else {
        try {
            in = new ConsoleReaderInputStream(new ConsoleReader());
        } catch (IOException e) {
            in = System.in;
        }
    }
    run("<stdin>",in);
    // System.exit(0); // possible jvm 1.6 work around for "JDWP Unable to get JNI 1.2 environment"
  }

  public static void run(String filename, InputStream in) throws Exception
  {
    Jaql engine = new Jaql(filename, in);
    engine.setOutput(System.out);
    engine.run();
  }

  public static void addExtensionJars(String[] jars) throws Exception
  {
    ClassLoaderMgr.addExtensionJars(jars);
  }

  //-----------------------------------------------------------------
  
  protected JaqlLexer lexer = null;
  protected JaqlParser parser = null;
  protected RewriteEngine rewriter = new RewriteEngine();
  protected Context context = new Context();
  protected boolean doRewrite = true;
  protected boolean stopOnException = false;
  protected PrintStream output = null;  // TODO: should generalize to generic output handler
  protected String prompt = "\njaql> ";
  
  static
  {
    // TODO: This was added to get Hadoop to find it's class path correctly when called from R
    Thread.currentThread().setContextClassLoader(ClassLoaderMgr.getClassLoader());
  }
  
  public Jaql()
  {
    setInput("'jaql input not set!';");
  }
  
  public Jaql(String jaql)
  {
    setInput(jaql);
  }
  
  public Jaql(String filename, InputStream in)
  {
    setInput(filename, in);
  }
  
  public Jaql(String filename, Reader in)
  {
    setInput(filename, in);
  }
  
  public void setInput(String filename, InputStream in)
  {
    lexer = new JaqlLexer(in);
    parser = new JaqlParser(lexer);
    lexer.setFilename(filename);
  }
  
  public void setInput(String filename, Reader in)
  {
    lexer = new JaqlLexer(in);
    parser = new JaqlParser(lexer);
    lexer.setFilename(filename);
  }
  
  public void setInput(String jaql)
  {
    setInput("<string>", new StringReader(jaql));
  }
  
  public void setOutput(PrintStream output)
  {
    this.output = output;
  }

  /**
   * Set the prompt displayed before each statement.  Set to "" to disable.
   * @param prompt
   */
  public void setPrompt(String prompt)
  {
    this.prompt = prompt;
  }

  /**
   * Turn on and off the query rewrite engine.
   * 
   * @param doRewrite
   */
  public void enableRewrite(boolean doRewrite)
  {
    this.doRewrite = doRewrite;
  }
  
//  public void enableMapReduce(boolean doMapReduce)
//  {
//    rewriter.enableMapReduce(doMapReduce);
//  }
  
  /**
   * True to cause an exception to stop a script
   * 
   * @param stopOnException
   */
  public void stopOnException(boolean stopOnException)
  {
    this.stopOnException = stopOnException;
  }
  
  /**
   * Set the value of a global variable.
   *  
   * @param varName
   * @param value
   */
  public void setVar(String varName, JsonValue value) 
  {
    Var var = parser.env.sessionEnv().scopeGlobal(varName);
    var.setValue(value);
  }

  /**
   * Materialize a variable if it hasn't been materialized yet.
   * 
   * @param var
   * @throws Exception
   */
  public void materializeVar(Var var) throws Exception
  {
    if( var.value == null )
    {
      try
      {
        Expr e = new MaterializeExpr(var);
        if( doRewrite )
        {
          e = rewriter.run(parser.env, e);
        }
        e.eval(context);
      }
      catch( Throwable error )
      {
        handleError(error);
      }
    }
  }

  /**
   * Get a the value of a global variable.
   * 
   * @param varName
   * @throws Exception 
   * @throws Exception 
   */
  public JsonValue getVarValue(String varName) throws Exception 
  {
    Var var = parser.env.sessionEnv().inscope(varName);
    materializeVar(var);
    JsonValue value = var.getValue(context); // TODO: use global context?
    return value;
  }

  /**
   * Get an iterator over the value of a global variable.
   * 
   * @param varName
   * @throws Exception 
   * @throws Exception 
   */
  public JsonIterator getVarIter(String varName) throws Exception 
  {
    Var var = parser.env.sessionEnv().inscope(varName);
    if( var.value != null )
    {
      JsonIterator iter = var.iter(context);
      return iter;
    }
    // TODO: the expression needs to be optimized!
    JsonIterator iter = var.iter(context);
    return iter;
  }

//  /**
//   * Get an iterator over the value of a global variable.
//   * 
//   * @param varName
//   * @param args 
//   * @throws Exception 
//   * @throws Exception 
//   */
//  public JsonIterator invokeFunctionIter(String varName, JsonValue[] args) throws Exception 
//  {
//    Var var = parser.env.sessionEnv().inscope(varName);
//    JsonValue value = var.getValue(context);
//    JaqlFunction fn = (JaqlFunction)value;
//    JsonIterator iter = fn.iter(context, args);
//    return iter;
//  }

  /**
   * Prepares the next evaluable statement.
   * 
   * If the next statement is an assignment, explain, or empty statement, it is
   * processed and the next statement considered.
   * 
   * If there is trouble parsing or compiling the statement, handleError()
   * is called to process the error.  handleError() may rethrow the exception
   * or it may simply log the error and prepareNext() move on to the next statement. 
   * 
   * Returns <tt>null</tt> at end of script.
   * 
   * @return
   * @throws Exception
   */
  public Expr prepareNext() throws Exception
  {
    Expr expr;
    while( true )
    {
      parser.env.reset();
      context.reset(); // close the last query, if still open
      try
      {
        if( output != null )
        {
          output.print(prompt);
        }
        expr = parser.parse();
      }
      catch (Throwable error)
      {
        BitSet bs = new BitSet();
        bs.add(JaqlParser.EOF);
        bs.add(JaqlParser.SEMI);
        parser.consumeUntil(bs);
        handleError(error);
        expr = null;
      }
      
      if( parser.done )
      {
        return null;
      }
      
      if( expr != null )
      {
        if( doRewrite )
        {
          try
          {
            expr = rewriter.run(parser.env, expr);
          }
          catch( Throwable error )
          {
            expr = null;
            handleError(error);
          }
        }

        if( expr instanceof AssignExpr )
        {
          expr.eval(context);
        }
        else if( expr instanceof ExplainExpr )
        {
          JsonValue value = expr.eval(context);
          System.out.println(value);
        }
        else if( expr != null )
        {
          return expr;
        }
      }
    }
  }
  
  /**
   * Evaluate the next query in the script and return an iterator over the result.
   * If the result is not an array or null, it is coerced into an array.
   * If there is no such query, return null.
   * 
   * @return
   * @throws Exception 
   */
  public JsonIterator iter() throws Exception
  {
    Expr expr = prepareNext();
    if( expr == null )
    {
      return null;
    }
    context.reset();
    JsonIterator iter;
    if( expr.getSchema().is(ARRAY, NULL).always() )
    {
      iter = expr.iter(context);
    }
    else
    {
      JsonValue value = expr.eval(context);
      iter = new SingleJsonValueIterator(value);
    }
    return iter;
  }

  /**
   * Evaluate the next query in the script and return the result.
   * If there is no such query, return null (which is ambiguous with
   * a query that returns null).
   * 
   * @return
   * @throws Exception 
   */
  public JsonValue evalNext() throws Exception
  {
    Expr expr = prepareNext();
    if( expr == null )
    {
      return null;
    }
    context.reset();
    JsonValue value = expr.eval(context);
    return value;
  }

  /**
   * Run the script and print any output to out.
   * If an error is detected, handleError() is called to process it.
   * handleError may rethrow the exception. 
   * It is safe to call run again to resume the script.
   * 
   * @return <tt>true</tt> if we hit the end of the script (otherwise exception)
   * @throws Exception 
   */
  public boolean run() throws Exception
  {
    Expr expr;
    while( (expr = prepareNext()) != null )
    {
      try
      {
        if (expr.getSchema().is(ARRAY, NULL).always())
        {
          JsonIterator iter = expr.iter(context);
          iter.print(output);
        }
        else
        {
          JsonValue value = expr.eval(context);
          JsonUtil.print(output, value);
        }
        output.println();
        output.flush();
      }
      catch( Throwable error )
      {
        handleError(error);
      }
      finally
      {
        context.reset(); 
      }
    }
    return true;
  }

  /**
   * Process an exception during a run() call.  
   * 
   * @param ex
   * @throws Exception 
   */
  protected void handleError(Throwable error) throws Exception
  {
    System.err.println(error);
    error.printStackTrace();
    System.err.flush();
    if( stopOnException )
    {
      if( error instanceof Exception )
      {
        throw (Exception)error;
      }
      else if( error instanceof Error )
      {
        throw (Error)error;
      }
      throw new RuntimeException(error);
    }
  }
}
