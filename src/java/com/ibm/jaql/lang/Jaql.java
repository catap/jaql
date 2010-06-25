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

import static com.ibm.jaql.json.type.JsonType.ARRAY;
import static com.ibm.jaql.json.type.JsonType.NULL;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Reader;
import java.io.StringReader;

import org.apache.commons.lang.BooleanUtils;

import antlr.TokenStreamException;
import antlr.collections.impl.BitSet;

import com.ibm.jaql.io.OutputAdapter;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.json.util.SingleJsonValueIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.io.RegisterAdapterExpr;
import com.ibm.jaql.lang.expr.top.AssignExpr;
import com.ibm.jaql.lang.expr.top.ExplainExpr;
import com.ibm.jaql.lang.expr.top.MaterializeExpr;
import com.ibm.jaql.lang.expr.top.QueryExpr;
import com.ibm.jaql.lang.parser.JaqlLexer;
import com.ibm.jaql.lang.parser.JaqlParser;
import com.ibm.jaql.lang.rewrite.RewriteEngine;
import com.ibm.jaql.lang.rewrite.VarTagger;
import com.ibm.jaql.util.ClassLoaderMgr;

public class Jaql implements CoreJaql
{
  public static String ENV_JAQL_HOME = "JAQL_HOME"; 
  
  public static void main(String args[]) throws Exception
  {
    InputStream in;
    if (args.length > 0) {
        in = new FileInputStream(args[0]);
    } else {
      in = System.in;
    }
    run("<stdin>", new InputStreamReader(in, "UTF-8"));
    // System.exit(0); // possible jvm 1.6 work around for "JDWP Unable to get JNI 1.2 environment"
  }

  public static void run(String filename,
                         Reader in) throws Exception
  {
    run(filename, in, null, null, false);
  }
  
  public static void run(String filename,
                         Reader reader,
                         OutputAdapter outputAdapter,
                         OutputAdapter logAdapter,
                         boolean batchMode) throws Exception
  {
    Jaql engine = new Jaql(filename, reader);
    if (outputAdapter == null) {
      PrintStream out = new PrintStream(System.out, true, "UTF-8"); // TODO: use system encoding? have jaql encoding parameter?
      engine.setJaqlPrinter(new StreamPrinter(out, batchMode));
    } else {
      engine.setJaqlPrinter(new IODescriptorPrinter(outputAdapter.getWriter()));
    }
    
    if (logAdapter != null)
    {
      engine.setExceptionHandler(new JsonWriterExceptionHandler(logAdapter.getWriter()));
    }
    
    engine.run();
  }

  public static void addExtensionJars(String[] jars) throws Exception
  {
    ClassLoaderMgr.addExtensionJars(jars);
  }
  
  public void addJar(String path) throws Exception {
	  ClassLoaderMgr.addExtensionJars(new String[]{path});
  }

  //-----------------------------------------------------------------
  
  protected JaqlLexer lexer = null;
  protected JaqlParser parser = null;
  protected final Context context = new Context();
  protected final Env env = new Env(context);
  protected RewriteEngine rewriter = new RewriteEngine();
  protected boolean doRewrite = true;
  protected boolean stopOnException = false;
  protected String explainMode = System.getProperty("jaql.explain.mode"); // eventually more modes: jaql, graphical, json, logJaql?
  protected boolean explainOnly = "jaql".equals(explainMode);
  
  protected JaqlPrinter printer = NullPrinter.get();
  protected ExceptionHandler exceptionHandler = new DefaultExceptionHandler();
  protected ExplainHandler explainHandler = new DefaultExplainHandler(System.out);
  
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
  
//  public Jaql(String filename, InputStream in)
//  {
//    setInput(filename, in);
//  }
  
  public Jaql(String filename, Reader in)
  {
    setInput(filename, in);
  }
  
  public void close() throws IOException
  {
    context.reset();
    explainHandler.close();
    printer.close();
    exceptionHandler.close();
  }
  
  public void setInput(String filename, InputStream in)
  {
    lexer = new JaqlLexer(in);
    parser = new JaqlParser(lexer);
    parser.env = env;
    lexer.setFilename(filename);
  }
  
  public void setInput(String filename, Reader in)
  {
    lexer = new JaqlLexer(in);
    parser = new JaqlParser(lexer);
    parser.env = env;
    lexer.setFilename(filename);
  }
  
  public void setInput(String jaql)
  {
    setInput("<string>", new StringReader(jaql));
  }
  
//  public void setError(ClosableJsonWriter writer)
//  {
//    log = writer;
//  }
  
  /**
   * Turn on and off the query rewrite engine.
   * 
   * @param doRewrite
   */
  public void enableRewrite(boolean doRewrite)
  {
    this.doRewrite = doRewrite;
  }
  
  public void setJaqlPrinter(JaqlPrinter printer) {
    this.printer = printer;
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
  
  public void setExplainOnly(boolean explainOnly)
  {
    this.explainOnly = explainOnly;
  }
  
  public void setExplainHandler(ExplainHandler explainHandler)
  {
    this.explainHandler = explainHandler;
  }
  
  public void setExceptionHandler(ExceptionHandler exceptionHandler)
  {
    this.exceptionHandler = exceptionHandler;
  }
  
  public void setProperty(String name, String value) {
	  if(name.equalsIgnoreCase("enableRewrite")){
		  doRewrite = BooleanUtils.toBoolean(value);
	  }else if(name.equalsIgnoreCase("stopOnException")){
		  stopOnException = BooleanUtils.toBoolean(value);
	  }else if(name.equalsIgnoreCase("JaqlPrinter")){
		  try{
			  printer = (JaqlPrinter)Class.forName(value).newInstance();
		  }catch(Exception ex){
			  ex.printStackTrace();
		  }
	  }
  }
  
  public String getProperty(String name) {
	  if(name.equalsIgnoreCase("enableRewrite")){
		  return BooleanUtils.toStringTrueFalse(doRewrite);
	  }else if(name.equalsIgnoreCase("stopOnException")){
		  return BooleanUtils.toStringTrueFalse(stopOnException);
	  }else if(name.equalsIgnoreCase("JaqlPrinter") && printer != null){
		  return printer.getClass().getName();
	  }else{
		  return null;
	  }
  }
  
  /**
   * Set the value of a global variable.
   *  
   * @param varName
   * @param value
   */
  public void setVar(String varName, JsonValue value) 
  {
    parser.env.scopeGlobal(varName, value);    
  }
  
  public void setVar(String varName, JsonIterator iter) {
	  try{
		  if(iter.moveNext())
			  setVar(varName, iter.current());
	  }catch(Exception ex){
		  ex.printStackTrace();
	  }
  }

  /**
   * Materialize a variable if it hasn't been materialized yet.
   * 
   * @param var
   * @throws Exception
   */
  public void materializeVar(Var var) throws Exception
  {
    if( var.type() == Var.Type.EXPR )
    {
      try
      {
        Expr e = new MaterializeExpr(var);
        e = new QueryExpr(parser.env, e);
        e = env.importGlobals(e);
        if( doRewrite )
        {
          e = rewriter.run(e);
        }
        e.eval(context);
      }
      catch( Throwable error )
      {
        handleError(error);
      }
    }
  }
  
  public void materializeVar(String name) throws Exception {
	  materializeVar(new Var(name));
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
    Var var = parser.env.inscope(varName);
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
    Var var = parser.env.inscope(varName);
    if( var.type() != Var.Type.EXPR )
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

  public Expr expr() throws Exception
  {
	  return prepareNext();
  }
  
  public String explain() throws Exception
  {
    Expr expr = prepareNext();
    expr = explainHandler.explain(expr);
    String s = "(explain unavailable)";
    if( expr != null )
    {
      s = expr.eval(context).toString();
    }
    return s;
  }
  
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
    nextStmt: while( true )
    {
      parser.env.reset();
      context.reset(); // close the last query, if still open
      
      try
      {
        printer.printPrompt();
        expr = parser.parse();
        if( parser.done )
        {
          return null;
        }
        if( expr == null )
        {
          continue nextStmt;
        }
      }
      catch (Throwable error)
      {
        BitSet bs = new BitSet();
        bs.add(JaqlParser.EOF);
        bs.add(JaqlParser.SEMI);
        while(true)
        {
          try
          {
            parser.consumeUntil(bs);
            break;
          }
          catch( TokenStreamException tse )
          {
            lexer.consume();
          }
        }
        handleError(error);
        continue nextStmt;
      }

      try
      {
//        if( explainOnly && 
//            !(expr instanceof AssignExpr) &&
//            !(expr instanceof QueryExpr && expr.child(0) instanceof RegisterAdapterExpr) && // HACK: if we don't register, explain will change or bomb. This will go away with the registry.
//            !(expr instanceof ExplainExpr) )
//        {
//          expr = new ExplainExpr(expr);
//        }
          
        if( doRewrite )
        {
          expr = rewriter.run(expr);
        }

        VarTagger.tag(expr);

        if( explainOnly || expr instanceof ExplainExpr )
        {
          expr = explainHandler.explain(expr);
        }

        if( expr instanceof AssignExpr ||
            expr instanceof QueryExpr && expr.child(0) instanceof RegisterAdapterExpr ) // HACK: if we don't register, explain will change or bomb. This will go away with the registry.
        {
          expr.eval(context);
        }
        else if( expr != null )
        {
          return expr;
        }

//        if( expr instanceof AssignExpr ||
//            expr instanceof QueryExpr && expr.child(0) instanceof RegisterAdapterExpr ) // HACK: if we don't register, explain will change or bomb. This will go away with the registry.
//        {
//          expr.eval(context);
//          if( explainOnly && 
//              expr instanceof QueryExpr && expr.child(0) instanceof RegisterAdapterExpr ) // HACK: if we don't register, explain will change or bomb. This will go away with the registry.
//          {
//            expr = explainHandler.explain(expr);
//          }
//        }
//        else 
//        {
//          if( expr instanceof ExplainExpr )
//          {
//            expr = explainHandler.explain(expr);
//          }
//          if( expr != null )
//          {
//            return expr;
//          }
//        }
      }
      catch( Throwable error )
      {
        handleError(error);
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
  
  public JsonValue eval() throws Exception {
	  return evalNext();
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
        printer.print(expr, context);
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
    exceptionHandler.handleException(error);
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
