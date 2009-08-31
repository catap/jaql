/*
 * Copyright (C) IBM Corp. 2009.
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
package com.ibm.jaql.lang.expr.system;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.StringReader;
import java.lang.reflect.UndeclaredThrowableException;

import com.ibm.jaql.json.parser.JsonParser;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/**
 * A very rough cut at calling R from Jaql.
 * 
 * R(string init, string fn, item arg1, ..., item argN)
 * 
 * A single R process is forked per RFn instance (i.e., call site in the query).
 * The R process is forked and the init string is passed to R only on the first invocation.
 * 
 * To configure R, add -DR.home=<path to R> to the VM arguments. // TODO: need jaql.conf
 * 
 * @author kbeyer
 *
 */
@JaqlFn(fnName = "R", minArgs = 2, maxArgs = Expr.UNLIMITED_EXPRS)
public class RFn extends Expr
{
  protected static String program = System.getProperty("R.home", "R");
  protected static String args = "--no-save --no-restore --slave --ess";
  protected static String cmd = program + " " + args;
  protected Process proc;
  protected BufferedReader stdout;
  protected PrintStream stdin;
  protected Throwable error;
  protected JsonParser parser;


  public RFn(Expr[] exprs)
  {
    super(exprs);
  }

  @Override
  public Item eval(Context context) throws Exception
  {
    try
    {
      if( proc == null )
      {
        init(context);
      }

      JString fn = (JString)exprs[1].eval(context).get();
      if( fn == null )
      {
        throw new RuntimeException("callR(init, fn, ...): R function required");
      }
      stdin.print("cat(toJSON("); // TODO: we should do the conversion in jaql
      stdin.print(fn);
      stdin.print("(");
      String sep = "";
      for(int i = 2 ; i < exprs.length ; i++)
      {
        Item item = exprs[i].eval(context);
        stdin.print(sep);
        stdin.print("fromJSON('"); // TODO: we should do the conversion in jaql
        item.print(stdin);
        stdin.print("')");
        sep = ",";
      }
      stdin.println(")),'\n')");
      stdin.flush();

      // parser.ReInit(stdout); // TODO: we can read directly from the stdout stream, but error reporting is not so good...
      String s = stdout.readLine();
      if( s == null )
      {
        throw new RuntimeException("unexpected EOF from R");
      }
      parser.ReInit(new StringReader(s));
      try
      {
        Item result = parser.JsonVal();
        return result;
      }
      catch( Exception e )
      {
        System.err.println("Bad JSON from R:\n"+s);
        throw e;
      }
    }
    catch (Throwable e)
    {
      if( error == null )
      {
        error = e;
      }
      if( stdin != null )
      {
        try { stdin.close(); } catch(Throwable t) {}
        stdin = null;
      }
      if( stdout != null )
      {
        try { stdout.close(); } catch(Throwable t) {}
        stdout = null;
      }
      if( proc != null )
      {
        try { proc.destroy(); } catch(Throwable t) {}
        proc = null;
      }
      if( error instanceof Exception )
      {
        throw (Exception)error;
      }
      throw new UndeclaredThrowableException(error);
    }
  }

  protected void init(Context context) throws Exception
  {
    parser = new JsonParser();
    
    proc = Runtime.getRuntime().exec(cmd);    
    InputStream is = proc.getInputStream();
    stdout = new BufferedReader(new InputStreamReader(is));
    ErrorThread errorThread = new ErrorThread();
    errorThread.start();

    OutputStream os = proc.getOutputStream();
    stdin = new PrintStream(new BufferedOutputStream(os));

    JString initStr = (JString)exprs[0].eval(context).get();
    stdin.println("sink(type='output',file=stderr())");
    stdin.println("require('rjson')");
    if( initStr != null )
    {
      stdin.println(initStr);
    }
    stdin.println("sink(type='output')");
    stdin.flush();
    
    // TODO: contexts are not getting closed properly; mapreduce creates contexts, fncall creates contexts, but not cleaned up!
    context.doAtQueryEnd(
        new Runnable()
        {
          @Override
          public void run()
          {
            try { 
              if( stdin != null )
              {
                stdin.println("q()"); 
                stdin.close();
              }
              else if( proc != null )
              {
                proc.destroy();
              }
              if( proc != null )
              {
                proc.getErrorStream().close();
                int rc = proc.waitFor();
                if( rc != 0 )
                {
                  System.err.println("non-zero exit code from process ["+cmd+"]: "+rc);
                }
              }
            } catch( Throwable t ) {}
          }
        });
  }

  protected class ErrorThread extends Thread
  {
    @Override
    public void run()
    {
      try
      {
        InputStream is = proc.getErrorStream();
        byte[] buffer = new byte[1024];
        int n;
        while( (n = is.read(buffer)) >= 0 )
        {
          System.err.write(buffer,0,n); // TODO: use logging? 
        }
        System.err.flush();
        is.close();
      }
      catch (Throwable e)
      {
        if( error == null )
        {
          error = e;
        }
        proc.destroy();
      }
    }
  }
}