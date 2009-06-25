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

import jline.ConsoleReader;
import jline.ConsoleReaderInputStream;

import antlr.collections.impl.BitSet;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.top.ExplainExpr;
import com.ibm.jaql.lang.parser.JaqlLexer;
import com.ibm.jaql.lang.parser.JaqlParser;
import com.ibm.jaql.lang.rewrite.RewriteEngine;
import com.ibm.jaql.util.ClassLoaderMgr;

public class Jaql
{
  public static void main(String av[]) throws Exception
  {
    main1(av);
    //dump(0);
    System.exit(0); // possible jvm 1.6 work around for "JDWP Unable to get JNI 1.2 environment"
  }

//  public static void main2(String av[]) throws Exception
//  {
//    InputStream input = av.length > 0 ? new FileInputStream(av[0]) : System.in;
//    Jaql2Lexer lexer = new Jaql2Lexer(input);
//    Jaql2Parser parser = new Jaql2Parser(lexer);
//    Context context = new Context();
//    while (true)
//    {
//      System.out.print("\njaql2> ");
//      System.out.flush();
//      parser.env.reset();
//      Expr expr = parser.parse();
//      if (parser.done)
//      {
//        break;
//      }
//      else if( expr != null )
//      {
//        context.reset();
//        // TODO: enable push style?
//        // expr.write(context, writer);
//        if (expr instanceof ExplainExpr) // TODO: statement.eval
//        {
//          Item item = expr.eval(context);
//          System.out.println(item.get());
//        }
//        else if (expr.isArray().always())
//        {
//          Iter iter = expr.iter(context);
//          iter.print(System.out);
//        }
//        else
//        {
//          Item item = expr.eval(context);
//          item.print(System.out);
//        }
//      }
//    }
//  }

  public static void main1(String[] args) throws Exception
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
    main(in);
  }
  
  public static void main(InputStream in) throws Exception {
    JaqlLexer lexer = new JaqlLexer(in);
    JaqlParser parser = new JaqlParser(lexer);
    Context context = new Context();
    RewriteEngine rewriter = new RewriteEngine();
    boolean parsing = false;
    //PrintTableWriter writer = new PrintTableWriter(System.out);

    while (true)
    {
      try
      {
        System.out.print("\njaql> ");
        System.out.flush();
        parser.env.reset();
        parsing = true;
        Expr expr = parser.parse();
        parsing = false;
        if (parser.done)
        {
          break;
        }
        if (expr == null)
        {
          continue;
        }
        expr = rewriter.run(parser.env, expr);
        context.reset();
        // TODO: enable push style?
        // expr.write(context, writer);
        if (expr instanceof ExplainExpr) // TODO: statement.eval
        {
          Item item = expr.eval(context);
          System.out.println(item.get());
        }
        else if (expr.isArray().always())
        {
          Iter iter = expr.iter(context);
          iter.print(System.out);
        }
        else
        {
          Item item = expr.eval(context);
          item.print(System.out);
        }
        context.endQuery(); // TODO: need to wrap up parse, eval, cleanup into one class and use everywhere
      }
      catch (Exception ex)
      {
        System.err.println(ex);
        ex.printStackTrace();
        System.err.flush();
        if (parsing)
        {
          BitSet bs = new BitSet();
          bs.add(JaqlParser.EOF);
          bs.add(JaqlParser.SEMI);
          parser.consumeUntil(bs);
        }
      }
    }
  }
  
  public static void addExtensionJars(String[] jars) throws Exception
  {
    ClassLoaderMgr.addExtensionJars(jars);
  }

}
