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
package com.ibm.jaql;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.HashSet;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import antlr.TokenStreamException;
import antlr.collections.impl.BitSet;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.top.AssignExpr;
import com.ibm.jaql.lang.parser.JaqlLexer;
import com.ibm.jaql.lang.parser.JaqlParser;
import com.ibm.jaql.lang.registry.RNGStore;
import com.ibm.jaql.lang.rewrite.RewriteEngine;
import com.ibm.jaql.lang.util.JaqlUtil;
import com.ibm.jaql.util.TeeInputStream;
import com.ibm.jaql.util.UtilForTest;

/**
 * 
 */
public abstract class JaqlBaseTestCase extends TestCase
{

  private static final Log LOG             = LogFactory
                                               .getLog(JaqlBaseTestCase.class
                                                   .getName());

  public static String     FAILURE         = "FAILURE";

  private String           m_queryFileName = null;
  private String           m_tmpFileName   = null;
  private String           m_goldFileName  = null;
  private String           m_decompileName = null;
  private String           m_rewriteName   = null;

  private HashSet<Var>     captures        = new HashSet<Var>();

  /*
   * (non-Javadoc)
   * 
   * @see junit.framework.TestCase#setUp()
   */
  protected abstract void setUp() throws IOException;

  /**
   * @param prefix
   */
  protected void setFilePrefix(String prefix)
  {
    m_queryFileName = System.getProperty("test.cache.data") + File.separator
        + prefix + "Queries.txt";
    m_tmpFileName = System.getProperty("test.cache.data") + File.separator
        + prefix + "Tmp.txt";
    m_goldFileName = System.getProperty("test.cache.data") + File.separator
        + prefix + "Gold.txt";
    m_decompileName = System.getProperty("test.cache.data") + File.separator
        + prefix + "Decompile.txt";
    m_rewriteName = System.getProperty("test.cache.data") + File.separator
        + prefix + "Rewrite.txt";
  }

  /*
   * (non-Javadoc)
   * 
   * @see junit.framework.TestCase#tearDown()
   */
  protected abstract void tearDown() throws IOException;

  /**
   * @param inputFileName
   * @param outputFileName
   * @throws FileNotFoundException
   * @throws IOException
   */
  protected void evaluateQueries(String inputFileName, String outputFileName)
      throws FileNotFoundException, IOException
  {
    // open input and output streams
    InputStream input = new FileInputStream(inputFileName);
    input = new TeeInputStream(input, System.err);

    PrintStream oStr = new PrintStream(new FileOutputStream(outputFileName));
    TeeInputStream teeQueries = new TeeInputStream(input, oStr);

    PrintStream dStr = new PrintStream(new FileOutputStream(m_decompileName));
    TeeInputStream teeDecompile = new TeeInputStream(teeQueries, dStr);

    PrintStream rStr = new PrintStream(new FileOutputStream(m_rewriteName));
    TeeInputStream teeRewrite = new TeeInputStream(teeDecompile, rStr);

    // setup the parser
    JaqlLexer lexer = new JaqlLexer(teeRewrite);
    JaqlParser parser = new JaqlParser(lexer);
    Context context = new Context();
    boolean parsing = false;

    // saves and restores for rng's
    HashMap<JsonValue, RNGStore.RNGEntry> qRngMap = new HashMap<JsonValue, RNGStore.RNGEntry>();
    HashMap<JsonValue, RNGStore.RNGEntry> dRngMap = new HashMap<JsonValue, RNGStore.RNGEntry>();
    HashMap<JsonValue, RNGStore.RNGEntry> rRngMap = new HashMap<JsonValue, RNGStore.RNGEntry>();

    int qNum = 0;
    // consume the input
    while (true)
    {
      try
      {
        System.err.println("\n\nParsing query at " + inputFileName + ":"
            + lexer.getLine());
        parser.env.reset();
        parsing = true;
        Expr expr = parser.parse();
        parsing = false;
        System.err.println("\n\nParsed query at " + inputFileName + ":"
            + lexer.getLine());
        oStr.flush();
        if (parser.done)
        {
          break;
        }
        if (expr == null)
        {
          continue;
        }
        captures.clear();
        System.err.println("\nDecompiled query:");
        expr.decompile(System.err, captures);
        System.err.println(";\nEnd decompiled query\n");
        context.reset();
        qNum++;

        System.err.println("\nrunning formatResult");
        JaqlUtil.getRNGStore().save(rRngMap);
        JaqlUtil.getRNGStore().restore(qRngMap);
        formatResult(qNum, expr, context, oStr);

        System.err.println("\nrunning formatDecompileResult");
        JaqlUtil.getRNGStore().save(qRngMap);
        JaqlUtil.getRNGStore().restore(dRngMap);
        formatDecompileResult(expr, context, dStr);

        System.err.println("\nrunning formatRewriteResult");
        JaqlUtil.getRNGStore().save(dRngMap);
        JaqlUtil.getRNGStore().restore(rRngMap);
        formatRewriteResult(expr, parser, context, rStr);
        System.err.println("\nMade it to the end for this query!");
      }
      catch (Exception ex)
      {
        LOG.error(ex);
        ex.printStackTrace(System.err);
        formatFailure(qNum, oStr);
        formatFailure(qNum, dStr);
        formatFailure(qNum, rStr);
        if (parsing)
        {
          BitSet bs = new BitSet();
          bs.add(JaqlParser.EOF);
          bs.add(JaqlParser.SEMI);
          try
          {
            parser.consumeUntil(bs);
          }
          catch (TokenStreamException tse)
          {
            queryFailure(tse);
          }
        }
      }
    }

    // close the input and output
    context.reset();
    teeRewrite.close();
    teeDecompile.close();
    teeQueries.close();
    rStr.flush();
    rStr.close();
    dStr.flush();
    dStr.close();
    oStr.flush();
    oStr.close();
  }

  /**
   * @param qId
   * @param expr
   * @param context
   * @param str
   */
  private void formatResult(int qId, Expr expr, Context context, PrintStream str)
  {
    printHeader(str);
    try
    {
      Schema schema = expr.getSchema();
      if (schema.isArrayOrNull().always())
      {
//        JsonIterator iter = expr.iter(context);
//        iter.print(str);

        JsonValue value = expr.eval(context);
        JsonValue.print(str, value);
        if (!schema.matches(value))
        {
          throw new AssertionError("VALUE\n" + value + "\nDOES NOT MATCH SCHEMA\n" + schema);        
        }
      }
      else
      {
        JsonValue value = expr.eval(context);
        JsonValue.print(str, value);
        if (!schema.matches(value))
        {
          throw new AssertionError("VALUE\n" + value + "\nDOES NOT MATCH SCHEMA\n" + schema);        
        }
      }
    }
    catch (Exception e)
    {
      queryFailure(e);
      str.print(FAILURE);
    }
    printFooter(str);
    str.flush();
  }

  /**
   * @param expr
   * @param context
   * @param str
   */
  private void formatDecompileResult(Expr expr, Context context, PrintStream str)
  {
    // decompile expr into a temp buffer
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    PrintStream tmpStr = new PrintStream(buf);
    try
    {
      Expr dexpr = expr;
      if (!(expr instanceof AssignExpr))
      {
        expr.decompile(tmpStr, new HashSet<Var>());
        tmpStr.close();
        //System.err.println("REAL DECOMP:"+buf);
        // parse it and eval
        JaqlLexer lexer = new JaqlLexer(new ByteArrayInputStream(buf
            .toByteArray()));
        JaqlParser parser = new JaqlParser(lexer);
        dexpr = parser.parse();
      }
      formatResult(-1, dexpr, context, str);
    }
    catch (Exception e)
    {
      queryFailure(e);
      str.print(FAILURE);
    }
    str.flush();
  }

  /**
   * @param expr
   * @param parser
   * @param context
   * @param str
   * @throws Exception
   */
  private void formatRewriteResult(Expr expr, JaqlParser parser,
      Context context, PrintStream str) throws Exception
  {
    // rewrite expr
    RewriteEngine rewriter = new RewriteEngine();
    expr = rewriter.run(parser.env, expr);
    captures.clear();
    System.err.println("\nRewritten query:");
    expr.decompile(System.err, captures);
    System.err.println(";\nEnd rewritten query");
    context.reset();
    // eval
    formatResult(-1, expr, context, str);
  }

  /**
   * @param qId
   * @param str
   */
  private void formatFailure(int qId, PrintStream str)
  {
    printHeader(str);
    str.print(FAILURE);
    printFooter(str);
  }

  /**
   * @param str
   */
  private void printHeader(PrintStream str)
  {
    str.println("##");
  }

  /**
   * @param str
   */
  private void printFooter(PrintStream str)
  {
    str.println();
  }

  /**
   * @param e
   */
  private void queryFailure(Exception e)
  {
    LOG.error(e);
    e.printStackTrace(System.err);
  }

  /**
   * 
   */
  public void testQueries()
  {
    // evaluate the queries
    try
    {
      evaluateQueries(m_queryFileName, m_tmpFileName);
    }
    catch (Exception e)
    {
      fail(e.getMessage());
    }

    // compare the new vs. expected output
    try
    {
      assertTrue("Found difference between current and expected output",
          UtilForTest.compareResults(m_tmpFileName, m_goldFileName, LOG));
      assertTrue("Found differences between decompiles and expected ouput",
          UtilForTest.compareResults(m_decompileName, m_goldFileName, LOG));
      assertTrue("Found difference between rewrite and expected output",
          UtilForTest.compareResults(m_rewriteName, m_goldFileName, LOG));
    }
    catch (IOException e)
    {
      e.printStackTrace(System.err);
      fail();
    }
  }
}
