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

import static com.ibm.jaql.json.type.JsonType.ARRAY;
import static com.ibm.jaql.json.type.JsonType.NULL;

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
import java.util.Map;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import antlr.TokenStreamException;
import antlr.collections.impl.BitSet;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.top.AssignExpr;
import com.ibm.jaql.lang.parser.JaqlLexer;
import com.ibm.jaql.lang.parser.JaqlParser;
import com.ibm.jaql.lang.rewrite.RewriteEngine;
import com.ibm.jaql.lang.rewrite.VarTagger;
import com.ibm.jaql.util.TeeInputStream;

/**
 * 
 */
/**
 * @author kelvin
 * 
 */
public abstract class JaqlBaseTestCase extends TestCase {

	private static final Log LOG = LogFactory.getLog(JaqlBaseTestCase.class
			.getName());

	public static String FAILURE = "FAILURE";
	
	public static final String TEST_CACHE_DATA = System.getProperty("test.cache.data");

	private String m_queryFileName = null;
	private String m_tmpFileName = null;
	private String m_goldFileName = null;
	private String m_goldCountFileName = null;
	private String m_decompileName = null;
	private String m_rewriteName = null;
	private String m_countName = null;

	private HashSet<Var> captures = new HashSet<Var>();

	protected boolean runResult = false;
	protected boolean runDecompileResult = false;
	protected boolean runRewriteResult = false;
	protected boolean runCountResult = false;

	protected int PLAIN = 1;
	protected int DECOMPLIE = 2;
	protected int REWRITE = 3;

	private Map<String, Long> exprTypeCounter = null;

	@Override
	protected abstract void setUp() throws IOException;

	/**
	 * @param prefix
	 */
	protected void setFilePrefix(String prefix) {
    m_queryFileName = getQueryPathname(prefix);
    m_goldFileName = getGoldPathname(prefix);
    m_tmpFileName = getTmpPathname(prefix);
    m_goldCountFileName = getPathname(prefix, "GoldCount");
    m_decompileName = getPathname(prefix, "Decompile");
    m_rewriteName = getPathname(prefix, "Rewrite");
    m_countName = getPathname(prefix, "Count");

    runResult = "true".equals(System.getProperty("test.plain"));
    System.err.println("runResult = " + runResult);

    runDecompileResult = "true".equals(System.getProperty("test.explain"));
    System.err.println("runDecompileResult = " + runDecompileResult);

    runRewriteResult = "true".equals(System.getProperty("test.rewrite"));
    System.err.println("runRewriteResult = " + runRewriteResult);

    runCountResult = "true".equals(System.getProperty("test.count"));
    System.err.println("runCountResult = " + runCountResult);
	}
	
	public static String getPathname(String fileName) {
	  return TEST_CACHE_DATA + File.separator + fileName + ".txt";
	}

  public static String getPathname(String prefix, String suffix) {
    return getPathname(prefix + suffix);
  }
  
  public static String getQueryPathname(String prefix) {
    return getPathname(prefix, "Queries");
  }

  public static String getGoldPathname(String prefix) {
    return getPathname(prefix, "Gold");
  }
  
  public static String getTmpPathname(String prefix) {
    return getPathname(prefix, "Tmp");
  }
  
  @Override
	protected abstract void tearDown() throws IOException;

	private void execute(String inputFileName, String outputFilename, int type)
			throws FileNotFoundException, IOException {
		// Initialize input, output streams
		InputStream input = new FileInputStream(inputFileName);
		input = new TeeInputStream(input, System.err);
		PrintStream oStr = new PrintStream(new FileOutputStream(outputFilename));
		TeeInputStream teeInput = new TeeInputStream(input, oStr);

		// Initialize parser
		JaqlLexer lexer = new JaqlLexer(teeInput);
		JaqlParser parser = new JaqlParser(lexer);
		Context context = new Context();

		// Begin to loop all sentences
		boolean parsing = false;
		int qNum = 0;
		while (true) {
			try {
				parser.env.reset();
				parsing = true;
				Expr expr = parser.parse();
				parsing = false;
				System.err.println("\n\nParsing query at " + inputFileName
						+ ":" + lexer.getLine());
				oStr.flush();
				if (parser.done) {
					break;
				}
				if (expr == null) {
					continue;
				}

				VarTagger.tag(expr);
				captures.clear();
				System.err.println("\nDecompiled query:");
				expr.decompile(System.err, captures);
				System.err.println(";\nEnd decompiled query\n");

				if (type == PLAIN) {
					System.err.println("\nrunning formatResult");
					formatResult(qNum, expr, context, oStr);
				} else if (type == DECOMPLIE) {
					System.err.println("\nrunning formatDecompileResult");
					formatDecompileResult(expr, context, oStr);
				} else if (type == REWRITE) {
					System.err.println("\nrunning formatRewriteResult");
					formatRewriteResult(expr, context, oStr);
				}

				context.reset();
				qNum++;
			} catch (Exception ex) {
				LOG.error(ex);
				ex.printStackTrace(System.err);
				formatFailure(qNum, oStr);
				if (parsing) {
					BitSet bs = new BitSet();
					bs.add(JaqlParser.EOF);
					bs.add(JaqlParser.SEMI);
					try {
						parser.consumeUntil(bs);
					} catch (TokenStreamException tse) {
						queryFailure(tse);
					}
				}
			}
		}

		if (this.runRewriteResult & this.runCountResult && exprTypeCounter != null) {
			PrintStream cStr = new PrintStream(new FileOutputStream(
					this.m_countName));
			// Print out all concerned expression occurrences			
			java.util.Iterator<String> i = exprTypeCounter.keySet().iterator();
			while (i.hasNext()) {
				String tem = i.next();
				cStr.println(tem + "\t" + exprTypeCounter.get(tem));
			}
			cStr.flush();
			cStr.close();
			// clear type counter
			exprTypeCounter.clear();
		}

		// Close all streams
		context.reset();
		oStr.flush();
		oStr.close();
		teeInput.close();

	}

	/**
	 * @param qId
	 * @param expr
	 * @param context
	 * @param str
	 */
	private void formatResult(int qId, Expr expr, Context context,
			PrintStream str) {
		printHeader(str);
		try {
			Schema schema = expr.getSchema();
			if (schema.is(ARRAY, NULL).always()) {
				// JsonIterator iter = expr.iter(context);
				// iter.print(str);

				JsonValue value = expr.eval(context);
				JsonUtil.print(str, value);
				if (!schema.matches(value)) {
					throw new AssertionError("VALUE\n" + value
							+ "\nDOES NOT MATCH\n" + schema);
				}
			} else {
				JsonValue value = expr.eval(context);
				JsonUtil.print(str, value);
				if (!schema.matches(value)) {
					throw new AssertionError("VALUE\n" + value
							+ "\nDOES NOT MATCH\n" + schema);
				}
			}
		} catch (Exception e) {
			queryFailure(e);
			str.print(FAILURE);
		}
		printFooter(str);
		str.flush();
	}

	/**
	 * @param expr
	 */
	private void countType(Expr expr) {		
		if (expr instanceof com.ibm.jaql.lang.expr.io.AbstractWriteExpr
				|| expr instanceof com.ibm.jaql.lang.expr.io.AbstractReadExpr
				|| expr instanceof com.ibm.jaql.lang.expr.hadoop.MapReduceBaseExpr) {			
			if (exprTypeCounter == null)
				exprTypeCounter = new HashMap<String, Long>();			
			String exprTypeStr = expr.getClass().getName();
			if (exprTypeCounter.containsKey(exprTypeStr)) {
				exprTypeCounter.put(exprTypeStr, exprTypeCounter
						.get(exprTypeStr) + 1);
			} else {
				exprTypeCounter.put(exprTypeStr, new Long(1));
			}
		}
		// loop all its children to get nested expressions
		for (int i = 0; i < expr.numChildren(); i++) {
			countType(expr.child(i));
		}
	}

	/**
	 * @param expr
	 * @param context
	 * @param str
	 */
	private void formatDecompileResult(Expr expr, Context context,
			PrintStream str) {

		// decompile expr into a temp buffer
		ByteArrayOutputStream buf = new ByteArrayOutputStream();
		PrintStream tmpStr = new PrintStream(buf);
		try {
			Expr dexpr = expr;
			if (!(expr instanceof AssignExpr)) {
				expr.decompile(tmpStr, new HashSet<Var>());
				tmpStr.close();
				// System.err.println("REAL DECOMP:"+buf);
				// parse it and eval
				JaqlLexer lexer = new JaqlLexer(new ByteArrayInputStream(buf
						.toByteArray()));
				JaqlParser parser = new JaqlParser(lexer);
				dexpr = parser.stmt();
			}
			formatResult(-1, dexpr, context, str);
		} catch (Exception e) {
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
	private void formatRewriteResult(Expr expr, Context context, PrintStream str)
			throws Exception {
		// rewrite expr
		RewriteEngine rewriter = new RewriteEngine();
		try {
			expr = rewriter.run(expr);
			VarTagger.tag(expr);
			captures.clear();
			System.err.println("\nRewritten query:");
			expr.decompile(System.err, captures);
			System.err.println(";\nEnd rewritten query");
			context.reset();
		} catch (Exception e) {
			printHeader(str);
			queryFailure(e);
			str.print(FAILURE);
			printFooter(str);
			str.flush();
			return;
		}
		
		// if counter is required
		if (this.runCountResult)
			countType(expr);
		
		// eval
		formatResult(-1, expr, context, str);
	}

	/**
	 * @param qId
	 * @param str
	 */
	private void formatFailure(int qId, PrintStream str) {
		printHeader(str);
		str.print(FAILURE);
		printFooter(str);
	}

	/**
	 * @param str
	 */
	private void printHeader(PrintStream str) {
		str.println("##");
	}

	/**
	 * @param str
	 */
	private void printFooter(PrintStream str) {
		str.println();
	}

	/**
	 * @param e
	 */
	private void queryFailure(Exception e) {
		LOG.error(e);
		e.printStackTrace(System.err);
	}

	/**
   * 
   */
	public void testQueries() {
		boolean success = true;
		try {
			if (this.runResult) {
				System.err.println("\n\nExecuting testQueries in plain mode");
				execute(m_queryFileName, m_tmpFileName, this.PLAIN);
				try {
					assertTrue(
							"\n\nFound difference between current and expected output",
							compareResults(m_tmpFileName, m_goldFileName));
					System.err
							.println("\n\nExecuted testQueries in plain mode successfully");
				} catch (Exception e) {
					e.printStackTrace(System.err);
					fail(e.getMessage());
					success = false;
					System.err
							.println("\n\nExecuted testQueries in plain mode with failure");
				}
			}

			if (this.runDecompileResult) {
				System.err
						.println("\n\nExecuting testQueries in decompile mode");
				execute(m_queryFileName, m_decompileName, this.DECOMPLIE);
				try {
					assertTrue(
							"\n\nFound differences between decompiles and expected ouput",
							compareResults(m_decompileName, m_goldFileName));
					System.err
							.println("\n\nExecuted testQueries in decompile mode successfully");
				} catch (Exception e) {
					e.printStackTrace(System.err);
					fail(e.getMessage());
					success = false;
					System.err
							.println("\n\nExecuted testQueries in decompile mode with failure");
				}
			}

			if (this.runRewriteResult) {
				System.err.println("\n\nExecuting testQueries in rewrite mode");
				execute(m_queryFileName, m_rewriteName, this.REWRITE);
				try {
					assertTrue(
							"\n\nFound difference between rewrite and expected output",
							compareResults(m_rewriteName, m_goldFileName));

					if (this.runCountResult && new File(m_goldCountFileName).isFile()){
						assertTrue(
								"\n\nExpression Occurences are different with desired",
								compareResults(this.m_countName,
										m_goldCountFileName));
						System.err
						.println("\n\nGot desired expression occurrences");
					}
					System.err
							.println("\n\nExecuted testQueries in rewrite mode successfully");
				} catch (Exception e) {
					e.printStackTrace(System.err);
					fail(e.getMessage());
					success = false;
					System.err
							.println("\n\nExecuted testQueries in rewrite mode with failure");
				}
			}
		} catch (Exception e) {
			e.printStackTrace(System.err);
			fail(e.getMessage());
		}
		if (!success)
			fail();

	}

  /**
   * Compares the tmp file and gold file using unix diff. whitespace is ignored
   * for the diff.
   * 
   * 
   * @param tmpFileName tmp file name
   * @param goldFileName gold file name
   * @return <tt>true</tt> if the tmp file and gold file are the same;
   *         <tt>false</tt> otherwise.
   * @throws IOException
   */
	public static boolean compareResults(String tmpFileName,
			String goldFileName) throws IOException {
		// use unix 'diff', ignoring whitespace
    ProcessBuilder pb = new ProcessBuilder("diff", "-w", tmpFileName,
                                           goldFileName);
    /*
     * Two input file for diff are the same only if nothing is printed to stdout
     * and stderr. Redirect stderr to stdout so that only stdout needs to
     * checked.
     */
    pb.redirectErrorStream(true);
    Process p = pb.start();
		InputStream str = p.getInputStream();

		byte[] b = new byte[1024];
		int numRead = 0;
		StringBuilder sb = new StringBuilder();
		while ((numRead = str.read(b)) > 0) {
			sb.append(new String(b, 0, numRead, "US-ASCII"));
		}
		if (sb.length() > 0)
			System.err.println("\ndiff -w " + tmpFileName + " " + goldFileName + "\n"
					+ sb);

		return sb.length() == 0;
	}
}
