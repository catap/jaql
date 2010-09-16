/*
 * Copyright (C) IBM Corp. 2010.
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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.junit.Assert;
import org.junit.Test;

import com.ibm.jaql.io.hadoop.Globals;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.DefaultExplainHandler;
import com.ibm.jaql.lang.ExceptionHandler;
import com.ibm.jaql.lang.ExplainHandler;
import com.ibm.jaql.lang.Jaql;
import com.ibm.jaql.lang.StreamPrinter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Module;
import com.ibm.jaql.lang.core.Namespace;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.hadoop.MapReduceBaseExpr;
import com.ibm.jaql.lang.expr.io.AbstractReadExpr;
import com.ibm.jaql.lang.expr.io.AbstractWriteExpr;
import com.ibm.jaql.lang.expr.io.RegisterAdapterExpr;
import com.ibm.jaql.lang.expr.top.AssignExpr;
import com.ibm.jaql.lang.expr.top.QueryExpr;
import com.ibm.jaql.lang.walk.PostOrderExprWalker;
import com.ibm.jaql.util.ClassLoaderMgr;
import com.ibm.jaql.util.EchoedReader;


// @RunWith(Parameterized.class)
public class JaqlScriptTestCase
{
  public static final String DATADIR_NAME = "DATADIR";
  public static final String DATADIR_DIR = "../../../src/test/com/ibm/jaql/data/";
  public static final JsonString DATADIR_VALUE = new JsonString(DATADIR_DIR);
  public static String WORK_DIR = "";
  public static String HDFS_URL = "";
  public static String HDFS_NAMENODE= "";
  
  public enum Mode
  {
    NO_REWRITE,
    REWRITE,
    DECOMPILE,
    COUNT;
    
    public String toString()
    {
      return name().toLowerCase();
    }
  }
  
//  @Parameters public static Collection<Object[]> data() 
//  {
//    Collection<Object[]> data = new ArrayList<Object[]>();
//    data.add(new Object[]{ "examples" });
//    data.add(new Object[]{ "hashtable" });
//    data.add(new Object[]{ "inputSplits" });
//    data.add(new Object[]{ "longList" });
//    data.add(new Object[]{ "module" });
//    data.add(new Object[]{ "options" });
//    data.add(new Object[]{ "registry" });
//    data.add(new Object[]{ "rng" });  
//    data.add(new Object[]{ "schema" });
//    data.add(new Object[]{ "storage" });
//    data.add(new Object[]{ "schemaPrinting" });
//    data.add(new Object[]{ "storageTemp" });
//    data.add(new Object[]{ "storageText" });
//    
//    // data.add(new Object[]{ "hbase" });
//    return data;
//  }

  protected String script;

  public JaqlScriptTestCase(String script)
  {
    this.script = script;
  }

  public JaqlScriptTestCase()
  {
    String name = getClass().getSimpleName();
    if( ! name.startsWith("Test") )
    {
      throw new IllegalArgumentException("Cannot infer script name");
    }
    this.script = Character.toLowerCase(name.charAt(4)) + name.substring(5);
  }

  @Test public void testNoRewrite() throws Throwable
  {
    runScript(Mode.NO_REWRITE);
  }

  @Test public void testRewrite() throws Throwable
  {
    runScript(Mode.REWRITE);
  }

  @Test public void testDecompile() throws Throwable
  {
    runScript(Mode.DECOMPILE);
  }

  @Test public void testCount() throws Throwable
  {
    runScript(Mode.COUNT);
  }

  protected String getScriptDir() { return "src/test/com/ibm/jaql/" ; }
  
  protected String getModuleDir() { return getScriptDir() + "modules/"; }
  
  protected String getExtensionJar() { return "build/extension.jar"; }
  
  protected void runScript( Mode mode ) throws Exception
  {
    String testLabel = script + "." + mode; 
    try {
      String runMode = System.getProperty("test."+mode, "true");
      if( ! runMode.equals("true") )
      {
        System.err.println("\nSkipping disabled jaql test " + testLabel + " (test."+mode+"="+runMode+" != true)\n");
        return;
      }
      
      String jaqlHome = System.getProperty("jaql.home", ".");
      jaqlHome = new File(jaqlHome).getAbsolutePath().toString().replace('\\', '/') + "/";
      
      String scriptDir   = jaqlHome + getScriptDir();
      String moduleDir   = getModuleDir();
      String queriesName = scriptDir + script + "Queries.txt";
      String goldName    = scriptDir + testLabel + ".gold";
      
      if( ! new File(goldName).exists() )
      {
        // look for the mode-independent gold file
        if( mode == Mode.COUNT  )
        {
          System.err.println("\nSkipping jaql count test " + testLabel + " (no gold file)\n");
          return;
        }
        goldName = scriptDir + script + ".gold";
        if( ! new File(goldName).exists() )
        {
          Assert.fail("\nNo gold file for jaql test " + testLabel + "at path: " + goldName);
          return;
        }
      }

      System.err.println("\nRunning jaql test " + testLabel + "\n");

      String outDir  = jaqlHome + "build/test/";
      String workDir = outDir + "run." + testLabel + "/";
      String outName = workDir + testLabel + ".out";
      new File(workDir).mkdirs();

      // Set the default directories
      System.setProperty("jaql.local.dir", workDir);
      Configuration conf = new Configuration();
      LocalFileSystem lfs = FileSystem.getLocal(conf);
      lfs.setWorkingDirectory(new Path(workDir));
      FileSystem fs = FileSystem.get(conf);
      if( !(fs instanceof LocalFileSystem) )
      {
        String user = UnixUserGroupInformation.login(conf).getUserName();
        fs.setWorkingDirectory(new Path("/temp/"+user+"/com.ibm.jaql/test/"+script));
        WORK_DIR = "/temp/"+user+"/com.ibm.jaql/test/"+script;
      }
      // mapred.working.dir is automatically set from the fs, but only once. 
      // When running multiple tests in the same JVM, it only picks up the first setting.
      if( Globals.getJobConf() != null )
      {
        Globals.getJobConf().setWorkingDirectory(fs.getWorkingDirectory());
      }

      HDFS_URL = conf.get("fs.default.name");

      // make tests work the same on windows as unix.
      System.setProperty("line.separator", "\n");      
      final PrintStream resultStream = new PrintStream(
          new FileOutputStream(outName), false, "UTF-8");
      Reader queryReader = new InputStreamReader(new FileInputStream(queriesName), "UTF-8"); 
      queryReader = new EchoedReader( queryReader,
          new PrintWriter(new OutputStreamWriter(System.err, "UTF-8")) );
      queryReader = new EchoedReader( queryReader,
          new PrintWriter(new OutputStreamWriter(resultStream, "UTF-8")) );

      // TODO: These should be on Jaql, not static.
      Module.setSearchPath(new String[]{moduleDir});
      ClassLoaderMgr.reset();
      Namespace.clearNamespaces();
      
      Jaql jaql = new Jaql(queriesName, queryReader);
      
      if( mode == Mode.COUNT )
      {
        final Class<?>[] exprsToCount = new Class<?>[] {
            AbstractReadExpr.class,
            AbstractWriteExpr.class,
            MapReduceBaseExpr.class,
        };
        jaql.setExplainHandler(new CountExplainHandler(resultStream, exprsToCount));
        jaql.setExplainOnly(true);
      }
      else if( mode == Mode.DECOMPILE )
      {
        jaql.setExplainHandler(new DecompileExplainHandler(System.err));
        jaql.setExplainOnly(true);
      }
      
      jaql.setExceptionHandler(new TestExceptionHandler(resultStream, jaqlHome));
      jaql.enableRewrite( mode != Mode.NO_REWRITE );
      boolean schemaPrinting = "schemaPrinting".equals(script);
      jaql.setJaqlPrinter(new TestPrinter(resultStream, schemaPrinting));

      String extJar = getExtensionJar();
      if( extJar != null )
        jaql.addJar( jaqlHome + extJar );
      jaql.setVar(DATADIR_NAME, DATADIR_VALUE);
      
      HDFS_NAMENODE = HDFS_URL.substring(HDFS_URL.indexOf("://")+3, HDFS_URL.lastIndexOf(":"));
      jaql.setVar("WORK_DIR", new JsonString(WORK_DIR));
      jaql.setVar("HDFS_URL", new JsonString(HDFS_URL));
      jaql.setVar("HDFS_NAMENODE", new JsonString(HDFS_NAMENODE));
      
      // run the script
      jaql.run();

      // finish up
      jaql.close();
      queryReader.close();
      resultStream.close();

      // compare with expected output
      boolean diff = compareResults(outName, goldName);
      if( diff )
      {
        String msg = "Found differences during jaql test " + testLabel;
        System.err.println("\n"+msg);
        Assert.fail(msg);
      }
      
      System.err.println("\nSuccessfully ran jaql test " + testLabel + "\n");
    }
    catch (Exception e)
    {
      e.printStackTrace(System.err);
      System.err.println("\n\nFailure of jaql test " + testLabel);
      Assert.fail(e.getMessage());
    }
  }


  /**
   * Compares the tmp file and gold file using unix diff. whitespace is ignored
   * for the diff.
   * 
   * @param tmpFile tmp file name
   * @param goldFile gold file name
   * @return <tt>true</tt> if the tmp file and gold file are the same;
   *         <tt>false</tt> otherwise.
   * @throws IOException
   */
  public static boolean compareResults(String tmpFile, String goldFile)
      throws IOException
  {
    // use unix 'diff', ignoring whitespace
    System.err.println("\ndiff -w " + tmpFile + " " + goldFile + ":\n");
    ProcessBuilder pb = new ProcessBuilder("diff", "-w", tmpFile, goldFile);
    
    /*
     * Two input file for diff are the same only if nothing is printed to stdout
     * and stderr. Redirect stderr to stdout so that only stdout needs to
     * checked.
     */
    pb.redirectErrorStream(true);
    Process p = pb.start();
    InputStream str = p.getInputStream();

    boolean diff = false;
    byte[] buf = new byte[32*1024];
    int numRead = 0;
    while( (numRead = str.read(buf)) > 0 )
    {
      diff = true;
      System.err.write(buf,0,numRead);
    }
    return diff;
  }

  public static class TestPrinter extends StreamPrinter
  {
    protected boolean schemaPrinting;
    
    public TestPrinter(PrintStream ps, boolean schemaPrinting)
    {
      super(ps, true);
      this.schemaPrinting = schemaPrinting;
    }

    @Override
    public void printPrompt() throws IOException
    {
      output.println("\n;//------------------- TEST-CASE -----------------");
      output.flush();
    }

    @Override
    public void print(Expr expr, Context context) throws Exception
    {
      output.println("\n\n;//--------------------- RESULT ------------------\n");
      output.flush();
      
      if( schemaPrinting )
      {
        // TODO: We should be able to always use the standard StreamPrinter.print()
        // but there is schema sensitivity in the record printer.
        super.print(expr, context);
      }
      else
      {
        if( expr.getSchema().is(ARRAY, NULL).always() )
        {
          JsonIterator iter = expr.iter(context);
          iter.print(output, 0);
        }
        else
        {
          JsonValue value = expr.eval(context);
          JsonUtil.getDefaultSerializer().write(output, value);
        }
        output.println();
      }
      
      output.flush();
    }

    @Override
    public void close() throws IOException
    {
      this.output.println("\n;//------------------- TEST-DONE -----------------");
      this.output.flush();
    }
  }

  public static class TestExceptionHandler extends ExceptionHandler
  {
    protected PrintStream ps;
    protected String pathToRemove;

    public TestExceptionHandler(PrintStream ps, String pathToRemove)
    {
      this.ps = ps;
      this.pathToRemove = pathToRemove;
    }

    @Override
    public void handleException(Throwable error, JsonValue ctx) throws Exception
    {
      Throwable cause = error;
      while( cause.getCause() != null )
      {
        cause = cause.getCause();
      }
      // Expected exceptions should be caught by script using expectedException().
      ps.println("\n\n;//----- EXCEPTION -----\n");
      // error.printStackTrace(ps);
      ps.println("// "+cause.getClass().getName());
      // MODIFIED: this is not portable across JVM's (e.g., IBM and Sun)
//      String msg = cause.getMessage();
//      msg = msg.replace(pathToRemove, ".../");
//      msg = msg.replace("\n", "\n//");
//      // get rid of object specific debug statements (not repeatable)
//      int atIdx = msg.indexOf('@'); 
//      if(atIdx >= 0)
//    	  msg = msg.substring(0, atIdx);
//      ps.println("// " + msg );
      ps.println("\n;//------------------- TEST-CASE -----------------");
      ps.flush();

      System.err.println("\nEXCEPTION: ");
      error.printStackTrace();
      System.err.println();
      System.err.flush();
    }
  };

  public static class DecompileExplainHandler extends ExplainHandler
  {
    protected PrintStream out;
    protected Jaql jaql2 = new Jaql();
    
    public DecompileExplainHandler(PrintStream out)
    {
      this.out = out;
    }
    
    @Override
    public Expr explain(Expr expr) throws Exception
    {
      String stmt = DefaultExplainHandler.decompile(expr);
      // expr.getEnvExpr().getEnv();
      out.println("\n;//------------------- DECOMPILE -----------------");
      out.println(stmt);
      out.flush();
      jaql2.setInput(stmt);
      Expr expr2 = jaql2.expr();
      
      if( expr2 != null )
      {
        String stmt2 = DefaultExplainHandler.decompile(expr2);
        if( ! stmt.equals(stmt2) )
        {
          out.println("\nWARNING: Decompiled statements are not identical!");
          out.println(stmt2);
          out.flush();
        }
        return expr2;
      }
      
      if( expr2 instanceof QueryExpr )
      {
        Assert.fail("Lost statement when recompiling: "+expr);
      }
      return expr;
    }
  }

  
  public static class CountExplainHandler extends ExplainHandler
  {
    protected PrintStream ps;
    protected Class<?>[] classes;
    protected TreeMap<String, Counter> counts;
    protected PostOrderExprWalker walker = new PostOrderExprWalker();

    public CountExplainHandler(PrintStream ps, Class<?>[] classes)
    {
      this.ps = ps;
      this.classes = classes;
      this.counts = new TreeMap<String,Counter>();
    }

    @Override
    public Expr explain(Expr root) throws Exception
    {
      // Return exprs for evaluation that modify the state.
      // Don't bother counting them.
      if( root instanceof QueryExpr && root.child(0) instanceof RegisterAdapterExpr ) // HACK: if we don't register, explain will change or bomb. This will go away with the registry.
      {
        return root;
      }
      
      counts.clear();
      walker.reset(root);
      Expr expr;
      while( (expr = walker.next()) != null )
      {
        Class<?> ec = expr.getClass();
        for( Class<?> cls: classes )
        {
          if( cls.isAssignableFrom(ec) )
          {
            Counter ctr = counts.get(ec.getName());
            if( ctr == null )
            {
              ctr = new Counter();
              counts.put(ec.getName(), ctr);
            }
            ctr.count++;
            break;
          }
        }
      }
      
      ps.println("\n\n;//------------------- EXPR-COUNTS -----------------\n");
      ps.println("{");
      String sep = "";
      for( Map.Entry<String, Counter> e: counts.entrySet() )
      {
        long n = e.getValue().count;
        ps.print(sep);
        ps.print("  '");
        ps.print(e.getKey());
        ps.print("': ");
        ps.print(n);
        sep = ",\n";
      }
      ps.println("\n}");
      ps.flush();
      
      // We could always return expr to continue running
      return null;
    }
    
    protected static class Counter
    {
      public long count;
    }
  }

}
