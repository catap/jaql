package com.ibm.jaql;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Reader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.junit.Assert;

import com.ibm.jaql.io.hadoop.Globals;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.lang.Jaql;
import com.ibm.jaql.lang.core.Module;
import com.ibm.jaql.lang.core.Namespace;
import com.ibm.jaql.lang.expr.hadoop.MapReduceBaseExpr;
import com.ibm.jaql.lang.expr.io.AbstractReadExpr;
import com.ibm.jaql.lang.expr.io.AbstractWriteExpr;
import com.ibm.jaql.util.ClassLoaderMgr;
import com.ibm.jaql.util.EchoedReader;
import com.ibm.jaql.util.FastPrintWriter;
import com.ibm.jaql.util.FastPrinter;

public class TestDistribute extends JaqlScriptTestCase {

    public static String WORK_DIR = "";
    public static String HDFS_URL = "";
    public static String HDFS_NAMENODE= "";
    
    public TestDistribute (String script) {
        super(script);
    }
    
    public TestDistribute () {
        super();
    }
    
    @Override
    protected void runScript( Mode mode) throws Exception {

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


          // make tests work the same on windows as unix.
          System.setProperty("line.separator", "\n");   
          
          final FastPrinter resultStream = new FastPrintWriter(new FileWriter(outName));
          Reader queryReader = new InputStreamReader(new FileInputStream(queriesName), "UTF-8"); 
          queryReader = new EchoedReader( queryReader,
              new PrintWriter(new OutputStreamWriter(System.err, "UTF-8")) );
          queryReader = new EchoedReader( queryReader, resultStream );

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
          
          HDFS_URL = conf.get("fs.default.name");
          
          // in local mode, the value of HDFS_URL is file:///
          // which is the default value in hadoop
          if (HDFS_URL.indexOf("://") != HDFS_URL.lastIndexOf(":")) {
              HDFS_NAMENODE = HDFS_URL.substring(HDFS_URL.indexOf("://")+3, HDFS_URL.lastIndexOf(":"));
          }
          
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
    
}
