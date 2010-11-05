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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.MiniMRCluster;

/**
 * Start up a hadoop minicluster, which runs all servers inside a single JVM.
 * 
 * To access the hadoop webapps, it requires that the hadoop webapps directory
 * be in the classpath.  It _should_ work with them in the hadoop .jar file, but
 * because Jetty doesn't unpack the directory properly without a trailing '/'
 * on the name (in particular, Hadoop sets up webapps/hdfs instead of webapps/hdfs/,
 * and jetty then unpacks into a file $TEMP/.../webapps/hdfs instead of
 * $TEMP/.../.  Then it cannot find the WEB-INF/web.xml file off the root, so it
 * cannot run the hadoop JSPs.
 */
public class MiniCluster
{
  public static String tempDir = "/temp";
  public static final Log LOG = LogFactory.getLog(MiniCluster.class.getName());

  protected Configuration conf;
  protected MiniDFSCluster dfsCluster;
  protected MiniMRCluster mrCluster;


  /**
   * @param args
   */
  public static void main(String[] args) throws IOException
  {
    String clusterHome = System.getProperty("hadoop.minicluster.dir");
    if( clusterHome == null )
    {
      clusterHome = "./minicluster";
      System.setProperty("hadoop.minicluster.dir", clusterHome); 
    }
    LOG.info("hadoop.minicluster.dir="+clusterHome);
    File clusterFile = new File(clusterHome);
    if( ! clusterFile.exists() )
    {
      clusterFile.mkdirs();
    }
    if( ! clusterFile.isDirectory() )
    {
      throw new IOException("minicluster home directory must be a directory: "+clusterHome);
    }
    if( ! clusterFile.canRead() || ! clusterFile.canWrite() )
    {
      throw new IOException("minicluster home directory must be readable and writable: "+clusterHome);
    }
    
    String logDir = System.getProperty("hadoop.log.dir");
    if( logDir == null )
    {
      logDir = clusterHome + File.separator + "logs";
      System.setProperty("hadoop.log.dir", logDir); 
    }
    File logFile = new File(logDir);
    if( ! logFile.exists() )
    {
      logFile.mkdirs();
    }
    
    String confDir = System.getProperty("hadoop.conf.override");
    if( confDir == null )
    {
      confDir = clusterHome + File.separator + "conf";
      System.setProperty("hadoop.conf.override", confDir); 
    }
    File confFile = new File(confDir);
    if( !confFile.exists() )
    {
      confFile.mkdirs();
    }

    System.out.println("starting minicluster in "+clusterHome);
    MiniCluster mc = new MiniCluster(args);
    // To find the ports in the 
    // hdfs: search for: Web-server up at: localhost:####
    // mapred: search for: mapred.JobTracker: JobTracker webserver: ####
    Configuration conf = mc.getConf();
    System.out.println("fs.default.name: " + conf.get("fs.default.name"));
    System.out.println("dfs.http.address: " + conf.get("dfs.http.address"));
    System.out.println("mapred.job.tracker.http.address: " + conf.get("mapred.job.tracker.http.address"));

    boolean waitForInterrupt;
    try
    {
      System.out.println("press enter to end minicluster (or eof to run forever)...");
      waitForInterrupt = System.in.read() < 0; // wait for any input or eof
    }
    catch( Exception e )
    {
      // something odd happened.  Just shutdown. 
      LOG.error("error reading from stdin", e);
      waitForInterrupt = false;
    }
    
    // eof means that we will wait for a kill signal
    while( waitForInterrupt )
    {
      System.out.println("minicluster is running until interrupted...");
      try
      {
        Thread.sleep(60*60*1000);
      }
      catch (InterruptedException e)
      {
        waitForInterrupt = false;
      }
    }
    
    System.out.println("shutting down minicluster...");
    try
    {
      mc.tearDown();
    }
    catch( Exception e )
    {
      LOG.error("error while shutting down minicluster", e);
    }
  }
  
  private Configuration getConf()
  {
    return conf;
  }

  public MiniCluster(String[] args) throws IOException
  {
    setUp();
    Runtime.getRuntime().addShutdownHook(new Thread() 
    {
      @Override
      public void run()
      {
        try
        {
        }
        catch( Exception e )
        {
          LOG.error("failure during minicluster teardown", e);
        }
      }
    });
  }

  /*
   * (non-Javadoc)
   *
   * @see com.ibm.jaql.lang.JaqlBaseTestCase#setUp()
   */
  protected void setUp() throws IOException
  {
    final int numNodes = 1;
    
    conf = new Configuration();
    
    if( System.getProperty("os.name").startsWith("Windows") )
    {
      // There is a bug in hadoop 0.20.1 on windows
      // ... INFO mapred.JobClient: Task Id : attempt_..., Status : FAILED
      // java.io.FileNotFoundException: 
      //    File C:/tmp/hadoop-xxx/mapred/local/1_0/taskTracker/jobcache/job_xxx/attempt_xxx/0_2/work/tmp 
      //    does not exist.
      // at org.apache.hadoop.fs.RawLocalFileSystem.getFileStatus(RawLocalFileSystem.java:361)
      // at org.apache.hadoop.fs.FilterFileSystem.getFileStatus(FilterFileSystem.java:245)
      // at org.apache.hadoop.mapred.TaskRunner.setupWorkDir(TaskRunner.java:519)
      // at org.apache.hadoop.mapred.Child.main(Child.java:155)
      //
      // The following is reported to work around the problem
      String tmp = conf.get("hadoop.tmp.dir", "c:/temp");
      conf.set("mapred.child.tmp", tmp + "/mapred/child.tmp");
    }
    
    dfsCluster = new MiniDFSCluster(conf, numNodes, true, null);
    mrCluster = new MiniMRCluster(numNodes, dfsCluster.getFileSystem().getUri().getAuthority(), 1);
    setupOverride(mrCluster.createJobConf(), conf);
    
    // this.conf = conf = new Configuration();
//    FileSystem fs = FileSystem.get(conf);

//    // make the home directory if it does not exist
//    Path hd = fs.getWorkingDirectory();
//    if (!fs.exists(hd)) fs.mkdirs(hd);
//
//    // make a tmp directory if it does not exist
//    Path t = new Path(tempDir);
//    if (!fs.exists(t))
//    {
//      fs.mkdirs(t);
//    }
  }

  /*
   * (non-Javadoc)
   *
   * @see com.ibm.jaql.lang.JaqlBaseTestCase#tearDown()
   */
  protected void tearDown() throws IOException
  {
    if (mrCluster != null) mrCluster.shutdown();
    if (dfsCluster != null)
    {
      dfsCluster.getFileSystem().close();
      dfsCluster.shutdown();
    }
  }

  /**
   * @param jconf
   * @param conf
   * @throws IOException
   */
  protected void setupOverride(Configuration jconf, Configuration conf)
      throws IOException
  {
    // require that override dir exists
    File overrideDir = new File(System.getProperty("hadoop.conf.override"));
    if( !overrideDir.exists() )
    {
      throw new IOException("hadoop-override dir must exist");
    }

    // write out the JobConf from MiniMR to the override dir
    jconf.writeXml(System.out);
    System.out.println();
    System.out.flush();
    conf.set("mapred.job.tracker", jconf.get("mapred.job.tracker", null));
    String name = "mapred.job.tracker.info.port";
    String addr = jconf.get(name, null);
    if (addr == null)
    {
      name = "mapred.job.tracker.http.address";
      addr = jconf.get(name, null);
    }
    conf.set(name, addr);
    
//    this.dfsCluster.getNameNode().get.getHttpAddress()

    String path = overrideDir.getCanonicalPath() + File.separator;
    System.out.println("writing conf to: "+path);
    OutputStream outCore   = new FileOutputStream(path + "core-default.xml");
    OutputStream outMapred = new FileOutputStream(path + "mapred-default.xml");
    OutputStream outHdfs   = new FileOutputStream(path + "hdfs-default.xml");
    conf.writeXml(outCore);
    conf.writeXml(outMapred);
    conf.writeXml(outHdfs);
    outCore.close();
    outMapred.close();
    outHdfs.close();
    System.out.println("setup complete");
    System.out.flush();
  }
}
