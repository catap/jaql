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

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.dfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.util.VersionInfo;

/**
 * 
 */
public class JaqlShell
{

  private static final Log LOG = LogFactory.getLog(JaqlShell.class.getName());

  private Configuration    m_conf;
  private MiniHBaseCluster m_base;
  private MiniMRCluster    m_mr;
  private MiniDFSCluster   m_fs;
  private HBaseAdmin       m_admin;

  /**
   * @param dir
   * @param numNodes
   * @param format
   * @throws Exception
   */
  public JaqlShell(String dir, int numNodes, boolean format) throws Exception
  {
    String vInfo = VersionInfo.getVersion();
    System.setProperty("test.build.data", dir);
    m_conf = new HBaseConfiguration();

    // setup conf according to the Hadoop version
    if (vInfo.indexOf("0.16") >= 0 || vInfo.indexOf("0.17") >= 0 || vInfo.indexOf("0.18") >= 0)
    {
      m_conf.set(HConstants.MASTER_ADDRESS, "local");
    }
    else if (vInfo.indexOf("0.15") >= 0)
    {
      m_conf.set(HConstants.MASTER_ADDRESS, "localhost:0");
      m_conf.set(HConstants.REGIONSERVER_ADDRESS, "localhost:0");
    }
    else
    {
      throw new Exception("Unsupported Hadoop version: " + vInfo);
    }
    m_conf.set("hbase.master.info.port", "-1");
    m_conf.set("hbase.regionserver.info.port", "-1");
    //setupOverride();

    m_fs = new MiniDFSCluster(m_conf, numNodes, true, (String[])null);
    FileSystem filesystem = m_fs.getFileSystem();
    m_conf.set("fs.default.name", filesystem.getUri().toString());
    Path parentdir = filesystem.getHomeDirectory();
    m_conf.set(HConstants.HBASE_DIR, parentdir.toString());
    filesystem.mkdirs(parentdir);
    FSUtils.setVersion(filesystem, parentdir);
    
    try
    {
      // hbase 0.1.3 is compatible w/ 0.16
      String hbvInfo = org.apache.hadoop.hbase.util.VersionInfo.getVersion();
      if (hbvInfo.indexOf("0.18") >= 0 && vInfo.indexOf("0.18") >= 0)
      {
        m_base = new MiniHBaseCluster((HBaseConfiguration) m_conf, numNodes);
        setupOverride();
      }
    }
    catch (Exception e)
    {
      LOG.warn("Could not start HBase cluster due to exception", e);
    }
    catch (Error er)
    {
      LOG.warn("Could not start HBase cluster due to error", er);
    }
    //m_base = new MiniHBaseCluster((HBaseConfiguration)m_conf, numNodes, true, format, false);
    //setupOverride();

//    if (m_base != null)
//    {
//      m_mr = startMRCluster(numNodes, m_fs.getFileSystem().getUri().getAuthority(), m_conf);
//    }
//    else
//    {
//      //m_fs = new MiniDFSCluster(m_conf, numNodes, true, null);
//      m_mr = startMRCluster(numNodes, m_fs.getFileSystem().getUri()
//          .getAuthority(), m_conf);
//      //m_mr = new MiniMRCluster(numNodes, m_fs.getFileSystem().getUri().getAuthority(), 1);
//    }

    m_mr = startMRCluster(numNodes, m_fs.getFileSystem().getUri().getAuthority(), m_conf);

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    // make the home directory if it does not exist
    Path hd = fs.getWorkingDirectory();
    if (!fs.exists(hd)) fs.mkdirs(hd);
    
    // make the $USER/_temporary directory if it does not exist
    Path tmpPath = new Path(hd, "_temporary");
    if (!fs.exists(tmpPath)) fs.mkdirs(tmpPath);

    if (m_base != null)
    {
      m_admin = new HBaseAdmin((HBaseConfiguration) m_conf);
      HTableDescriptor[] tables = m_admin.listTables();
      if (tables != null)
      {
        for (int i = 0; i < tables.length; i++)
        {
          m_admin.enableTable(tables[i].getName());
        }
      }
    }
  }

  /**
   * @throws Exception
   */
  public JaqlShell() throws Exception
  {
    // do nothing in the case of cluster
    //m_conf = new HBaseConfiguration();
    //m_admin = new HBaseAdmin(m_conf);
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    // make the home directory if it does not exist
    Path hd = fs.getWorkingDirectory();
    if (!fs.exists(hd)) fs.mkdirs(hd);
  }

  /**
   * @param jars
   * @throws Exception
   */
  public void addExtensions(String[] jars) throws Exception
  {
    if (jars != null) com.ibm.jaql.lang.Jaql.addExtensionJars(jars);
  }

  /**
   * @throws Exception
   */
  public void runInteractively() throws Exception
  {
    try
    {
      com.ibm.jaql.lang.Jaql.main1(new String[]{});
      System.out.println("shutting down jaql");
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  /**
   * @throws Exception
   */
  private void setupOverride() throws Exception
  {
    File overrideDir = new File(System.getProperty("hadoop.conf.override"));
    if (!overrideDir.exists())
    {
      overrideDir.mkdirs();
    }

    // write out the JobConf from MiniMR to the override dir
    OutputStream out = new FileOutputStream(overrideDir.getCanonicalPath()
        + File.separator + "hbase-default.xml");
    m_conf.write(out);
    out.close();
  }

  /**
   * @param numNodes
   * @param nameNode
   * @param conf
   * @return
   * @throws Exception
   */
  private static MiniMRCluster startMRCluster(int numNodes, String nameNode,
      Configuration conf) throws Exception
  {
    MiniMRCluster mrc = new MiniMRCluster(numNodes, nameNode, 1);
    setupOverride(mrc, conf);

    return mrc;
  }

  /**
   * @param mrc
   * @param conf
   * @throws Exception
   */
  private static void setupOverride(MiniMRCluster mrc, Configuration conf)
      throws Exception
  {
    File overrideDir = new File(System.getProperty("hadoop.conf.override"));
    if (!overrideDir.exists())
    {
      overrideDir.mkdirs();
    }

    // write out the JobConf from MiniMR to the override dir
    JobConf jc = mrc.createJobConf();
    conf.set("mapred.job.tracker", jc.get("mapred.job.tracker", null));
    String name = "mapred.job.tracker.info.port";
    String addr = jc.get(name, null);
    if (addr == null)
    {
      name = "mapred.job.tracker.http.address";
      addr = jc.get(name, null);
    }
    conf.set(name, addr);
    OutputStream out = new FileOutputStream(overrideDir.getCanonicalPath()
        + File.separator + "hadoop-default.xml");
    conf.write(out);
    out.close();
  }

  /**
   * @throws Exception
   */
  private static void cleanupOverride() throws Exception
  {
    File overrideDir = new File(System.getProperty("hadoop.conf.override"));
    File mrOverride = new File(overrideDir, "hadoop-default.xml");
    File hbOverride = new File(overrideDir, "hbase-default.xml");
    mrOverride.delete();
    hbOverride.delete();
  }

  /**
   * @param mrc
   * @throws Exception
   */
  private static void stopMRCluster(MiniMRCluster mrc) throws Exception
  {
    mrc.shutdown();

    // clean up the temp conf dir
    cleanupOverride();
  }

  /**
   * @throws Exception
   */
  public void close() throws Exception
  {
    if (m_base != null)
    {
      stopMRCluster(m_mr);
      m_base.shutdown();
      cleanupOverride();
    }
  }

  static String USAGE = "jaql JaqlShell" + "\n\t-cluster"
                          + "\n\t-jars <list of comma separated jars>"
                          + "\n\t-dir <HDFS dir>" + "\n\t-fmt (format hdfs?)"
                          + "\n\t-n <number of nodes>";

  /**
   * @param msg
   */
  static void printUsage(String msg)
  {
    System.err.println(msg);
    System.err.println(USAGE);
    System.exit(0);
  }

  /**
   * @param args
   * @return
   */
  static HashMap<String, String> parseArgs(String[] args)
  {
    HashMap<String, String> map = new HashMap<String, String>();
    for (int i = 0; i < args.length; i++)
    {
      if ("-fmt".equals(args[i]) || "-i".equals(args[i])
          || "-cluster".equals(args[i]))
      {
        map.put(args[i], null);
      }
      else
      {
        if ((i + 1) >= args.length)
          printUsage("Incorrect args, expected pair");
        map.put(args[i], args[++i]);
      }
    }
    return map;
  }

  /**
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception
  {
    //  parse args
    HashMap<String, String> argMap = parseArgs(args);
    String dir = "/tmp/jaql/dfs";
    int numNodes = 1;
    boolean fmt = true;
    boolean mini = true;
    String[] jars = null;

    for (Iterator<String> iter = argMap.keySet().iterator(); iter.hasNext();)
    {
      String key = iter.next();
      if ("-cluster".equals(key))
      {
        mini = false;
      }
      else if ("-dir".equals(key))
      {
        dir = argMap.get(key);
      }
      else if ("-n".equals(key))
      {
        numNodes = Math.max(Integer.parseInt(argMap.get(key)), 1);
      }
      else if ("-fmt".equals(key))
      {
        fmt = true;
      }
      else if ("-jars".equals(key))
      {
        jars = argMap.get("-jars").split(",");
      }
      else
      {
        printUsage("Incorrect args: " + key);
      }
    }

    JaqlShell jaql = null;
    try
    {
      if (mini)
      {
        jaql = new JaqlShell(dir, numNodes, fmt);
      }
      else
      {
        jaql = new JaqlShell();
      }
      if (jars != null) jaql.addExtensions(jars);
      jaql.runInteractively();
      jaql.close();
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
    finally
    {
      if (jaql != null) jaql.close();
    }
  }

}
