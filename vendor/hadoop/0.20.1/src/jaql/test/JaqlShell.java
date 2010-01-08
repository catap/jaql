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
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.MiniZooKeeperCluster;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.util.VersionInfo;

import com.ibm.jaql.util.shell.AbstractJaqlShell;

/**
 * 
 */
public class JaqlShell extends AbstractJaqlShell
{

  private static final Log LOG = LogFactory.getLog(JaqlShell.class.getName());

  private Configuration    m_conf;
  private MiniHBaseCluster m_base;
  private MiniMRCluster    m_mr;
  private MiniDFSCluster   m_fs;
  private MiniZooKeeperCluster zooKeeperCluster;
  private HBaseAdmin       m_admin;

  private JaqlShell() { };
  
  /**
   * @param dir
   * @param numNodes
   * @param format
   * @throws Exception
   */
  public void init(String dir, int numNodes) throws Exception
  {
    String vInfo = VersionInfo.getVersion();
    System.setProperty("test.build.data", dir);
    m_conf = new HBaseConfiguration();

    // setup conf according to the Hadoop version
    if (vInfo.indexOf("0.20") < 0)
    {
      throw new Exception("Unsupported Hadoop version: " + vInfo);
    }
    

    // setup the mini dfs cluster
    m_fs = new MiniDFSCluster(m_conf, numNodes, true, (String[])null);
    FileSystem filesystem = m_fs.getFileSystem();
    m_conf.set("fs.default.name", filesystem.getUri().toString());
    Path parentdir = filesystem.getHomeDirectory();
    filesystem.mkdirs(parentdir);
    FSUtils.setVersion(filesystem, parentdir);
    
    // setup hbase cluster (only if OS is not windows)
    if(!System.getProperty("os.name").toLowerCase().contains("win")) {
      m_conf.set(HConstants.HBASE_DIR, parentdir.toString());      
      Path hdfsTestDir = filesystem.makeQualified(new Path(m_conf.get(HConstants.HBASE_DIR)));

      // prime the hdfs for hbase information...
      HRegion root = HRegion.createHRegion(HRegionInfo.ROOT_REGIONINFO, hdfsTestDir, (HBaseConfiguration)m_conf);
      HRegion meta = HRegion.createHRegion(HRegionInfo.FIRST_META_REGIONINFO, hdfsTestDir, (HBaseConfiguration)m_conf);
      HRegion.addRegionToMETA(root, meta);

      // ... and close the root and meta
      if (meta != null) {
        meta.close();
        meta.getLog().closeAndDelete();
      }
      if (root != null) {
        root.close();
        root.getLog().closeAndDelete();
      }

      try
      {
        this.zooKeeperCluster = new MiniZooKeeperCluster();
        File testDir = new File(dir);
        int clientPort = this.zooKeeperCluster.startup(testDir);
        m_conf.set("hbase.zookeeper.property.clientPort", Integer.toString(clientPort));
      } catch(Exception e) {
        LOG.error("Unable to startup zookeeper");
        throw new IOException(e);
      }
      try {
        // start the mini cluster
        m_base = new MiniHBaseCluster((HBaseConfiguration)m_conf, numNodes);
      } catch(Exception e) {
        LOG.error("Unable to startup hbase");
        throw new IOException(e);
      }
      try {
        // opening the META table ensures that cluster is running
        new HTable((HBaseConfiguration)m_conf, HConstants.META_TABLE_NAME);        

        //setupOverride(conf);
      }
      catch (Exception e)
      {
        LOG.warn("Could not verify that hbase is up", e);
      }
      setupOverride();
    }

    m_mr = startMRCluster(numNodes, m_fs.getFileSystem().getName(), m_conf);

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
      try {
        m_admin = new HBaseAdmin((HBaseConfiguration) m_conf);
        HTableDescriptor[] tables = m_admin.listTables();
        if (tables != null)
        {
          for (int i = 0; i < tables.length; i++)
          {
            m_admin.enableTable(tables[i].getName());
          }
        }
      } catch(Exception e) {
        LOG.warn("failed to enable hbase tables");
      }
    }
  }

  /**
   * @throws Exception
   */
  public void init() throws Exception
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
    m_conf.writeXml(out);
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
    OutputStream outCore = new FileOutputStream(overrideDir.getCanonicalPath()
        + File.separator + "core-default.xml");
    OutputStream outMapred = new FileOutputStream(overrideDir.getCanonicalPath()
        + File.separator + "mapred-default.xml");
    OutputStream outHdfs = new FileOutputStream(overrideDir.getCanonicalPath()
        + File.separator + "hdfs-default.xml");
    conf.writeXml(outCore);
    conf.writeXml(outMapred);
    conf.writeXml(outHdfs);
    outCore.close();
    outMapred.close();
    outHdfs.close();
  }

  /**
   * @throws Exception
   */
  private static void cleanupOverride() throws Exception
  {
    File overrideDir = new File(System.getProperty("hadoop.conf.override"));
    File coreOverride = new File(overrideDir, "core-default.xml");
    File mrOverride = new File(overrideDir, "mapred-default.xml");
    File hdfsOverride = new File(overrideDir, "hdfs-default.xml");
    File hbOverride = new File(overrideDir, "hbase-default.xml");
    coreOverride.delete();
    mrOverride.delete();
    hdfsOverride.delete();
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
    if (m_mr != null)
    {
      m_mr.shutdown();
      m_mr = null;
    }
    if (m_base != null)
    {
      m_base.shutdown();
      m_base = null;
    }
    if (zooKeeperCluster != null) {
      zooKeeperCluster.shutdown();
    }
    if (m_fs != null)
    {
      m_fs.shutdown();
      m_fs = null;
    }
    cleanupOverride();
  }

  

  /**
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception
  {
    try {
      main(new JaqlShell(), args);
    } catch(Exception e) {
      System.exit(1);
    }
        
    System.exit(0);
  }

}
