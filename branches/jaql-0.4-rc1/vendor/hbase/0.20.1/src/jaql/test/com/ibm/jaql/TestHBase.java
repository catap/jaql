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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.FSUtils;

import com.ibm.jaql.JaqlBaseTestCase;
import com.ibm.jaql.TestHBase;
import com.ibm.jaql.TestMapReduce;
import com.ibm.jaql.UtilForTest;

/**
 * 
 */
public class TestHBase extends JaqlBaseTestCase
{

  private static final Log LOG        = LogFactory.getLog(TestHBase.class
                                          .getName());
  private Configuration    m_conf;
  private MiniMRCluster    m_mr       = null;
  private MiniHBaseCluster m_db       = null;
  private MiniDFSCluster   m_fs       = null;
  private MiniZooKeeperCluster zooKeeperCluster;
  public static String     dataPrefix = "jaqlTest";
  public static String     tempDir    = "/temp";
  private HBaseAdmin       m_admin;

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.JaqlBaseTestCase#setUp()
   */
  @Override
  protected void setUp() throws IOException
  {
    setFilePrefix("hbase");

    if (!"cluster".equals(System.getProperty("test.deployment")))
    {
      m_conf = new HBaseConfiguration();

      String vInfo = VersionInfo.getVersion();
      if (vInfo.indexOf("0.20") < 0)
      {
        throw new RuntimeException("Unsupported Hadoop version: " + vInfo);
      }

      final int numNodes = 2;

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
          File testDir = new File(System.getProperty("test.cache.data")).getParentFile();
          int clientPort = this.zooKeeperCluster.startup(testDir);
          m_conf.set("hbase.zookeeper.property.clientPort", Integer.toString(clientPort));
        } catch(Exception e) {
          LOG.error("Unable to startup zookeeper");
          throw new IOException(e);
        }
        try {
          // start the mini cluster
          m_db = new MiniHBaseCluster((HBaseConfiguration)m_conf, numNodes);
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
        setupOverride((HBaseConfiguration)m_conf);

        m_mr = new MiniMRCluster(numNodes, filesystem.getUri().toString(), 1);
        m_conf.set("mapred.output.dir", m_conf.get("hadoop.tmp.dir"));
        try
        {
          TestMapReduce.setupOverride(m_mr.createJobConf(), m_conf);
        }
        catch (Exception e)
        {
          e.printStackTrace(System.out);
          System.out.flush();
          fail("could not setup map-reduce override");
        }
      }
    }
    //Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(m_conf);

    // make the home directory if it does not exist
    Path hd = fs.getWorkingDirectory();
    if (!fs.exists(hd)) fs.mkdirs(hd);

    // make a data directory if it does not exist
    Path p = new Path(dataPrefix);
    if (fs.exists(p))
    {
      LOG.info("directory: " + p + ", exists");
      fs.delete(p, true);
    }
    fs.mkdirs(p);

    // make a tmp directory if it does not exist
    Path t = new Path(tempDir);
    if (!fs.exists(t))
    {
      fs.mkdirs(t);
    }
    
    if (m_db != null)
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
        LOG.warn("unable to enable existing tables:" + e);
      }
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.JaqlBaseTestCase#tearDown()
   */
  @Override
  protected void tearDown() throws IOException
  {
    if (m_mr != null)
    {
      m_mr.shutdown();
    }
    try {
      HConnectionManager.deleteConnectionInfo((HBaseConfiguration)m_conf, true);
      if (this.m_db != null) {
        try {
          this.m_db.shutdown();
        } catch (Exception e) {
          LOG.warn("Closing mini dfs", e);
        }
        try {
          this.zooKeeperCluster.shutdown();
        } catch (IOException e) {
          LOG.warn("Shutting down ZooKeeper cluster", e);
        }
      }
      FileSystem fs = m_fs.getFileSystem();
      fs.close();
      m_fs.shutdown();
    } catch (Exception e) {
      LOG.error(e);
    }
    
    if ("cluster".equals(System.getProperty("test.deployment")))
    {
      UtilForTest.cleanUpHDFS(dataPrefix);
      UtilForTest.cleanUpHBase(dataPrefix);
      UtilForTest.cleanUpHDFS(tempDir);
    }
  }

  /**
   * @param conf
   * @throws IOException
   */
  static void setupOverride(HBaseConfiguration conf) throws IOException
  {
    File overrideDir = new File(System.getProperty("hadoop.conf.override"));
    // assume that override dir exists for unit tests
    if (!overrideDir.exists()) fail("hadoop-override dir must exist");

    // write out the JobConf from MiniMR to the override dir
    File f = new File(overrideDir.getCanonicalPath() + File.separator
        + "hbase-default.xml");
    f.deleteOnExit();
    OutputStream out = new FileOutputStream(f);
    conf.writeXml(out);
    out.flush();
    out.close();
  }
}
