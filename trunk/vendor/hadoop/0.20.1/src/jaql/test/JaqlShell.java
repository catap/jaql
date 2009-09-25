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

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
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
  
  @Override
  public void init(String dir, int numNodes) throws Exception
  {
    String vInfo = VersionInfo.getVersion();
    System.setProperty("test.build.data", dir);
    m_conf = new HBaseConfiguration();

    // setup conf according to the Hadoop version
    if (vInfo.indexOf("0.20") >= 0)
    {
      m_conf.set(HConstants.CLUSTER_IS_LOCAL, "true");
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
      String hbvInfo = org.apache.hadoop.hbase.util.VersionInfo.getVersion();
      if (hbvInfo.indexOf("0.20") >= 0)
      {
        this.zooKeeperCluster = new MiniZooKeeperCluster();
        int clientPort = this.zooKeeperCluster.startup(new File(dir));
        m_conf.set("hbase.zookeeper.property.clientPort", Integer.toString(clientPort));

        // start the mini cluster
        m_base = new MiniHBaseCluster((HBaseConfiguration)m_conf, numNodes);

        // opening the META table ensures that cluster is running
        new HTable((HBaseConfiguration)m_conf, HConstants.META_TABLE_NAME);
        m_base = new MiniHBaseCluster((HBaseConfiguration) m_conf, numNodes);
        //setupOverride();
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

  @Override
  public void init() throws Exception
  {
    // do nothing in the case of cluster
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
    DataOutputStream out = new DataOutputStream(new FileOutputStream(overrideDir.getCanonicalPath()
        + File.separator + "hbase-default.xml"));
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
    DataOutputStream out = new DataOutputStream(new FileOutputStream(overrideDir.getCanonicalPath()
        + File.separator + "hadoop-default.xml"));
    conf.write(out);
    out.close();
  }

  /**
   * Removes the default configurations for hadoop and hbase.
   * 
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

  @Override
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

  public static void main(String[] args) throws Exception {
  	main(new JaqlShell(), args);
  }

}
