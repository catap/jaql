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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.util.VersionInfo;

import com.ibm.jaql.JaqlBaseTestCase;
import com.ibm.jaql.TestMapReduce;
import com.ibm.jaql.UtilForTest;

/**
 * 
 */
public class TestMapReduce extends JaqlBaseTestCase
{

  private static final Log LOG        = LogFactory.getLog(TestMapReduce.class
                                          .getName());

  private MiniDFSCluster   m_dfs      = null;
  private MiniMRCluster    m_mr       = null;
  public static String     dataPrefix = "jaqlTest";
  public static String     tempDir    = "/temp";

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.JaqlBaseTestCase#setUp()
   */
  @Override
  protected void setUp() throws IOException
  {
    setFilePrefix("mr");

    final int numNodes = 4;
    Configuration conf = null;
    if (!"cluster".equals(System.getProperty("test.deployment")))
    {
      conf = new Configuration();
      try
      {
        m_dfs = new MiniDFSCluster(conf, numNodes, true, null);
      }
      catch (IOException ie)
      {
        fail("Could not setup dfs cluster");
      }

      try
      {
        m_mr = new MiniMRCluster(numNodes, m_dfs.getFileSystem().getUri()
            .getAuthority(), 1);
      }
      catch (IOException ie)
      {
        fail("Could not setup map/reduce cluster");
      }

      try
      {
        setupOverride(m_mr.createJobConf(), conf);
      }
      catch (IOException ie)
      {
        fail("Could not setup override conf file");
      }
    }
    conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    // make the home directory if it does not exist
    Path hd = fs.getWorkingDirectory();
    if (!fs.exists(hd)) fs.mkdirs(hd);
    
    // make the $USER/_temporary directory if it does not exist
    Path tmpPath = new Path(hd, "_temporary");
    if (!fs.exists(tmpPath)) fs.mkdirs(tmpPath);

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
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.JaqlBaseTestCase#tearDown()
   */
  @Override
  protected void tearDown() throws IOException
  {
    if (m_mr != null) m_mr.shutdown();
    if (m_dfs != null)
    {
      m_dfs.getFileSystem().close();
      m_dfs.shutdown();
    }
    if ("cluster".equals(System.getProperty("test.deployment")))
    {
      UtilForTest.cleanUpHDFS(dataPrefix);
      UtilForTest.cleanUpHDFS(tempDir);
    }
  }

  /**
   * @param jconf
   * @param conf
   * @throws IOException
   */
  static void setupOverride(JobConf jconf, Configuration conf)
      throws IOException
  {
    File overrideDir = new File(System.getProperty("hadoop.conf.override"));
    // assume that override dir exists for unit tests
    if (!overrideDir.exists()) fail("hadoop-override dir must exist");

    // write out the JobConf from MiniMR to the override dir
    jconf.writeXml(System.out);
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

    File f = new File(overrideDir.getCanonicalPath() + File.separator
        + "hadoop-default.xml");
    f.deleteOnExit();
    OutputStream out = new FileOutputStream(f);
    conf.writeXml(out);
    out.flush();
    out.close();

  }
}
