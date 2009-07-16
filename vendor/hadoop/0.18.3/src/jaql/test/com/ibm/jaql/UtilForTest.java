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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;

/**
 * 
 */
public class UtilForTest
{
  /**
   * @param dir
   * @throws IOException
   */
  public static void cleanUpHDFS(String dir) throws IOException
  {
    if ("true".equals(System.getProperty("test.cleanup")))
    {
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(conf);
      Path p = new Path(dir);
      if (fs.exists(p))
      {
        fs.delete(p);
      }
    }
  }

  /**
   * @param prefix
   * @throws IOException
   */
  public static void cleanUpHBase(String prefix) throws IOException
  {
    if ("true".equals(System.getProperty("test.cleanup")))
    {
      HBaseConfiguration conf = new HBaseConfiguration();
      HBaseAdmin admin = new HBaseAdmin(conf);
      HTableDescriptor[] tables = admin.listTables();
      int numTables = tables.length;
      for (int i = 0; i < numTables; i++)
      {
        HTableDescriptor td = tables[i];
        String tName = td.getName().toString();
        if (tName.startsWith(prefix))
        {
          admin.disableTable(td.getName());
          admin.deleteTable(td.getName());
        }
      }
    }
  }
}
