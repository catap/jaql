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
package com.ibm.jaql.io.hbase;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormatBase;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JRecord;

/**
 * 
 */
public class JaqlTableOutputFormat extends OutputFormatBase<Item, Item>
{

  static final Logger LOG = Logger.getLogger(JaqlTableOutputFormat.class.getName());

  /**
   * 
   */
  public JaqlTableOutputFormat()
  {
  }

  /**
   * 
   */
  protected class JaqlTableRecordWriter implements RecordWriter<Item, Item>
  {
    private HTable m_table;

    /**
     * @param table
     */
    public JaqlTableRecordWriter(HTable table)
    {
      m_table = table;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapred.RecordWriter#close(org.apache.hadoop.mapred.Reporter)
     */
    public void close(@SuppressWarnings("unused")
    Reporter reporter)
    {
      if (m_table == null)
      {
        LOG.info("attempting to close a non-existent table");
      }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapred.RecordWriter#write(java.lang.Object,
     *      java.lang.Object)
     */
    public void write(Item key, Item value) throws IOException
    {
      HBaseStore.Util.writeJMapToHBase(key, (JRecord) value.getNonNull(),
          m_table, true);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.mapred.OutputFormatBase#getRecordWriter(org.apache.hadoop.fs.FileSystem,
   *      org.apache.hadoop.mapred.JobConf, java.lang.String,
   *      org.apache.hadoop.util.Progressable)
   */
  @Override
  @SuppressWarnings("unused")
  public RecordWriter<Item, Item> getRecordWriter(FileSystem ignored,
      JobConf job, String name, Progressable progress) throws IOException
  {

    // expecting exactly one path

    String tableName = job.get(TableOutputFormat.OUTPUT_TABLE);

    HTable table = null;
    try
    {
      table = new HTable(new HBaseConfiguration(), tableName);
    }
    catch (Exception e)
    {
      LOG.error(e);
    }
    return new JaqlTableRecordWriter(table);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.mapred.OutputFormatBase#checkOutputSpecs(org.apache.hadoop.fs.FileSystem,
   *      org.apache.hadoop.mapred.JobConf)
   */
  @Override
  @SuppressWarnings("unused")
  public void checkOutputSpecs(FileSystem ignored, JobConf job)
      throws FileAlreadyExistsException, InvalidJobConfException, IOException
  {

    String tableName = job.get(TableOutputFormat.OUTPUT_TABLE);
    if (tableName == null)
    {
      throw new IOException("Must specify table name");
    }
  }
}
