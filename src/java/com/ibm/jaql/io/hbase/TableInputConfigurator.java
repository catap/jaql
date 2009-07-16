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

import org.apache.hadoop.mapred.JobConf;

import com.ibm.jaql.io.AdapterStore;
import com.ibm.jaql.io.hadoop.JSONConfSetter;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JArray;
import com.ibm.jaql.json.type.JLong;
import com.ibm.jaql.json.type.JRecord;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.util.Iter;

/**
 * 
 */
public class TableInputConfigurator implements JSONConfSetter
{

  protected String  location;

  protected JRecord options;

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.hadoop.ConfSetter#init(java.lang.Object)
   */
  public void init(Item options) throws Exception
  {
    location = AdapterStore.getStore().getLocation((JRecord) options.get());
    this.options = AdapterStore.getStore().input.getOption((JRecord) options
        .get());
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.hadoop.ConfSetter#setParallel(org.apache.hadoop.mapred.JobConf)
   */
  public void setParallel(JobConf conf) throws Exception
  {
    set(conf);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.hadoop.ConfSetter#setSequential(org.apache.hadoop.mapred.JobConf)
   */
  public void setSequential(JobConf conf) throws Exception
  {
    set(conf);
  }

  /**
   * @param conf
   * @throws Exception
   */
  protected void set(JobConf conf) throws Exception
  {
    conf.set(JaqlTableInputFormat.JOB_TABLE, location);
    // set column family
    JArray columnNames = null;
    if (options != null)
    {
      columnNames = (JArray) options.getValue("columns").get();
    }
    if (columnNames == null)
    {
      conf.set(JaqlTableInputFormat.JOB_COLUMNS,
          HBaseStore.Util.DEFAULT_HBASE_COLUMN_FAMILY_NAME);
    }
    else
    {
      Iter colIter = columnNames.iter();
      Item current = null;
      StringBuilder colList = new StringBuilder();
      boolean first = true;
      while ((current = colIter.next()) != null)
      {
        if (!first)
        {
          first = false;
          colList.append(",");
        }
        JString colName = (JString) current.getNonNull();
        colList.append(HBaseStore.Util.convertColumn(colName));
      }
      conf.set(JaqlTableInputFormat.JOB_COLUMNS, colList.toString());
    }
    // get the other arguments

    if (options != null)
    {
      // set timestamp
      JLong timestampValue = (JLong) options.getValue("timestamp").get();
      if (timestampValue != null)
      {
        conf.set(JaqlTableInputFormat.JOB_TS, String
            .valueOf(timestampValue.value));
      }

      // set start key
      JString lowKeyArg = (JString) options.getValue("lowKey").get();
      if (lowKeyArg != null)
      {
        conf.set(JaqlTableInputFormat.JOB_LOWKEY, lowKeyArg.toString());
      }

      // set the end key
      JString highKeyArg = (JString) options.getValue("highKey").get();
      if (highKeyArg != null)
      {
        conf.set(JaqlTableInputFormat.JOB_HIGHKEY, highKeyArg.toString());
      }
    }
  }
}
