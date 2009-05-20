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
import com.ibm.jaql.io.hadoop.InitializableConfSetter;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.util.JaqlUtil;

/**
 * 
 */
public class TableInputConfigurator implements InitializableConfSetter
{

  protected String  location;

  protected JsonRecord options;

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.hadoop.ConfSetter#init(java.lang.Object)
   */
  public void init(JsonValue options) throws Exception
  {
    location = AdapterStore.getStore().getLocation((JsonRecord) options);
    this.options = AdapterStore.getStore().input.getOption((JsonRecord) options);
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
    JsonArray columnNames = null;
    if (options != null)
    {
      columnNames = (JsonArray) options.getValue("columns");
    }
    if (columnNames == null)
    {
      conf.set(JaqlTableInputFormat.JOB_COLUMNS,
          HBaseStore.Util.DEFAULT_HBASE_COLUMN_FAMILY_NAME);
    }
    else
    {
      JsonIterator colIter = columnNames.iter();
      StringBuilder colList = new StringBuilder();
      boolean first = true;
      for (JsonValue current : colIter)
      {
        if (!first)
        {
          first = false;
          colList.append(",");
        }
        JsonString colName = JaqlUtil.enforceNonNull((JsonString) current);
        colList.append(HBaseStore.Util.convertColumn(colName));
      }
      conf.set(JaqlTableInputFormat.JOB_COLUMNS, colList.toString());
    }
    // get the other arguments

    if (options != null)
    {
      // set timestamp
      JsonLong timestampValue = (JsonLong) options.getValue("timestamp");
      if (timestampValue != null)
      {
        conf.set(JaqlTableInputFormat.JOB_TS, String
            .valueOf(timestampValue.value));
      }

      // set start key
      JsonString lowKeyArg = (JsonString) options.getValue("lowKey");
      if (lowKeyArg != null)
      {
        conf.set(JaqlTableInputFormat.JOB_LOWKEY, lowKeyArg.toString());
      }

      // set the end key
      JsonString highKeyArg = (JsonString) options.getValue("highKey");
      if (highKeyArg != null)
      {
        conf.set(JaqlTableInputFormat.JOB_HIGHKEY, highKeyArg.toString());
      }
    }
  }
}
