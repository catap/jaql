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
import java.util.ArrayList;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapred.TableSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import com.ibm.jaql.io.ClosableJsonIterator;
import com.ibm.jaql.io.hadoop.JsonHolder;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonValue;

/**
 * 
 */
public class JaqlTableInputFormat
    implements InputFormat<JsonHolder, JsonHolder>, JobConfigurable
{
  static final Logger  LOG         = Logger.getLogger(JaqlTableInputFormat.class.getName());

  public static String JOB_ARGS    = "com.ibm.jaql.lang.JaqlTableInputFormat";

  public static String JOB_TABLE   = JOB_ARGS + ".table";

  public static String JOB_COLUMNS = JOB_ARGS + ".columns";

  public static String JOB_LOWKEY  = JOB_ARGS + ".lowkey";

  public static String JOB_HIGHKEY = JOB_ARGS + ".highkey";

  public static String JOB_TS      = JOB_ARGS + ".ts";

  private byte[]         tableName;

  private JsonString[]       columnNames;

  private JsonString         lowKey      = null;

  @SuppressWarnings("unused")
  private JsonString         highKey     = null;

  private long         timeStamp   = -1;

  private HTable       table;

  /**
   * 
   */
  public JaqlTableInputFormat()
  {
  }

  /**
   * 
   */
  public class JaqlTableRecordReader implements RecordReader<JsonHolder, JsonHolder>
  {
    // replace with Muse Iter
    private ClosableJsonIterator tupleIter;

    private boolean      hasMore;

    /**
     * @param split
     * @param job
     * @param reporter
     * @throws IOException
     */
    public JaqlTableRecordReader(TableSplit split, JobConf job,
        Reporter reporter) throws IOException
    {
      // setup tuple memory
      BufferedJsonRecord current = new BufferedJsonRecord();

      // setup startKey
      JsonString startKey = new JsonString(split.getStartRow());
      if (lowKey != null && lowKey.compareTo(startKey) > 0) startKey = lowKey;

      // create iterator
      LOG.info("Opening iterator on " + table.getTableName());
      try
      {
        // TODO: not clear that this is the right context?
        JsonString endKey = new JsonString(split.getEndRow());
        tupleIter = HBaseStore.Util.createResultBase(table, startKey, endKey,
            columnNames, timeStamp, current);
      }
      catch (Exception e)
      {
        throw new IOException(e.getMessage());
      }
      hasMore = true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapred.RecordReader#close()
     */
    public void close() throws IOException
    {
      tupleIter.close();
    }

    /*
     * @return Tuple
     * 
     * @see org.apache.hadoop.mapred.RecordReader#createKey()
     */
    public JsonHolder createKey()
    {
      return new JsonHolder();
    }

    /*
     * @return Tuple
     * 
     * @see org.apache.hadoop.mapred.RecordReader#createValue()
     */
    public JsonHolder createValue()
    {
      return new JsonHolder();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapred.RecordReader#getPos()
     */
    public long getPos()
    {
      // This should be the ordinal tuple in the range;
      // not clear how to calculate...
      return 0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapred.RecordReader#getProgress()
     */
    public float getProgress()
    {
      // Depends on the total number of tuples and getPos
      return 0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapred.RecordReader#next(java.lang.Object,
     *      java.lang.Object)
     */
    public boolean next(JsonHolder key, JsonHolder value) throws IOException
    {
      if (!hasMore) return hasMore;

      // assume that Iter has been passed value during its setup and will set it
      // to current tuple
      try
      {
        if (!tupleIter.moveNext())
        {
          hasMore = false;
        }
        else
        {
          JsonValue t = tupleIter.current();
          LOG.info("Retrieved tuple: " + t);
          value.value = t;
        }
      }
      catch (IOException e)
      {
        throw e;
      }
      catch (Exception e)
      {
        throw new IOException(e.getMessage());
      }
      return hasMore;
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.mapred.InputFormat#getRecordReader(org.apache.hadoop.mapred.InputSplit,
   *      org.apache.hadoop.mapred.JobConf, org.apache.hadoop.mapred.Reporter)
   */
  public RecordReader<JsonHolder, JsonHolder> getRecordReader(InputSplit split,
      JobConf job, Reporter reporter) throws IOException
  {

    TableSplit tSplit = (TableSplit) split;
    return new JaqlTableRecordReader(tSplit, job, reporter);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.mapred.InputFormat#getSplits(org.apache.hadoop.mapred.JobConf,
   *      int)
   */
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException
  {
    byte[][] startKeys = table.getStartKeys();
    if (startKeys == null || startKeys.length == 0)
    {
      throw new IOException("Expecting at least one region");
    }

    ArrayList<TableSplit> splitList = new ArrayList<TableSplit>();
    InputSplit[] splits = new InputSplit[startKeys.length];
    for (int i = 0; i < startKeys.length; i++)
    {
      String start = new String(startKeys[i]);
      String end = ((i + 1) < startKeys.length) ? new String(startKeys[i + 1]) : "";
      if (lowKey == null)
      {
        // add all splits if no starting point was given
        splitList.add(new TableSplit(tableName, start.getBytes(), end.getBytes()));
      }
      else if (lowKey.compareTo(end) < 0)
      {
        // add a split if its end point is greater than the given starting point
        splitList.add(new TableSplit(tableName, start.getBytes(), end.getBytes()));
        LOG.debug("split: " + i + "->" + splits[i]);
      }
    }
    return splitList.toArray(new TableSplit[]{});
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.mapred.JobConfigurable#configure(org.apache.hadoop.mapred.JobConf)
   */
  public void configure(JobConf job)
  {
    // table name
    tableName = job.get(JOB_TABLE).getBytes();

    // column names
    String colArg = job.get(JOB_COLUMNS);
    String[] splitArr = colArg.split(" ");
    columnNames = new JsonString[splitArr.length];
    for (int i = 0; i < columnNames.length; i++)
    {
      columnNames[i] = new JsonString(splitArr[i]);
    }

    // option arguments

    // low key
    String lowKeyArg = job.get(JOB_LOWKEY);
    if (lowKeyArg != null) lowKey = new JsonString(lowKeyArg);

    // high key
    String highKeyArg = job.get(JOB_HIGHKEY);
    if (highKeyArg != null) highKey = new JsonString(highKeyArg);

    // timestamp
    String timestampArg = job.get(JOB_TS);
    if (timestampArg != null) timeStamp = Long.parseLong(timestampArg);

    // setup the table interface
    try
    {
      table = new HTable(new HBaseConfiguration(), tableName);
    }
    catch (Exception e)
    {
      LOG.error(e);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.mapred.InputFormat#validateInput(org.apache.hadoop.mapred.JobConf)
   */
  public void validateInput(JobConf job) throws IOException
  {
    // expecting one table name

    String tableArg = job.get(JOB_TABLE);
    if (tableArg == null || tableArg.length() == 0)
    {
      throw new IOException("expecting a table name");
    }

    // expecting at least one column

    String colArg = job.get(JOB_COLUMNS);
    if (colArg == null || colArg.length() == 0)
    {
      throw new IOException("expecting at least one column");
    }
  }
}
