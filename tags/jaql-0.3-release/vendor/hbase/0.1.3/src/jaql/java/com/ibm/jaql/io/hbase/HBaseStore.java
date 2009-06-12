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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HScannerInterface;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.HTable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JArray;
import com.ibm.jaql.json.type.JLong;
import com.ibm.jaql.json.type.JRecord;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.type.MemoryJRecord;
import com.ibm.jaql.json.type.SpillJArray;
import com.ibm.jaql.json.util.ClosableIter;
import com.ibm.jaql.json.util.Iter;

/**
 * 
 */
public class HBaseStore
{

  /**
   * 
   */
  public static final class Util
  {
    public final static char                  HBASE_CF_SEPARATOR_CHAR            = ':';

    public final static char                  JAQL_CF_SEPARATOR_CHAR             = '#';

    public final static String                DEFAULT_COLUMN_FAMILY_NAME         = "DEFAULT_COLUMN_FAMILY";

    public final static String                DEFAULT_HBASE_COLUMN_FAMILY_NAME   = DEFAULT_COLUMN_FAMILY_NAME
                                                                                     + HBASE_CF_SEPARATOR_CHAR;

    public final static HColumnDescriptor     DEFAULT_COLUMN_FAMILY              = new HColumnDescriptor(
                                                                                     DEFAULT_HBASE_COLUMN_FAMILY_NAME);

    public final static JString               J_DEFAULT_HBASE_COLUMN_FAMILY_NAME = new JString(
                                                                                     DEFAULT_HBASE_COLUMN_FAMILY_NAME);

    public final static String                KEY_NAME                           = "key";

    public final static Text                  T_KEY                              = new Text(
                                                                                     KEY_NAME);

    public final static JString               J_KEY                              = new JString(
                                                                                     KEY_NAME);

    public final static HBaseConfiguration    hbaseConf                          = new HBaseConfiguration();

    protected static HashMap<JString, HTable> htableMap                          = new HashMap<JString, HTable>();

    /**
     * @param s
     * @return
     */
    public static Text makeText(JString s)
    {
      Text t = new Text();
      t.set(s.getBytes(), 0, s.getLength());
      return t;
    }

    /**
     * @param tableName
     * @return
     * @throws IOException
     */
    public static HTable openHTable(JString tableName) throws IOException
    {
      HTable htable = htableMap.get(tableName);
      if (htable == null)
      {
        // TODO: mismatch between Text and JString
        htable = new HTable(hbaseConf, makeText(tableName));
      }
      return htable;
    }

    /**
     * @param htable
     */
    public static void closeHTable(HTable htable)
    {
      // TODO: all tables are left open until jvm exit. Should they be closed
      // earlier?
    }

    /**
     * @param w
     * @return
     * @throws IOException
     */
    public static byte[] convertToBytes(Writable w) throws IOException
    {
      ByteArrayOutputStream bstr = new ByteArrayOutputStream();
      DataOutputStream str = new DataOutputStream(bstr);
      w.write(str);
      str.close();
      return bstr.toByteArray();
    }

    /**
     * @param val
     * @return
     * @throws IOException
     */
    public static Item convertFromBytes(byte[] val) throws IOException
    {
      // TODO: memory
      DataInputStream str = new DataInputStream(new ByteArrayInputStream(val));
      Item i = new Item();
      i.readFields(str);
      return i;
    }

    /**
     * @param colName
     * @param item
     * @param rec
     * @throws IOException
     */
    public static void setMap(JString colName, Item item, MemoryJRecord rec)
        throws IOException
    {
      // unpeel the column family only if its the default column family
      if (colName.startsWith(J_DEFAULT_HBASE_COLUMN_FAMILY_NAME))
      {
        colName.removeBytes(0, J_DEFAULT_HBASE_COLUMN_FAMILY_NAME.getLength());
      }
      else
      {
        colName.replace(HBASE_CF_SEPARATOR_CHAR, JAQL_CF_SEPARATOR_CHAR);
      }
      int idx = rec.findName(colName);
      if (idx < 0)
        rec.add(colName, item);
      else
        rec.set(idx, item);
    }

    /**
     * @param colName
     * @param colVal
     * @param item
     * @param rec
     * @throws IOException
     */
    public static void convertFromBytes(JString colName, byte[] colVal,
        Item item, MemoryJRecord rec) throws IOException
    {
      DataInputStream str = new DataInputStream(
          new ByteArrayInputStream(colVal)); // TODO: memory
      item.readFields(str);

      setMap(colName, item, rec);
    }

    /**
     * convert from jaql to hbase
     * 
     * @param col
     * @return
     * @throws Exception
     */
    public static JString convertColumn(JString col) throws Exception
    {
      JString ncol = null;
      // add the default column family if one is not specified
      if (col.indexOf(HBASE_CF_SEPARATOR_CHAR) < 0)
      {
        ncol = new JString(DEFAULT_HBASE_COLUMN_FAMILY_NAME + col);
      }
      else
      {
        // TODO: memory
        String o = col.toString();
        String n = o.replace(JAQL_CF_SEPARATOR_CHAR, HBASE_CF_SEPARATOR_CHAR);
        ncol = new JString(n);
      }
      return ncol;
    }

    /**
     * @param columns
     * @return
     * @throws Exception
     */
    public static JString[] convertColumns(JArray columns) throws Exception
    {
      JString[] cols = null;
      if (columns != null)
      {
        int ncols = (int) columns.count();
        cols = new JString[ncols];
        Iter colIter = columns.iter();
        for (int i = 0; i < ncols; i++)
        {
          JString col = (JString) colIter.next().getNonNull();
          cols[i] = convertColumn(col);
        }
      }
      return cols;
    }

    /**
     * @param key
     * @param row
     * @param rec
     * @throws IOException
     */
    public static void convertMap(Text key, SortedMap<Text, byte[]> row,
        MemoryJRecord rec) throws IOException
    {
      rec.clear();
      rec.ensureCapacity(row.size() + 1);
      int i = 0;
      JString name = rec.getName(i);
      Item item = rec.getValue(i);
      i++;
      name.copy(J_KEY);
      item.set(new JString(key.getBytes(), key.getLength())); // TODO: memory
      rec.add(name, item);
      for (Map.Entry<Text, byte[]> e : row.entrySet())
      {
        Text n = e.getKey();
        if (!n.equals(T_KEY))
        {
          name = rec.getName(i);
          item = rec.getValue(i);
          i++;
          name.copy(n.getBytes(), n.getLength());
          convertFromBytes(name, e.getValue(), item, rec);
        }
      }
    }

    /**
     * 
     * @param key
     *            key associated with tuple. If null, extract from museValue
     *            (name='key')
     * @param rec
     *            the tuple to insert
     * @param hbaseKey
     *            the hbase key that gets set with the converted muse key
     * @param client
     *            the hbase client
     * @param extractKey
     *            extract the key from the museValue
     * @throws IOException
     */
    public static void writeJMapToHBase(Item key, JRecord rec, HTable table,
        boolean extractKey) throws IOException
    {
      if (key == null && !extractKey)
      {
        throw new RuntimeException("key must be specified or extracted");
      }

      // convert key to hbaseKey
      if (extractKey)
      {
        key = rec.getValue(J_KEY, null);
      }
      JString hbaseKey = (JString) key.getNonNull();
      Text t_hbaseKey = makeText(hbaseKey);

      // start transaction

      // TODO: mismatch between Text and JString
      long xid = table.startUpdate(t_hbaseKey);
      int arity = rec.arity();
      for (int i = 0; i < arity; i++)
      {
        JString columnName = rec.getName(i);
        if (columnName.equals(J_KEY)) continue; // skip the key
        // specify the default column family only when no column family is
        // specified
        if (columnName.indexOf(JAQL_CF_SEPARATOR_CHAR) < 0)
        {
          columnName = new JString(DEFAULT_HBASE_COLUMN_FAMILY_NAME
              + columnName);
        }
        else
        {
          // TODO: memory
          columnName = new JString(columnName);
          columnName.replace(JAQL_CF_SEPARATOR_CHAR, HBASE_CF_SEPARATOR_CHAR);
        }
        Item val = rec.getValue(i);
        byte[] valueBytes = convertToBytes(val);
        table.put(xid, makeText(columnName), valueBytes);
      }

      // end transaction

      table.commit(xid);
    }

    /**
     * 
     * @param table
     * @param ey
     * @param columns
     *            (HBase formatted)
     * @throws IOException
     */
    public static void deleteFromHBase(HTable table, JString key,
        JString[] columns) throws Exception
    {
      // TODO: mismatch between Text and JString
      Text hbaseKey = makeText(key);
      // start the transaction
      long xid = table.startUpdate(hbaseKey);

      try
      {
        if (columns == null)
        {
          // TODO: hbase needs a delete all columns method.
          SortedMap<Text, byte[]> row = table.getRow(hbaseKey);
          columns = row.keySet().toArray(columns);
        }
        for (JString col : columns)
        {
          // int start = col.find(HBASE_CF_SEPARATOR_CHAR) + 1;
          table.delete(xid, makeText(col));
        }
        // end the transaction
        table.commit(xid);
      }
      catch (Exception e)
      {
        // abort the transaction
        table.abort(xid);
        throw e;
      }
    }

    /**
     * @param table
     * @param startKey
     * @param stopKey
     * @param columnNames
     * @param timestamp
     * @param current
     * @return
     * @throws IOException
     */
    public static ClosableIter createResult(HTable table, JString startKey,
        final JString stopKey, JString[] columnNames, long timestamp,
        final MemoryJRecord current) throws IOException
    {
      Text hbaseStartKey = makeText(startKey);
      Text hbaseStopKey = makeText(stopKey);
      Text[] hbaseColumnNames = new Text[columnNames.length];
      for (int i = 0; i < columnNames.length; i++)
      {
        hbaseColumnNames[i] = makeText(columnNames[i]);
      }
      return createResultBase(table, hbaseStartKey, hbaseStopKey,
          hbaseColumnNames, timestamp, current);
    }

    /**
     * @param table
     * @param startKey
     * @param stopKey
     * @param columnNames
     * @param timestamp
     * @param current
     * @return
     * @throws IOException
     */
    public static ClosableIter createResultBase(HTable table, Text startKey,
        final Text stopKey, Text[] columnNames, long timestamp,
        final MemoryJRecord current) throws IOException
    {
      HScannerInterface tmpScanner = null;
      if (timestamp >= 0)
        tmpScanner = table.obtainScanner(columnNames, startKey, timestamp);
      else
        tmpScanner = table.obtainScanner(columnNames, startKey);
      final HScannerInterface scanner = tmpScanner;

      return new ClosableIter() {
        HStoreKey               key    = new HStoreKey();

        SortedMap<Text, byte[]> row    = new TreeMap<Text, byte[]>();

        Item                    result = new Item(current);

        public Item next() throws Exception
        {
          row.clear();
          if (scanner.next(key, row))
          {
            if (stopKey == null || stopKey.getLength() == 0
                || key.getRow().compareTo(stopKey) <= 0)
            {
              Text keyVal = key.getRow(); // TODO: memory
              HBaseStore.Util.convertMap(keyVal, row, current);
              return result;
            }
          }
          return null;
        }

        public void close() throws IOException
        {
          scanner.close();
        }
      };
    }

    /**
     * @param tableName
     * @param columnNames
     * @param rows
     * @throws Exception
     */
    public static void deleteValues(JString tableName, JArray columnNames,
        Iter rows) throws Exception
    {
      // setup the columns
      JString[] cols = convertColumns(columnNames);

      // open the table
      HTable table = openHTable(tableName);

      Item keyItem;
      while ((keyItem = rows.next()) != null)
      {
        JString key = (JString) keyItem.getNonNull();
        HBaseStore.Util.deleteFromHBase(table, key, cols);
      }

      HBaseStore.Util.closeHTable(table);
    }

    /**
     * @param tableName
     * @param columnNames
     * @param timestampValue
     * @param numVersionsValue
     * @param rows
     * @return
     * @throws Exception
     */
    public static Iter fetchRecords(JString tableName, JArray columnNames,
        JLong timestampValue, JLong numVersionsValue, Iter rows)
        throws Exception
    {
      // open the table
      HTable table = HBaseStore.Util.openHTable(tableName);

      // setup the columns
      JString[] cols = HBaseStore.Util.convertColumns(columnNames);

      // setup the timestamp
      long timestamp = (timestampValue != null) ? timestampValue.value : -1;

      // setup the number of versions
      int numVersions = (numVersionsValue != null)
          ? (int) numVersionsValue.value
          : -1;

      Iter result = null;

      if (columnNames == null)
      {
        // Fetch entire rows if no columns are specified
        result = new FetchAllColumnsIter(table, rows);
      }
      else if (numVersions <= 0)
      {
        // Fetch single version rows
        result = new FetchSingleVersionIter(table, rows, cols);
      }
      else
      {
        // Fetch multi-version rows
        result = new FetchMultiVersionIter(table, rows, cols, timestamp,
            numVersions);
      }
      return result;
    }
  }

  /**
   * 
   */
  static abstract class FetchIter extends Iter
  {
    protected HTable        table;

    protected Iter          keyIter;

    protected MemoryJRecord rec;

    protected Item          result;

    /**
     * @param table
     * @param keyIter
     * @param numCols
     * @throws Exception
     */
    public FetchIter(HTable table, Iter keyIter, int numCols) throws Exception
    {
      this.table = table;
      this.keyIter = keyIter;
      this.rec = new MemoryJRecord(numCols);
      this.result = new Item(rec);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.ibm.jaql.json.util.Iter#next()
     */
    public Item next() throws Exception
    {
      while (true)
      {
        Item keyItem = keyIter.next();
        if (keyItem == null)
        {
          HBaseStore.Util.closeHTable(table);
          return null;
        }

        JString hbaseKey = (JString) keyItem.get();
        if (hbaseKey != null)
        {
          if (fetchRecord(hbaseKey))
          {
            rec.set(HBaseStore.Util.J_KEY, new Item(hbaseKey));
            return result;
          }
        }
      }
    }

    /**
     * @param key
     * @return
     * @throws Exception
     */
    protected abstract boolean fetchRecord(JString key) throws Exception;
  }

  /**
   * 
   */
  static class FetchAllColumnsIter extends FetchIter
  {
    /**
     * @param table
     * @param keyIter
     * @throws Exception
     */
    public FetchAllColumnsIter(HTable table, Iter keyIter) throws Exception
    {
      super(table, keyIter, 10);
      rec = new MemoryJRecord(10);
      result = new Item(rec);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.ibm.jaql.io.hbase.HBaseStore.FetchIter#fetchRecord(com.ibm.jaql.json.type.JString)
     */
    protected boolean fetchRecord(JString key) throws Exception
    {
      Text tkey = HBaseStore.Util.makeText(key);
      SortedMap<Text, byte[]> row = table.getRow(tkey);
      if (row.size() > 0)
      {
        rec.clear();
        HBaseStore.Util.convertMap(tkey, row, rec);
        return true;
      }
      return false;
    }
  }

  /**
   * 
   */
  static class FetchSingleVersionIter extends FetchIter
  {
    protected JString[] jcols;

    protected Text[]    tcols;

    /**
     * @param table
     * @param keyIter
     * @param cols
     * @throws Exception
     */
    public FetchSingleVersionIter(HTable table, Iter keyIter, JString[] cols)
      throws Exception
    {
      super(table, keyIter, cols.length + 1);
      this.jcols = cols;
      this.tcols = new Text[jcols.length];
      for (int i = 0; i < jcols.length; i++)
      {
        tcols[i] = HBaseStore.Util.makeText(jcols[i]);
      }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.ibm.jaql.io.hbase.HBaseStore.FetchIter#fetchRecord(com.ibm.jaql.json.type.JString)
     */
    protected boolean fetchRecord(JString key) throws Exception
    {
      Text tkey = HBaseStore.Util.makeText(key);
      boolean hasValue = false;
      for (int i = 0; i < jcols.length; i++)
      {
        byte[] value = table.get(tkey, tcols[i]);

        if (value != null)
        {
          int idx = rec.findName(jcols[i]);
          Item item = null;
          if (idx < 0)
            item = new Item();
          else
            item = rec.getValue(idx);
          HBaseStore.Util.convertFromBytes(jcols[i], value, item, rec);
          hasValue = true;
        }
      }
      return hasValue;
    }
  }

  /**
   * 
   */
  static class FetchMultiVersionIter extends FetchSingleVersionIter
  {
    protected long timestamp;

    protected int  numVersions;

    /**
     * @param table
     * @param keyIter
     * @param cols
     * @param timestamp
     * @param numVersions
     * @throws Exception
     */
    public FetchMultiVersionIter(HTable table, Iter keyIter, JString[] cols,
        long timestamp, int numVersions) throws Exception
    {
      super(table, keyIter, cols);
      this.timestamp = timestamp;
      this.numVersions = numVersions;
      for (int i = 0; i < cols.length; i++)
      {
        Item v = rec.getValue(i + 1);
        v.set(new SpillJArray());
        rec.set(i + 1, v);
      }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.ibm.jaql.io.hbase.HBaseStore.FetchSingleVersionIter#fetchRecord(com.ibm.jaql.json.type.JString)
     */
    protected boolean fetchRecord(JString key) throws Exception
    {
      Text tkey = HBaseStore.Util.makeText(key);
      boolean hasValue = false;
      for (int i = 0; i < jcols.length; i++)
      {
        byte[][] value = null;
        if (timestamp < 0)
          value = table.get(tkey, tcols[i], numVersions);
        else
          value = table.get(tkey, tcols[i], timestamp, numVersions);

        if (value != null && value.length > 0)
        {
          Item item = rec.getValue(i + 1);
          SpillJArray tArr = (SpillJArray) item.getNonNull();
          tArr.clear();

          for (int j = 0; j < value.length; j++)
          {
            tArr.addSerialized(value[j], 0, value[j].length);
          }
          tArr.freeze();

          // set the top-level map
          HBaseStore.Util.setMap(jcols[i], item, rec);
          hasValue = true;
        }
      }
      return hasValue;
    }
  }
}
