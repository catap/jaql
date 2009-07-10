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
import java.util.Map.Entry;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.io.Text;

import com.ibm.jaql.io.ClosableJsonIterator;
import com.ibm.jaql.io.hadoop.JsonHolder;
import com.ibm.jaql.io.serialization.binary.BinaryFullSerializer;
import com.ibm.jaql.io.serialization.binary.def.DefaultBinaryFullSerializer;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.SpilledJsonArray;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.util.JaqlUtil;

/**
 * 
 */
public class HBaseStore
{
  public static final BinaryFullSerializer SERIALIZER = DefaultBinaryFullSerializer.getDefault();
  
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

    public final static JsonString               J_DEFAULT_HBASE_COLUMN_FAMILY_NAME = new JsonString(
                                                                                     DEFAULT_HBASE_COLUMN_FAMILY_NAME);

    public final static String                KEY_NAME                           = "key";

    public final static JsonString               J_KEY                              = new JsonString(
                                                                                     KEY_NAME);

    public final static HBaseConfiguration    hbaseConf                          = new HBaseConfiguration();

    protected static HashMap<JsonString, HTable> htableMap                          = new HashMap<JsonString, HTable>();

    /**
     * @param s
     * @return
     */
    public static Text makeText(JsonString s)
    {
      Text t = new Text();
      t.set(s.getInternalBytes(), 0, s.lengthUtf8());
      return t;
    }

    /**
     * @param tableName
     * @return
     * @throws IOException
     */
    public static HTable openHTable(JsonString tableName) throws IOException
    {
      HTable htable = htableMap.get(tableName);
      if (htable == null)
      {
        // TODO: mismatch between Text and JString
        htable = new HTable(hbaseConf, tableName.toString());
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
    public static byte[] convertToBytes(JsonValue value) throws IOException
    {
      ByteArrayOutputStream bstr = new ByteArrayOutputStream();
      DataOutputStream str = new DataOutputStream(bstr);
      SERIALIZER.write(str, value);
      str.close();
      return bstr.toByteArray();
    }

    /**
     * @param val
     * @return
     * @throws IOException
     */
    public static JsonValue convertFromBytes(byte[] val) throws IOException
    {
      // TODO: memory
      DataInputStream str = new DataInputStream(new ByteArrayInputStream(val));
      return SERIALIZER.read(str, null);
    }

    /**
     * @param colName
     * @param value
     * @param rec
     * @throws IOException
     */
    public static void setMap(JsonString colName, JsonValue value, BufferedJsonRecord rec)
        throws IOException
    {
      // unpeel the column family only if its the default column family
      if (colName.startsWith(J_DEFAULT_HBASE_COLUMN_FAMILY_NAME))
      {
        colName.removeBytes(0, J_DEFAULT_HBASE_COLUMN_FAMILY_NAME.lengthUtf8());
      }
      else
      {
        colName.replace(HBASE_CF_SEPARATOR_CHAR, JAQL_CF_SEPARATOR_CHAR);
      }
      int idx = rec.indexOf(colName);
      if (idx < 0)
        rec.add(colName, value);
      else
        rec.set(idx, value);
    }

    /**
     * @param colName
     * @param colVal
     * @param item
     * @param rec
     * @throws IOException
     */
    public static void convertFromBytes(byte[] colVal,
        JsonHolder valueHolder, BufferedJsonRecord rec) throws IOException
    {
      DataInputStream str = new DataInputStream(
          new ByteArrayInputStream(colVal)); // TODO: memory
      valueHolder.value = SERIALIZER.read(str, valueHolder.value);
    }

    /**
     * convert from jaql to hbase
     * 
     * @param col
     * @return
     * @throws Exception
     */
    public static JsonString convertColumn(JsonString col) throws Exception
    {
      JsonString ncol = null;
      // add the default column family if one is not specified
      if (col.indexOf(HBASE_CF_SEPARATOR_CHAR) < 0)
      {
        ncol = new JsonString(DEFAULT_HBASE_COLUMN_FAMILY_NAME + col);
      }
      else
      {
        // TODO: memory
        String o = col.toString();
        String n = o.replace(JAQL_CF_SEPARATOR_CHAR, HBASE_CF_SEPARATOR_CHAR);
        ncol = new JsonString(n);
      }
      return ncol;
    }

    /**
     * @param columns
     * @return
     * @throws Exception
     */
    public static JsonString[] convertColumns(JsonArray columns) throws Exception
    {
      JsonString[] cols = null;
      if (columns != null)
      {
        int ncols = (int) columns.count();
        cols = new JsonString[ncols];
        JsonIterator colIter = columns.iter();
        for (int i = 0; i < ncols; i++)
        {
          if (!colIter.moveNext()) 
          {
            throw new IllegalStateException();
          }
          JsonString col = JaqlUtil.enforceNonNull((JsonString) colIter.current());
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
    public static void convertMap(JsonString key, RowResult row,
        BufferedJsonRecord rec) throws IOException
    {
      rec.clear();
      rec.ensureCapacity(row.size() + 1);
      JsonString name = new JsonString(J_KEY);
      JsonString value = new JsonString(key.getInternalBytes(), key.lengthUtf8());
      rec.add(name, value);
      JsonHolder valueHolder = new JsonHolder();
      for (Map.Entry<byte[], Cell> e : row.entrySet())
      {
        String n = new String(e.getKey());
        if (!n.equals(KEY_NAME))
        {
          convertFromBytes(e.getValue().getValue(), valueHolder, rec);
          JsonString newName = new JsonString(n.getBytes(), n.length());
          setMap(newName, valueHolder.value, rec);
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
    public static void writeJMapToHBase(JsonValue key, JsonRecord rec, HTable table,
        boolean extractKey) throws IOException
    {
      if (key == null && !extractKey)
      {
        throw new RuntimeException("key must be specified or extracted");
      }

      // convert key to hbaseKey
      if (extractKey)
      {
        key = rec.get(J_KEY, null);
      }
      JsonString hbaseKey = JaqlUtil.enforceNonNull((JsonString) key);
      
      // start transaction

      // TODO: mismatch between Text and JString
      BatchUpdate xact = new BatchUpdate(hbaseKey.toString());
      for (Entry<JsonString, JsonValue> e : rec)
      {
        JsonString columnName = e.getKey();
        if (columnName.equals(J_KEY)) continue; // skip the key
        // specify the default column family only when no column family is
        // specified
        if (columnName.indexOf(JAQL_CF_SEPARATOR_CHAR) < 0)
        {
          columnName = new JsonString(DEFAULT_HBASE_COLUMN_FAMILY_NAME
              + columnName);
        }
        else
        {
          // TODO: memory
          columnName = new JsonString(columnName);
          columnName.replace(JAQL_CF_SEPARATOR_CHAR, HBASE_CF_SEPARATOR_CHAR);
        }
        JsonValue val = e.getValue();
        byte[] valueBytes = convertToBytes(val);
        xact.put(columnName.toString(), valueBytes);
      }

      // end transaction

      table.commit(xact);
    }

    /**
     * 
     * @param table
     * @param ey
     * @param columns
     *            (HBase formatted)
     * @throws IOException
     */
    public static void deleteFromHBase(HTable table, JsonString key,
        JsonString[] columns) throws Exception
    {
      // TODO: mismatch between Text and JString
      // start the transaction
      BatchUpdate xact = new BatchUpdate(key.toString());

      try
      {
        if (columns == null)
        {
          // TODO: hbase needs a delete all columns method.
          RowResult row = table.getRow(key.toString());
          columns = row.keySet().toArray(columns);
        }
        for (JsonString col : columns)
        {
          // int start = col.find(HBASE_CF_SEPARATOR_CHAR) + 1;
          xact.delete(col.getInternalBytes());
        }
        // end the transaction
        table.commit(xact);
      }
      catch (Exception e)
      {
        // FIXME: is there an 'abort' in the current API?
        // abort the transaction
        // table.abort(xact);
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
    public static ClosableJsonIterator createResult(HTable table, JsonString startKey,
        final JsonString stopKey, JsonString[] columnNames, long timestamp,
        final BufferedJsonRecord current) throws IOException
    {
      String[] hbaseColumnNames = new String[columnNames.length];
      for (int i = 0; i < columnNames.length; i++)
      {
        hbaseColumnNames[i] = columnNames[i].toString();
      }
      return createResultBase(table, startKey, stopKey,
          columnNames, timestamp, current);
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
    public static ClosableJsonIterator createResultBase(HTable table, JsonString startKey,
        final JsonString stopKey, JsonString[] columnNames, long timestamp,
        final BufferedJsonRecord current) throws IOException
    {
      Scanner tmpScanner = null;
      String[] cnames = new String[columnNames.length];
      for(int i = 0; i < columnNames.length; i++) cnames[i] = columnNames[i].toString();
      
      if (timestamp >= 0)
        tmpScanner = table.getScanner(cnames, startKey.toString(), stopKey.toString(), timestamp);
      else
        tmpScanner = table.getScanner(cnames, startKey.toString());
      final Scanner scanner = tmpScanner;

      return new ClosableJsonIterator(current) {

        RowResult               row    = new RowResult();

        public boolean moveNext() throws Exception
        {
          if ( (row = scanner.next()) != null)
          {
            String key = new String(row.getRow());
            if (stopKey == null || stopKey.lengthUtf8() == 0
                || key.compareTo(stopKey.toString()) <= 0)
            {
              HBaseStore.Util.convertMap(new JsonString(key), row, current);
              return true; // currentValue == current
            }
          }
          return false;
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
    public static void deleteValues(JsonString tableName, JsonArray columnNames,
        JsonIterator rows) throws Exception
    {
      // setup the columns
      JsonString[] cols = convertColumns(columnNames);

      // open the table
      HTable table = openHTable(tableName);

      for (JsonValue value : rows)
      {
        JsonString key = JaqlUtil.enforceNonNull((JsonString) value);
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
    public static JsonIterator fetchRecords(JsonString tableName, JsonArray columnNames,
        JsonLong timestampValue, JsonLong numVersionsValue, JsonIterator rows)
        throws Exception
    {
      // open the table
      HTable table = HBaseStore.Util.openHTable(tableName);

      // setup the columns
      JsonString[] cols = HBaseStore.Util.convertColumns(columnNames);

      // setup the timestamp
      long timestamp = (timestampValue != null) ? timestampValue.get() : -1;

      // setup the number of versions
      int numVersions = (numVersionsValue != null)
          ? (int) numVersionsValue.get()
          : -1;

      JsonIterator result = null;

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
  static abstract class FetchIter extends JsonIterator
  {
    protected HTable        table;

    protected JsonIterator          keyIter;

    protected BufferedJsonRecord rec;

    /**
     * @param table
     * @param keyIter
     * @param numCols
     * @throws Exception
     */
    public FetchIter(HTable table, JsonIterator keyIter, int numCols) throws Exception
    {
      this.table = table;
      this.keyIter = keyIter;
      this.rec = new BufferedJsonRecord(numCols);
      this.currentValue = rec;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.ibm.jaql.json.util.Iter#next()
     */
    public boolean moveNext() throws Exception
    {
      while (true)
      {
        if (!keyIter.moveNext())
        {
          HBaseStore.Util.closeHTable(table);
          return false;
        }

        JsonString hbaseKey = (JsonString) keyIter.current();
        if (hbaseKey != null)
        {
          if (fetchRecord(hbaseKey))
          {
            rec.set(HBaseStore.Util.J_KEY, hbaseKey);
            return true; // currentValue == rec
          }
        }
      }
    }

    /**
     * @param key
     * @return
     * @throws Exception
     */
    protected abstract boolean fetchRecord(JsonString key) throws Exception;
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
    public FetchAllColumnsIter(HTable table, JsonIterator keyIter) throws Exception
    {
      super(table, keyIter, 10);
      rec = new BufferedJsonRecord(10);
      currentValue = rec;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.ibm.jaql.io.hbase.HBaseStore.FetchIter#fetchRecord(com.ibm.jaql.json.type.JString)
     */
    protected boolean fetchRecord(JsonString key) throws Exception
    {
      RowResult row = table.getRow(key.toString());
      if (row.size() > 0)
      {
        rec.clear();
        HBaseStore.Util.convertMap(key, row, rec);
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
    protected JsonString[] jcols;

    protected Text[]    tcols;
    private JsonHolder valueHolder = new JsonHolder();
    
    /**
     * @param table
     * @param keyIter
     * @param cols
     * @throws Exception
     */
    public FetchSingleVersionIter(HTable table, JsonIterator keyIter, JsonString[] cols)
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
    protected boolean fetchRecord(JsonString key) throws Exception
    {
//      Text tkey = HBaseStore.Util.makeText(key);
      boolean hasValue = false;
      for (int i = 0; i < jcols.length; i++)
      {
        Cell cell = table.get(key.getInternalBytes(), tcols[i].getBytes());

        if (cell != null)
        {
          byte[] value = cell.getValue();
          int idx = rec.indexOf(jcols[i]);
          if (idx < 0)
            valueHolder.value = null;
          else
            valueHolder.value = rec.valueOf(idx);
          HBaseStore.Util.convertFromBytes(value, valueHolder, rec);
          HBaseStore.Util.setMap(jcols[i], valueHolder.value, rec);
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
    public FetchMultiVersionIter(HTable table, JsonIterator keyIter, JsonString[] cols,
        long timestamp, int numVersions) throws Exception
    {
      super(table, keyIter, cols);
      this.timestamp = timestamp;
      this.numVersions = numVersions;
      for (int i = 0; i < cols.length; i++)
      {
        rec.set(i + 1, new SpilledJsonArray());
      }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.ibm.jaql.io.hbase.HBaseStore.FetchSingleVersionIter#fetchRecord(com.ibm.jaql.json.type.JString)
     */
    protected boolean fetchRecord(JsonString key) throws Exception
    {
      boolean hasValue = false;
      for (int i = 0; i < jcols.length; i++)
      {
        Cell[] value = null;
        if (timestamp < 0)
          value = table.get(key.getInternalBytes(), tcols[i].getBytes(), numVersions);
        else
          value = table.get(key.getInternalBytes(), tcols[i].getBytes(), timestamp, numVersions);

        if (value != null && value.length > 0)
        {
          SpilledJsonArray tArr = JaqlUtil.enforceNonNull((SpilledJsonArray) rec.valueOf(i + 1));
          tArr.clear();

          for (int j = 0; j < value.length; j++)
          {
            tArr.addCopySerialized(value[j].getValue(), 0, value[j].getValue().length, SERIALIZER);
          }
          tArr.freeze();

          // set the top-level map
          HBaseStore.Util.setMap(jcols[i], tArr, rec);
          hasValue = true;
        }
      }
      return hasValue;
    }
  }
}
