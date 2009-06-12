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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.io.Text;

/**
 * 
 */
public class HBaseShell
{

  private HBaseConfiguration m_conf;

  /**
   * @throws Exception
   */
  public HBaseShell() throws Exception
  {
    m_conf = new HBaseConfiguration();
  }

  /**
   * @param cmd
   * @return
   * @throws Exception
   */
  public String doCommand(String cmd) throws Exception
  {
    cmd = cmd.trim();
    String s = null;
    if ("list".equals(cmd))
    {
      s = printTables();
    }
    else
    {
      // figure out which command it is
      int sep = cmd.indexOf(" ");
      if (sep <= 0) throw new Exception("Illegal command: " + cmd);
      String op = cmd.substring(0, sep).trim();
      String args = cmd.substring(sep + 1).trim();

      // send it to the appropriate method for parsing
      // System.out.println("found: " + op + "," + args + "," + sep);
      if ("create".equals(op))
      {
        s = createTable(args);
      }
      else if ("delete".equals(op))
      {
        s = deleteTable(args);
      }
      else if ("set".equals(op))
      {
        s = setRowValues(args);
      }
      else if ("fetch".equals(op))
      {
        s = printRow(args);
      }
      else if ("scan".equals(op))
      {
        s = printRows(args);
      }
      else
      {
        throw new Exception("Unsupported command: " + op + "\n" + CMD_USAGE);
      }
    }
    return s;
  }

  private static final String LIST_USG = "list";

  /**
   * @return
   * @throws Exception
   */
  public String printTables() throws Exception
  {
    ByteArrayOutputStream bstr = new ByteArrayOutputStream();
    PrintStream str = new PrintStream(bstr);

    HBaseAdmin admin = new HBaseAdmin(m_conf);
    HTableDescriptor[] tables = admin.listTables();
    str.println("Currently registered tables: ");
    for (int i = 0; i < tables.length; i++)
    {
      str.println(tables[i].getName());
    }
    if (tables.length == 0)
    {
      str.println("No registered tables");
    }
    str.flush();
    str.close();
    return bstr.toString();
  }

  private static final String CREATE_USG = "create <table name> (colFamily_1, ..., colFamily_n)";

  /**
   * @param args
   * @return
   * @throws Exception
   */
  private String createTable(String args) throws Exception
  {
    int begin = args.indexOf("(");
    if (begin <= 0)
      throw new Exception("Malformed create statement: " + CREATE_USG);
    int end = args.indexOf(")", begin);
    if (end <= 0)
      throw new Exception("Malformed create statement: " + CREATE_USG);

    String table = args.substring(0, begin).trim();
    String[] cols = args.substring(begin + 1, end).trim().split(",");
    return createTable(table, cols, 5);
  }

  /**
   * @param table
   * @param colFamilies
   * @param maxVersions
   * @return
   * @throws Exception
   */
  public String createTable(String table, String[] colFamilies, int maxVersions)
      throws Exception
  {
    HBaseAdmin admin = new HBaseAdmin(m_conf);
    HTableDescriptor desc = new HTableDescriptor(table);
    for (int i = 0; i < colFamilies.length; i++)
      desc.addFamily(new HColumnDescriptor(colFamilies[i].trim()));
    admin.createTable(desc);
    return table;
  }

  private static final String DROP_USG = "drop <table name>";

  /**
   * @param table
   * @return
   * @throws Exception
   */
  public String deleteTable(String table) throws Exception
  {
    HBaseAdmin admin = new HBaseAdmin(m_conf);
    admin.deleteTable(table);
    return table;
  }

  private static final String SET_USG = "set <table name>.key=<value> col_1=<value|>, ..., col_n=<value|>";

  /**
   * @param args
   * @return
   * @throws Exception
   */
  private String setRowValues(String args) throws Exception
  {
    // <table name>.key=<value> col_1=<value|>,...,col_n=<value|>
    String[] argv = parseRecordArgs(args);

    String[] cols = new String[argv.length - 2];
    String[] vals = new String[argv.length - 2];
    for (int i = 2; i < argv.length; i++)
    {
      String info = argv[i].trim();
      int idx = info.indexOf("=");
      if (idx <= 0)
        throw new Exception("Malformed column set clause: " + SET_USG);
      cols[i - 2] = info.substring(0, idx).trim();
      if (idx == info.length() - 1)
        vals[i - 2] = null;
      else
        vals[i - 2] = info.substring(idx + 1).trim();
    }
    return setRowValues(argv[0], argv[1], cols, vals);
  }

  /**
   * @param args
   * @return
   * @throws Exception
   */
  private String[] parseRecordArgs(String args) throws Exception
  {
    // <table>.key=<value> <col_1>, ..., <col_n>
    int dotIdx = args.indexOf(".");
    if (dotIdx <= 0) throw new Exception("Malformed record statement");
    String tableName = args.substring(0, dotIdx).trim();

    int kvIdx = args.indexOf("key=", dotIdx);
    if (kvIdx <= 0) throw new Exception("Malformed record statement");
    int colIdx = args.indexOf(" ", dotIdx);
    String keyValue = null;
    String[] colInfo = new String[]{};
    if (colIdx <= 0)
    {
      keyValue = args.substring(kvIdx + "key=".length()).trim();
    }
    else
    {
      keyValue = args.substring(kvIdx + "key=".length(), colIdx).trim();
      colInfo = args.substring(colIdx).trim().split(",");
    }

    String[] argv = new String[2 + colInfo.length];
    argv[0] = tableName;
    argv[1] = keyValue;
    for (int i = 0; i < colInfo.length; i++)
      argv[i + 2] = colInfo[i];

    return argv;
  }

  /**
   * @param table
   * @param row
   * @param cols
   * @param vals
   * @return
   * @throws Exception
   */
  public String setRowValues(String table, String row, String[] cols,
      String[] vals) throws Exception
  {
    if (cols.length != vals.length) throw new Exception("Illegal cols/vals");

    HTable m_table = new HTable((HBaseConfiguration) m_conf, table);
    BatchUpdate xact = new BatchUpdate(row);
    for (int i = 0; i < cols.length; i++)
    {
      xact.put(cols[i], vals[i].getBytes());
    }
    m_table.commit(xact);

    return table + "->" + row;
  }

  private static final String DELETE_USG = "delete <table name>.key=<value> col_1,...,col_n";

  /**
   * @param args
   * @return
   * @throws Exception
   */
  public String deleteRowVals(String args) throws Exception
  {
    String[] argv = null;
    try
    {
      argv = parseRecordArgs(args);
    }
    catch (Exception e)
    {
      throw new Exception(FETCH_USG, e);
    }

    HTable m_table = new HTable((HBaseConfiguration) m_conf, argv[0]);
    BatchUpdate xact = new BatchUpdate(argv[1]);
    String[] cols = new String[argv.length - 2];
    for (int i = 0; i < cols.length; i++)
      cols[i] = argv[i + 2].trim();
    for (int i = 0; i < cols.length; i++)
    {
      xact.delete(cols[i]);
    }
    m_table.commit(xact);

    return argv[1];
  }

  private final static String FETCH_USG = "fetch <table name>.key=<value> col_1,...,col_n";

  /**
   * @param args
   * @return
   * @throws Exception
   */
  private String printRow(String args) throws Exception
  {
    String[] argv = null;
    try
    {
      argv = parseRecordArgs(args);
    }
    catch (Exception e)
    {
      throw new Exception(FETCH_USG, e);
    }

    StringBuilder sb = new StringBuilder();
    if (argv.length == 2)
    {
      sb.append(printRow(argv[0], argv[1]));
    }
    else
    {
      String[] cols = new String[argv.length - 2];
      for (int i = 0; i < cols.length; i++)
        cols[i] = argv[i + 2].trim();
      sb.append(printRowColumns(argv[0], argv[1], cols));
    }
    return sb.toString();
  }

  /**
   * @param table
   * @param key
   * @return
   * @throws Exception
   */
  private String printRow(String table, String key) throws Exception
  {
    ByteArrayOutputStream bstr = new ByteArrayOutputStream();
    PrintStream str = new PrintStream(bstr);

    HTable m_table = new HTable((HBaseConfiguration) m_conf, table);
    RowResult row = m_table.getRow(key);
    str.print(key + "=> ");
    for (Iterator<byte[]> iter = row.keySet().iterator(); iter.hasNext();)
    {
      byte[] label = iter.next();
      str.print("[" + label + "," + new String(row.get(label).getValue()).trim() + "]");
    }
    str.println();

    str.flush();
    str.close();
    return bstr.toString();
  }

  /**
   * @param table
   * @param key
   * @param cols
   * @return
   * @throws Exception
   */
  public String printRowColumns(String table, String key, String[] cols)
      throws Exception
  {
    ByteArrayOutputStream bstr = new ByteArrayOutputStream();
    PrintStream str = new PrintStream(bstr);

    HTable m_table = new HTable((HBaseConfiguration) m_conf, table);
    str.print(key + "=> ");
    for (int i = 0; i < cols.length; i++)
    {
      byte[] val = m_table.get(key, cols[i]).getValue();
      if (val != null)
        str.print("[" + cols[i] + "," + new String(val).trim() + "]");
    }
    str.println();
    str.flush();
    str.close();
    return bstr.toString();
  }

  private static final String SCAN_USG = "scan <table name> col_1, ..., col_n";

  /**
   * @param args
   * @return
   * @throws Exception
   */
  private String printRows(String args) throws Exception
  {
    int sepIdx = args.indexOf(" ");
    if (sepIdx <= 0)
      throw new Exception("Malformed scan statement: " + SCAN_USG);
    String tableName = args.substring(0, sepIdx).trim();
    String[] cols = args.substring(sepIdx + 1).trim().split(",");

    return printRows(tableName, null, cols);
  }

  /**
   * @param table
   * @param keyStart
   * @param cols
   * @return
   * @throws Exception
   */
  public String printRows(String table, String keyStart, String[] cols)
      throws Exception
  {
    HTable m_table = new HTable((HBaseConfiguration) m_conf, table);

    ByteArrayOutputStream bstr = new ByteArrayOutputStream();
    PrintStream str = new PrintStream(bstr);
    Scanner scanner = m_table.getScanner(cols);
    HStoreKey key = new HStoreKey();
    RowResult current = new RowResult();
    while ( (current = scanner.next()) != null)
    {
      str.print(key.getRow() + "=>");
      for (Iterator<byte[]> iter = current.keySet().iterator(); iter.hasNext();)
      {
        byte[] vk = iter.next();
        str.print("[" + vk + "," + new String(current.get(vk).getValue()).trim() + "]");
      }
      str.println();
      current.clear();
    }
    scanner.close();
    str.flush();
    str.close();
    return bstr.toString();
  }

  static String CMD_USAGE = LIST_USG + "\n\t\t" + CREATE_USG + "\n\t\t"
                              + DROP_USG + "\n\t\t" + SET_USG + "\n\t\t"
                              + DELETE_USG + "\n\t\t" + FETCH_USG + "\n\t\t"
                              + SCAN_USG;
}
