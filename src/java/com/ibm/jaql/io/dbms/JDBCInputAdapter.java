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
package com.ibm.jaql.io.dbms;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.sql.SQLException;

import com.ibm.jaql.io.AbstractInputAdapter;
import com.ibm.jaql.io.ItemReader;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JBinary;
import com.ibm.jaql.json.type.JDate;
import com.ibm.jaql.json.type.JDecimal;
import com.ibm.jaql.json.type.JLong;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.type.JValue;
import com.ibm.jaql.json.type.MemoryJRecord;

/**
 * An input adapter that wraps a JDBC connection. Usage: read({location:
 * '[connection url]' inoptions: {adapter:
 * 'com.ibm.jaql.lang.JDBCInputAdapter', driver: '...', query: '...'}});
 */
public class JDBCInputAdapter extends AbstractInputAdapter
{
  private Connection conn;

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.DataStoreAdapter#open()
   */
  @Override
  public void open() throws Exception
  {

    String driver = ((JString) options.getValue("driver").getNonNull())
        .toString();
    String url = location;

    String s = driver.toString();
    try
    {
      Class.forName(s).newInstance();
    }
    catch (Exception e)
    {
      System.out.println("Error in finding jdbc driver: " + s);
      e.printStackTrace();
    }

    // Properties props = new Properties();
    // props.setProperty("user", "db2user");
    // props.setProperty("password", "db2user");
    // props.setProperty("CONNECTNODE", "0");

    conn = DriverManager.getConnection(url); // , props);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.DataStoreAdapter#close()
   */
  @Override
  public void close() throws Exception
  {
    conn.close();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.InputAdapter#getItemReader()
   */
  public ItemReader getItemReader() throws Exception
  {
    Statement stmt = conn.createStatement();

    String query = ((JString) options.getValue("query").getNonNull())
        .toString();
    final ResultSet rs = stmt.executeQuery(query);
    final ResultSetMetaData meta = rs.getMetaData();

    final int ncols = meta.getColumnCount();
    final MemoryJRecord rec = new MemoryJRecord(ncols);
    final JValue[] writables = new JValue[ncols];
    for (int i = 0; i < ncols; i++)
    {
      switch (meta.getColumnType(i + 1))
      {
        case Types.BIGINT :
        case Types.INTEGER :
        case Types.TINYINT :
        case Types.SMALLINT :
          writables[i] = new JLong();
          break;
        case Types.DECIMAL :
        case Types.DOUBLE :
        case Types.FLOAT :
          writables[i] = new JDecimal();
          break;
        case Types.CHAR :
        case Types.VARCHAR :
        case Types.OTHER : // TODO: Types.XML, when jdbc gets there...
          writables[i] = new JString();
          break;
        case Types.DATE :
        case Types.TIME :
        case Types.TIMESTAMP :
          writables[i] = new JDate();
          break;
        case Types.BINARY :
          writables[i] = new JBinary();
          break;
        default :
          throw new RuntimeException("Unsupported column type: "
              + meta.getColumnTypeName(i + 1));
      }
      String name = meta.getColumnName(i + 1);
      boolean convert = true;
      for (int j = 0; j < name.length(); j++)
      {
        if (Character.isLowerCase(name.charAt(j)))
        {
          convert = false;
        }
      }
      if (convert)
      {
        name = name.toLowerCase();
      }
      rec.add(name, new Item(writables[i]));
    }

    return new ItemReader() {
      /*
       * (non-Javadoc)
       * 
       * @see com.ibm.jaql.io.ItemReader#next(com.ibm.jaql.json.type.Item)
       */
      @Override
      public boolean next(Item value) throws IOException
      {
        {
          try
          {
            if (!rs.next())
            {
              rs.close();
              return false;
            }

            for (int i = 0; i < ncols; i++)
            {
              switch (meta.getColumnType(i + 1))
              {
                case Types.BIGINT :
                case Types.INTEGER :
                case Types.TINYINT :
                case Types.SMALLINT :
                  ((JLong) writables[i]).value = rs.getLong(i + 1);
                  break;
                case Types.DECIMAL :
                case Types.DOUBLE :
                case Types.FLOAT :
                  ((JDecimal) writables[i]).value = rs.getBigDecimal(i + 1);
                  break;
                case Types.CHAR :
                case Types.VARCHAR :
                  String s = rs.getString(i + 1);
                  if (s != null)
                  {
                    ((JString) writables[i]).set(s);
                  }
                  break;
                case Types.DATE :
                  // TODO: all these need null handling...
                  ((JDate) writables[i]).millis = rs.getDate(i + 1).getTime();
                  break;
                case Types.TIME :
                  ((JDate) writables[i]).millis = rs.getTime(i + 1).getTime();
                  break;
                case Types.TIMESTAMP :
                  ((JDate) writables[i]).millis = rs.getTimestamp(i + 1)
                      .getTime();
                  break;
                case Types.BINARY :
                  ((JBinary) writables[i]).setBytes(rs.getBytes(i + 1));
                  break;
                default :
                  throw new RuntimeException("Unsupported column type: "
                      + meta.getColumnTypeName(i + 1));
              } // end switch

              rec.getValue(i).set(rs.wasNull() ? null : writables[i]);

            } // end for each column
            value.set(rec);
            return true;
          }
          catch (SQLException se)
          {
            throw new RuntimeException(se);
          }
        }
      }
    };
  }
}
