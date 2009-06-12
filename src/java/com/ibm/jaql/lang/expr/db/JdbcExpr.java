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
package com.ibm.jaql.lang.expr.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JBinary;
import com.ibm.jaql.json.type.JDate;
import com.ibm.jaql.json.type.JDecimal;
import com.ibm.jaql.json.type.JLong;
import com.ibm.jaql.json.type.JRecord;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.type.JValue;
import com.ibm.jaql.json.type.MemoryJRecord;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/**
 * 
 */
@JaqlFn(fnName = "jdbc", minArgs = 1, maxArgs = 1)
public class JdbcExpr extends IterExpr
{
  /**
   * @param exprs
   */
  public JdbcExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#isConst()
   */
  @Override
  public boolean isConst()
  {
    return false;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  public Iter iter(final Context context) throws Exception
  {
    JRecord args = (JRecord) exprs[0].eval(context).getNonNull();
    String driver = ((JString) args.getValue("driver").getNonNull()).toString();
    String url = ((JString) args.getValue("url").getNonNull()).toString();
    String query = ((JString) args.getValue("query").getNonNull()).toString();

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

    Connection conn = DriverManager.getConnection(url); // , props);
    Statement stmt = conn.createStatement();

    final ResultSet rs = stmt.executeQuery(query);
    final ResultSetMetaData meta = rs.getMetaData();

    final int ncols = meta.getColumnCount();
    final MemoryJRecord rec = new MemoryJRecord(ncols);
    final Item recItem = new Item(rec);
    final JValue[] values = new JValue[ncols];
    for (int i = 0; i < ncols; i++)
    {
      switch (meta.getColumnType(i + 1))
      {
        case Types.BIGINT :
        case Types.INTEGER :
        case Types.TINYINT :
        case Types.SMALLINT :
          values[i] = new JLong();
          break;
        case Types.DECIMAL :
        case Types.DOUBLE :
        case Types.FLOAT :
          values[i] = new JDecimal();
          break;
        case Types.CHAR :
        case Types.VARCHAR :
        case Types.OTHER : // TODO: Types.XML, when jdbc gets there...
          values[i] = new JString();
          break;
        case Types.DATE :
        case Types.TIME :
        case Types.TIMESTAMP :
          values[i] = new JDate();
          break;
        case Types.BINARY :
          values[i] = new JBinary();
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
      rec.add(name, new Item(values[i]));
    }

    return new Iter() {
      public Item next() throws Exception
      {
        if (!rs.next())
        {
          rs.close();
          return null;
        }

        for (int i = 0; i < ncols; i++)
        {
          switch (meta.getColumnType(i + 1))
          {
            case Types.BIGINT :
            case Types.INTEGER :
            case Types.TINYINT :
            case Types.SMALLINT :
              ((JLong) values[i]).value = rs.getLong(i + 1);
              break;
            case Types.DECIMAL :
            case Types.DOUBLE :
            case Types.FLOAT :
              ((JDecimal) values[i]).value = rs.getBigDecimal(i + 1);
              break;
            case Types.CHAR :
            case Types.VARCHAR :
              String s = rs.getString(i + 1);
              if (s != null)
              {
                ((JString) values[i]).set(s);
              }
              break;
            case Types.DATE :
              // TODO: all these need null handling...
              ((JDate) values[i]).millis = rs.getDate(i + 1).getTime();
              break;
            case Types.TIME :
              ((JDate) values[i]).millis = rs.getTime(i + 1).getTime();
              break;
            case Types.TIMESTAMP :
              ((JDate) values[i]).millis = rs.getTimestamp(i + 1).getTime();
              break;
            case Types.BINARY :
              ((JBinary) values[i]).setBytes(rs.getBytes(i + 1));
              break;
            default :
              throw new RuntimeException("Unsupported column type: "
                  + meta.getColumnTypeName(i + 1));
          } // end switch

          rec.getValue(i).set(rs.wasNull() ? null : values[i]);

        } // end for each column

        return recItem;
      }
    }; // end Iter
  }
}
