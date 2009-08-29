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
import java.util.Map;

import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonBinary;
import com.ibm.jaql.json.type.MutableJsonDate;
import com.ibm.jaql.json.type.MutableJsonDecimal;
import com.ibm.jaql.json.type.MutableJsonLong;
import com.ibm.jaql.json.type.MutableJsonString;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ExprProperty;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.lang.util.JaqlUtil;

/**
 * 
 */
public class JdbcExpr extends IterExpr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("jdbc", JdbcExpr.class);
    }
  }
  
  /**
   * @param exprs
   */
  public JdbcExpr(Expr[] exprs)
  {
    super(exprs);
  }

  public Map<ExprProperty, Boolean> getProperties()
  {
    Map<ExprProperty, Boolean> result = super.getProperties();
    result.put(ExprProperty.READS_EXTERNAL_DATA, true);
    return result;
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  public JsonIterator iter(final Context context) throws Exception
  {
    JsonRecord args = JaqlUtil.enforceNonNull((JsonRecord) exprs[0].eval(context));
    String driver = (JaqlUtil.enforceNonNull((JsonString) args.get(new JsonString("driver")))).toString();
    String url = (JaqlUtil.enforceNonNull((JsonString) args.get(new JsonString("url")))).toString();
    String query = (JaqlUtil.enforceNonNull((JsonString) args.get(new JsonString("query")))).toString();

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
    final BufferedJsonRecord rec = new BufferedJsonRecord(ncols);
    final JsonString[] names = new JsonString[ncols];
    final JsonValue[] values = new JsonValue[ncols];
    for (int i = 0; i < ncols; i++)
    {
      switch (meta.getColumnType(i + 1))
      {
        case Types.BIGINT :
        case Types.INTEGER :
        case Types.TINYINT :
        case Types.SMALLINT :
          values[i] = new MutableJsonLong();
          break;
        case Types.DECIMAL :
        case Types.DOUBLE :
        case Types.FLOAT :
          values[i] = new MutableJsonDecimal();
          break;
        case Types.CHAR :
        case Types.VARCHAR :
        case Types.OTHER : // TODO: Types.XML, when jdbc gets there...
          values[i] = new MutableJsonString();
          break;
        case Types.DATE :
        case Types.TIME :
        case Types.TIMESTAMP :
          values[i] = new MutableJsonDate();
          break;
        case Types.BINARY :
          values[i] = new MutableJsonBinary();
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
      names[i] = new JsonString(name);
      rec.add(names[i], values[i]);
    }

    return new JsonIterator(rec) {
      public boolean moveNext() throws Exception
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
              ((MutableJsonLong) values[i]).set(rs.getLong(i + 1));
              break;
            case Types.DECIMAL :
            case Types.DOUBLE :
            case Types.FLOAT :
              ((MutableJsonDecimal) values[i]).set(rs.getBigDecimal(i + 1));
              break;
            case Types.CHAR :
            case Types.VARCHAR :
              String s = rs.getString(i + 1);
              if (s != null)
              {
                ((MutableJsonString) values[i]).setCopy(s);
              }
              break;
            case Types.DATE :
              // TODO: all these need null handling...
              ((MutableJsonDate) values[i]).set(rs.getDate(i + 1).getTime());
              break;
            case Types.TIME :
              ((MutableJsonDate) values[i]).set(rs.getTime(i + 1).getTime());
              break;
            case Types.TIMESTAMP :
              ((MutableJsonDate) values[i]).set(rs.getTimestamp(i + 1).getTime());
              break;
            case Types.BINARY :
              ((MutableJsonBinary) values[i]).set(rs.getBytes(i + 1));
              break;
            default :
              throw new RuntimeException("Unsupported column type: "
                  + meta.getColumnTypeName(i + 1));
          } // end switch

          rec.set(names[i], rs.wasNull() ? null : values[i]);

        } // end for each column

        return true; // currentValue == rec
      }
    }; // end Iter
  }
}
