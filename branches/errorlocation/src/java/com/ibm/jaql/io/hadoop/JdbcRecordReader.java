/*
 * Copyright (C) IBM Corp. 2009.
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
package com.ibm.jaql.io.hadoop;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

import org.apache.hadoop.mapred.RecordReader;

import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonBinary;
import com.ibm.jaql.json.type.MutableJsonDate;
import com.ibm.jaql.json.type.MutableJsonDecimal;
import com.ibm.jaql.json.type.MutableJsonDouble;
import com.ibm.jaql.json.type.MutableJsonLong;
import com.ibm.jaql.json.type.MutableJsonString;


public class JdbcRecordReader implements RecordReader<JsonHolder, JsonHolder>
{
  protected final Connection conn;
  protected final PreparedStatement stmt;
  protected final ResultSet resultSet;
  protected final ResultSetMetaData meta;
  protected final int ncols;
  protected final JsonString[] names;
  protected final JsonValue[] values;
  protected long numRecs;
  
  public JdbcRecordReader(Connection conn, PreparedStatement stmt) throws SQLException
  {
    this.conn = conn;
    this.stmt = stmt;
    resultSet = stmt.executeQuery();
    meta = resultSet.getMetaData();
    ncols = meta.getColumnCount();
    names = new JsonString[ncols];
    values = new JsonValue[ncols];
    
    for(int i = 0 ; i < ncols ; i++)
    {
      names[i] = new JsonString(meta.getColumnName(i+1).toLowerCase());

      switch( meta.getColumnType(i+1) )
      {
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.OTHER:   // TODO: Types.XML, when jdbc gets there...
          values[i] = new MutableJsonString();
          break;
        case Types.BIGINT:
        case Types.INTEGER:
        case Types.TINYINT:
        case Types.SMALLINT:
          values[i] = new MutableJsonLong();
          break;
        case Types.DOUBLE: 
        case Types.FLOAT: 
          values[i] = new MutableJsonDouble();
          break;
        case Types.DECIMAL: 
          values[i] = new MutableJsonDecimal();
          break;
        case Types.DATE:
        case Types.TIME:
        case Types.TIMESTAMP:
          values[i] = new MutableJsonDate();
          break;
        case Types.BINARY:          
          values[i] = new MutableJsonBinary();
          break;
        default:
          throw new RuntimeException("Unsupported column type: " + meta.getColumnTypeName(i+1));
      }
    }
  }
  
  public void close() throws IOException
  {
    try
    {
      resultSet.close();
      stmt.close();
      conn.close();
    }
    catch( SQLException e )
    {
      throw new UndeclaredThrowableException(e); // IOException(e);
    }
  }

  public JsonHolder createKey()
  {
    return null;
  }

  /**
   * Warning: For efficiency, this class does not support more than one value to be created (no concurrent readers) 
   */
  public JsonHolder createValue()
  {
    return new JsonHolder(new BufferedJsonRecord(ncols));
  }

  public long getPos() throws IOException
  {
    return numRecs;
  }

  public float getProgress() throws IOException
  {
    return (float)(numRecs / 1000000.0); // hack
  }

  public boolean next(JsonHolder key, JsonHolder value) throws IOException
  {
    try
    {
      if( ! resultSet.next() )
      {
        return false;
      }

      BufferedJsonRecord jrec = (BufferedJsonRecord)value.value;
      jrec.clear();

      for(int i = 0 ; i < ncols ; i++)
      {
        switch( meta.getColumnType(i+1) )
        {
          case Types.CHAR:
          case Types.VARCHAR:
            String s = resultSet.getString(i+1);
            if( s != null )
            {
              ((MutableJsonString)values[i]).setCopy(s);
            }
            break;
          case Types.BIGINT:
          case Types.INTEGER:
          case Types.TINYINT:
          case Types.SMALLINT:
            ((MutableJsonLong)values[i]).set(resultSet.getLong(i+1));
            break;
          case Types.DOUBLE: 
          case Types.FLOAT: 
            ((MutableJsonDouble)values[i]).set(resultSet.getDouble(i+1));
            break;
          case Types.DECIMAL: 
            ((MutableJsonDecimal)values[i]).set(resultSet.getBigDecimal(i+1));
            break;
          case Types.DATE:
            // TODO: all these need null handling...
            Date d = resultSet.getDate(i+1);
            if( d != null )
            {
              ((MutableJsonDate)values[i]).set(d.getTime());
            }
            break;
          case Types.TIME:
            Time t = resultSet.getTime(i+1);
            if( t != null )
            {
              ((MutableJsonDate)values[i]).set(t.getTime());
            }
            break;
          case Types.TIMESTAMP:
            Timestamp ts = resultSet.getTimestamp(i+1);
            if( ts != null )
            {
              ((MutableJsonDate)values[i]).set(ts.getTime());
            }
            break;
          case Types.BINARY:
            ((MutableJsonBinary)values[i]).set(resultSet.getBytes(i+1));
            break;
          default:
            throw new RuntimeException("Unsupported column type: " + meta.getColumnTypeName(i+1));
        } // end switch

        if( ! resultSet.wasNull() )
        {
          jrec.add(names[i], values[i]);
        }
      }

      return true;
    }
    catch( SQLException e )
    {
      throw new UndeclaredThrowableException(e); // IOException(e);
    }
  }
}
