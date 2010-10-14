/*
 * Copyright (C) IBM Corp. 2010.
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
package com.ibm.jaql.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

import com.ibm.jaql.json.schema.RecordSchema;
import com.ibm.jaql.json.type.JsonBinary;
import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonDate;
import com.ibm.jaql.json.type.JsonDecimal;
import com.ibm.jaql.json.type.JsonDouble;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.util.BaseUtil;


public class JaqlJdbcPreparedStatement extends JaqlJdbcStatement implements PreparedStatement
{
  public JaqlJdbcPreparedStatement(JaqlJdbcConnection conn, String script) throws SQLException
  {
    super(conn);
    execute(script);
  }

  @Override
  public void addBatch() throws SQLException
  {
    throw new SQLFeatureNotSupportedException("batch not yet supported");
  }

  @Override
  public void clearParameters() throws SQLException
  {
    // cannot clear parameters
  }

  @Override
  public boolean execute() throws SQLException
  {
    try
    {
      parsedJaql.open();
      return parsedJaql.hasMoreResults();
    }
    catch( Exception e )
    {
      // 58033  An unexpected error occurred while attempting to access a client driver.
      Throwable c = BaseUtil.getRootCause(e);
      throw new SQLException("while opening script: ["+c.getClass().getSimpleName()+"]"+c.getMessage(),"58033",e);
    }
  }

  @Override
  public ResultSet executeQuery() throws SQLException
  {
    try
    {
      JsonIterator iter = parsedJaql.iter();
      RecordSchema recSchema = getRowSchema( parsedJaql.currentSchema() );
      return new JaqlJdbcResultSet(this, recSchema, iter);
    }
    catch( SQLException e )
    {
      throw e;
    }
    catch( Exception e )
    {
      // 58033  An unexpected error occurred while attempting to access a client driver.
      Throwable c = BaseUtil.getRootCause(e);
      throw new SQLException("while preparing statements: ["+c.getClass().getSimpleName()+"]"+c.getMessage(),"58033",e);
    }
  }

  @Override
  public int executeUpdate() throws SQLException
  {
    throw new SQLException("updates are not suppored -- use queries");
  }

  @Override
  public JaqlJdbcResultSetMetaData getMetaData() throws SQLException
  {
    try
    {
      RecordSchema schema = getRowSchema( parsedJaql.getNextSchema() );
      return new JaqlJdbcResultSetMetaData(schema);
    }
    catch( SQLException e )
    {
      throw e;
    }
    catch( Exception e )
    {
      // 58033  An unexpected error occurred while attempting to access a client driver.
      Throwable c = BaseUtil.getRootCause(e);
      throw new SQLException("while getting metadata: ["+c.getClass().getSimpleName()+"]"+c.getMessage(),"58033",e);
    }
  }

  @Override
  public JaqlJdbcParameterMetaData getParameterMetaData() throws SQLException
  {
    return new JaqlJdbcParameterMetaData(parsedJaql);
  }

  protected final void setVar(int index, JsonValue value)
  {
    // TODO: why cant we call var.set directly?
    Var var = parsedJaql.getExternalVariables().get(index-1);
    parsedJaql.setExternalVariable(var.name(), value);
  }
  
  @Override
  public void setArray(int parameterIndex, Array x) throws SQLException
  {
    throw new SQLException("setArray NYI");
//    Var var = getVar(parameterIndex);
//    SpilledJsonArray val = new SpilledJsonArray();
//    ResultSet rs = x.getResultSet();
//    while( rs.next() )
//    {
//      ???
//    }
//    JsonValue[] values = new JsonValue[]
//    var.setValue(new SpilledJsonArray(values, false));
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x)
      throws SQLException
  {
    throw new SQLException("setAsciiStream NYI");
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, int length)
      throws SQLException
  {
    throw new SQLException("setAsciiStream NYI");
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, long length)
      throws SQLException
  {
    throw new SQLException("setAsciiStream NYI");
  }

  @Override
  public void setBigDecimal(int parameterIndex, BigDecimal x)
      throws SQLException
  {
    setVar( parameterIndex, new JsonDecimal(x) );
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x)
      throws SQLException
  {
    throw new SQLException("setBinaryStream NYI");
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, int length)
      throws SQLException
  {
    throw new SQLException("setBinaryStream NYI");
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, long length)
      throws SQLException
  {
    throw new SQLException("setBinaryStream NYI");
  }

  @Override
  public void setBlob(int parameterIndex, Blob x) throws SQLException
  {
    throw new SQLException("setBlob NYI");
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream)
      throws SQLException
  {
    throw new SQLException("setBlob NYI");
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream, long length)
      throws SQLException
  {
    throw new SQLException("setBlob NYI");
  }

  @Override
  public void setBoolean(int parameterIndex, boolean x) throws SQLException
  {
    setVar( parameterIndex, JsonBool.make(x) );
  }

  @Override
  public void setByte(int parameterIndex, byte x) throws SQLException
  {
    setVar( parameterIndex, new JsonLong(x) );
  }

  @Override
  public void setBytes(int parameterIndex, byte[] x) throws SQLException
  {
    setVar( parameterIndex, new JsonBinary(x) );
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader)
      throws SQLException
  {
    throw new SQLException("setCharacterStream NYI");
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, int length)
      throws SQLException
  {
    throw new SQLException("setCharacterStream NYI");
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, long length)
      throws SQLException
  {
    throw new SQLException("setCharacterStream NYI");
  }

  @Override
  public void setClob(int parameterIndex, Clob x) throws SQLException
  {
    throw new SQLException("setClob NYI");
  }

  @Override
  public void setClob(int parameterIndex, Reader reader) throws SQLException
  {
    throw new SQLException("setClob NYI");
  }

  @Override
  public void setClob(int parameterIndex, Reader reader, long length)
      throws SQLException
  {
    throw new SQLException("setClob NYI");
  }

  @Override
  public void setDate(int parameterIndex, Date x) throws SQLException
  {
    // TODO: add timezone
    setVar( parameterIndex, new JsonDate(x.getTime()) );
  }

  @Override
  public void setDate(int parameterIndex, Date x, Calendar cal)
      throws SQLException
  {
    // TODO: add timezone
    cal.setTime(x);
    setVar( parameterIndex, new JsonDate(cal.getTimeInMillis()));
  }

  @Override
  public void setDouble(int parameterIndex, double x) throws SQLException
  {
    setVar( parameterIndex, new JsonDouble(x));
  }

  @Override
  public void setFloat(int parameterIndex, float x) throws SQLException
  {
    setVar( parameterIndex, new JsonDouble(x));
  }

  @Override
  public void setInt(int parameterIndex, int x) throws SQLException
  {
    setVar( parameterIndex, new JsonLong(x));
  }

  @Override
  public void setLong(int parameterIndex, long x) throws SQLException
  {
    setVar( parameterIndex, new JsonLong(x));
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value)
      throws SQLException
  {
    throw new SQLException("setNCharacterStream NYI");
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value, long length)
      throws SQLException
  {
    throw new SQLException("setNCharacterStream NYI");
  }

  @Override
  public void setNClob(int parameterIndex, NClob value) throws SQLException
  {
    throw new SQLException("setNClob NYI");
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader) throws SQLException
  {
    throw new SQLException("setNClob NYI");
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader, long length)
      throws SQLException
  {
    throw new SQLException("setNClob NYI");
  }

  @Override
  public void setNString(int parameterIndex, String value) throws SQLException
  {
    setVar( parameterIndex, new JsonString(value));
  }

  @Override
  public void setNull(int parameterIndex, int sqlType) throws SQLException
  {
    setVar( parameterIndex, null );
  }

  @Override
  public void setNull(int parameterIndex, int sqlType, String typeName)
      throws SQLException
  {
    setVar( parameterIndex, null );
  }

  @Override
  public void setObject(int parameterIndex, Object x) throws SQLException
  {
    // TODO: provide conversions from standard types
    setVar( parameterIndex, (JsonValue)x );
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType)
      throws SQLException
  {
    // TODO: provide conversions from standard types, check target
    setVar( parameterIndex, (JsonValue)x );
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType,
      int scaleOrLength) throws SQLException
  {
    // TODO: provide conversions from standard types, check target
    setVar( parameterIndex, (JsonValue)x );
  }

  @Override
  public void setRef(int parameterIndex, Ref x) throws SQLException
  {
    throw new SQLException("setRef NYI");
  }

  @Override
  public void setRowId(int parameterIndex, RowId x) throws SQLException
  {
    throw new SQLException("setRowId NYI");
  }

  @Override
  public void setSQLXML(int parameterIndex, SQLXML xmlObject)
      throws SQLException
  {
    throw new SQLException("setSQLXML NYI");
  }

  @Override
  public void setShort(int parameterIndex, short x) throws SQLException
  {
    setVar( parameterIndex, new JsonLong(x) );
  }

  @Override
  public void setString(int parameterIndex, String x) throws SQLException
  {
    setVar( parameterIndex, new JsonString(x) );
  }

  @Override
  public void setTime(int parameterIndex, Time x) throws SQLException
  {
    // TODO: timezone
    setVar( parameterIndex, new JsonDate(x.getTime()) );
  }

  @Override
  public void setTime(int parameterIndex, Time x, Calendar cal)
      throws SQLException
  {
    // TODO: timezone
    cal.setTime(x);
    setVar( parameterIndex, new JsonDate(cal.getTimeInMillis()) );
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException
  {
    // TODO: timezone
    setVar( parameterIndex, new JsonDate(x.getTime()) );
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
      throws SQLException
  {
    // TODO: timezone
    cal.setTime(x);
    setVar( parameterIndex, new JsonDate(cal.getTimeInMillis()) );
  }

  @Override
  public void setURL(int parameterIndex, URL x) throws SQLException
  {
    setVar( parameterIndex, new JsonString(x.toString()) );
  }

  @Override
  public void setUnicodeStream(int parameterIndex, InputStream x, int length)
      throws SQLException
  {
    throw new SQLException("setUnicodeStream NYI");
  }
}
