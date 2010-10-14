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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

import org.hsqldb.lib.StringInputStream;

import com.ibm.jaql.json.schema.RecordSchema;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;


public class JaqlJdbcResultSet implements ResultSet
{
  protected final JaqlJdbcStatement stmt;
  protected final JaqlJdbcResultSetMetaData metadata;
  protected JsonIterator iter;

  // The number of rows retrieved
  protected int rowCount;
  
  // The current row
  protected JsonRecord currentRec;

  // The current column type and value
  protected JaqlSqlColumn column;
  protected JaqlSqlType type;
  protected JsonValue value;

  public JaqlJdbcResultSet(JaqlJdbcStatement stmt, RecordSchema schema, JsonIterator iter)
  {
    this.stmt = stmt;
    this.iter = iter;
    metadata = new JaqlJdbcResultSetMetaData(schema);
  }

  @Override
  public boolean absolute(int row) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("absolute positioning not supported");
  }

  @Override
  public void afterLast() throws SQLException
  {
    throw new SQLFeatureNotSupportedException("absolute positioning not supported");
  }

  @Override
  public void beforeFirst() throws SQLException
  {
    throw new SQLFeatureNotSupportedException("absolute positioning not supported");
  }

  @Override
  public void cancelRowUpdates() throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates not supported");
  }

  @Override
  public void clearWarnings() throws SQLException
  {
  }

  @Override
  public void close() throws SQLException
  {
    iter = null;
  }

  @Override
  public void deleteRow() throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates not supported");
  }

  @Override
  public int findColumn(String columnLabel) throws SQLException
  {
    return metadata.getColumn(columnLabel).getIndex();
  }

  @Override
  public boolean first() throws SQLException
  {
    throw new SQLFeatureNotSupportedException("absolute positioning not supported");
  }

  public void getValue(int columnIndex)
  {
    column = metadata.getColumn(columnIndex);
    type = column.getType();
    value = column.getValue(currentRec);
  }

  @Override
  public Array getArray(int columnIndex) throws SQLException
  {
    getValue(columnIndex);
    return value == null ? null : type.getArray(value);
  }

  @Override
  public Array getArray(String columnLabel) throws SQLException
  {
    return getArray(findColumn(columnLabel));
  }

  @Override
  public InputStream getAsciiStream(int columnIndex) throws SQLException
  {
    String s = getString(columnIndex);
    return (s == null) ? null : new StringInputStream(s);
  }

  @Override
  public InputStream getAsciiStream(String columnLabel) throws SQLException
  {
    return getAsciiStream(findColumn(columnLabel));
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException
  {
    getValue(columnIndex);
    return value == null ? null : type.getBigDecimal(value);
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException
  {
    return getBigDecimal(findColumn(columnLabel));
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale)
      throws SQLException
  {
    BigDecimal x = getBigDecimal(columnIndex);
    return ( x == null ) ? null : x.setScale(scale);
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel, int scale)
      throws SQLException
  {
    return getBigDecimal(findColumn(columnLabel), scale);
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException
  {
    byte[] x = getBytes(columnIndex);
    return (x == null) ? null : new ByteArrayInputStream(x);
  }

  @Override
  public InputStream getBinaryStream(String columnLabel) throws SQLException
  {
    return getBinaryStream(findColumn(columnLabel));
  }

  @Override
  public Blob getBlob(int columnIndex) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("no blobs yet");
  }

  @Override
  public Blob getBlob(String columnLabel) throws SQLException
  {
    return getBlob(columnLabel);
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException
  {
    getValue(columnIndex);
    return value == null ? false : type.getBoolean(value);
  }

  @Override
  public boolean getBoolean(String columnLabel) throws SQLException
  {
    return getBoolean(findColumn(columnLabel));
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException
  {
    getValue(columnIndex);
    return value == null ? 0 : type.getByte(value);
  }

  @Override
  public byte getByte(String columnLabel) throws SQLException
  {
    return getByte(findColumn(columnLabel));
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException
  {
    getValue(columnIndex);
    return value == null ? null : type.getBytes(value);
  }

  @Override
  public byte[] getBytes(String columnLabel) throws SQLException
  {
    return getBytes(findColumn(columnLabel));
  }

  @Override
  public Reader getCharacterStream(int columnIndex) throws SQLException
  {
    String s = getString(columnIndex);
    return s == null ? null : new StringReader(s);
  }

  @Override
  public Reader getCharacterStream(String columnLabel) throws SQLException
  {
    return getCharacterStream(findColumn(columnLabel));
  }

  @Override
  public Clob getClob(int columnIndex) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("no clobs yet");
  }

  @Override
  public Clob getClob(String columnLabel) throws SQLException
  {
    return getClob(findColumn(columnLabel));
  }

  @Override
  public int getConcurrency() throws SQLException
  {
    return ResultSet.CONCUR_READ_ONLY;
  }

  @Override
  public String getCursorName() throws SQLException
  {
    throw new SQLFeatureNotSupportedException("cursors");
  }

  @Override
  public Date getDate(int columnIndex) throws SQLException
  {
    getValue(columnIndex);
    return value == null ? null : type.getDate(value);
  }

  @Override
  public Date getDate(String columnLabel) throws SQLException
  {
    return getDate(findColumn(columnLabel));
  }

  @Override
  public Date getDate(int columnIndex, Calendar cal) throws SQLException
  {
    return getDate(columnIndex);
  }

  @Override
  public Date getDate(String columnLabel, Calendar cal) throws SQLException
  {
    return getDate(findColumn(columnLabel), cal);
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException
  {
    getValue(columnIndex);
    return value == null ? 0 : type.getDouble(value);
  }

  @Override
  public double getDouble(String columnLabel) throws SQLException
  {
    return getDouble(findColumn(columnLabel));
  }

  @Override
  public int getFetchDirection() throws SQLException
  {
    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public int getFetchSize() throws SQLException
  {
    return 1;
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException
  {
    getValue(columnIndex);
    return value == null ? 0 : type.getFloat(value);
  }

  @Override
  public float getFloat(String columnLabel) throws SQLException
  {
    return getFloat(findColumn(columnLabel));
  }

  @Override
  public int getHoldability() throws SQLException
  {
    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override
  public int getInt(int columnIndex) throws SQLException
  {
    getValue(columnIndex);
    return value == null ? 0 : type.getInt(value);
  }

  @Override
  public int getInt(String columnLabel) throws SQLException
  {
    return getInt(findColumn(columnLabel));
  }

  @Override
  public long getLong(int columnIndex) throws SQLException
  {
    getValue(columnIndex);
    return value == null ? 0 : type.getLong(value);
  }

  @Override
  public long getLong(String columnLabel) throws SQLException
  {
    return getLong(findColumn(columnLabel));
  }

  @Override
  public JaqlJdbcResultSetMetaData getMetaData() throws SQLException
  {
    return metadata;
  }

  @Override
  public Reader getNCharacterStream(int columnIndex) throws SQLException
  {
    return getCharacterStream(columnIndex);
  }

  @Override
  public Reader getNCharacterStream(String columnLabel) throws SQLException
  {
    return getNCharacterStream(findColumn(columnLabel));
  }

  @Override
  public NClob getNClob(int columnIndex) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("clobs");
  }

  @Override
  public NClob getNClob(String columnLabel) throws SQLException
  {
    return getNClob(findColumn(columnLabel));
  }

  @Override
  public String getNString(int columnIndex) throws SQLException
  {
    return getString(columnIndex);
  }

  @Override
  public String getNString(String columnLabel) throws SQLException
  {
    return getNString(findColumn(columnLabel));
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException
  {
    getValue(columnIndex); // TODO: convert types?  eg, JsonString -> String
    return value;
  }

  @Override
  public Object getObject(String columnLabel) throws SQLException
  {
    return getObject(findColumn(columnLabel));
  }

  @Override
  public Object getObject(int columnIndex, Map<String, Class<?>> map)
      throws SQLException
  {
    return new SQLFeatureNotSupportedException("object maps");
  }

  @Override
  public Object getObject(String columnLabel, Map<String, Class<?>> map)
      throws SQLException
  {
    return getObject(findColumn(columnLabel), map);
  }

  @Override
  public Ref getRef(int columnIndex) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("ref");
  }

  @Override
  public Ref getRef(String columnLabel) throws SQLException
  {
    return getRef(findColumn(columnLabel));
  }

  @Override
  public int getRow() throws SQLException
  {
    return rowCount;
  }

  @Override
  public RowId getRowId(int columnIndex) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("rowid");
  }

  @Override
  public RowId getRowId(String columnLabel) throws SQLException
  {
    return getRowId(findColumn(columnLabel));
  }

  @Override
  public SQLXML getSQLXML(int columnIndex) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("sqlxml");
  }

  @Override
  public SQLXML getSQLXML(String columnLabel) throws SQLException
  {
    return getSQLXML(findColumn(columnLabel));
  }

  @Override
  public short getShort(int columnIndex) throws SQLException
  {
    getValue(columnIndex);
    return value == null ? 0 : type.getShort(value);
  }

  @Override
  public short getShort(String columnLabel) throws SQLException
  {
    return getShort(findColumn(columnLabel));
  }

  @Override
  public Statement getStatement() throws SQLException
  {
    return stmt;
  }

  @Override
  public String getString(int columnIndex) throws SQLException
  {
    getValue(columnIndex);
    return value == null ? null : type.getString(value);
  }

  @Override
  public String getString(String columnLabel) throws SQLException
  {
    return getString(findColumn(columnLabel));
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException
  {
    getValue(columnIndex);
    return value == null ? null : type.getTime(value);
  }

  @Override
  public Time getTime(String columnLabel) throws SQLException
  {
    return getTime(findColumn(columnLabel));
  }

  @Override
  public Time getTime(int columnIndex, Calendar cal) throws SQLException
  {
    return getTime(columnIndex);
  }

  @Override
  public Time getTime(String columnLabel, Calendar cal) throws SQLException
  {
    return getTime(findColumn(columnLabel), cal);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException
  {
    getValue(columnIndex);
    return value == null ? null : type.getTimestamp(value);
  }

  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException
  {
    return getTimestamp(findColumn(columnLabel));
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal)
      throws SQLException
  {
    return getTimestamp(columnIndex);
  }

  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar cal)
      throws SQLException
  {
    return getTimestamp(findColumn(columnLabel), cal);
  }

  @Override
  public int getType() throws SQLException
  {
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public URL getURL(int columnIndex) throws SQLException
  {
    String s = getString(columnIndex);
    try
    {
      return s == null ? null : new URL(s);
    }
    catch (MalformedURLException e)
    {
      throw new SQLException("malformed url", e);
    }
  }

  @Override
  public URL getURL(String columnLabel) throws SQLException
  {
    return getURL(findColumn(columnLabel));
  }

  @Override
  public InputStream getUnicodeStream(int columnIndex) throws SQLException
  {
    String s = getString(columnIndex);
    return s == null ? null : new ByteArrayInputStream(s.getBytes());
  }

  @Override
  public InputStream getUnicodeStream(String columnLabel) throws SQLException
  {
    return getUnicodeStream(findColumn(columnLabel));
  }

  @Override
  public SQLWarning getWarnings() throws SQLException
  {
    return null;
  }

  
  
  @Override
  public void insertRow() throws SQLException
  {
    throw new SQLFeatureNotSupportedException("insert");
  }

  @Override
  public boolean isAfterLast() throws SQLException
  {
    return iter == null;
  }

  @Override
  public boolean isBeforeFirst() throws SQLException
  {
    return rowCount == 0;
  }

  @Override
  public boolean isClosed() throws SQLException
  {
    return iter == null;
  }

  @Override
  public boolean isFirst() throws SQLException
  {
    return rowCount == 1;
  }

  @Override
  public boolean isLast() throws SQLException
  {
    throw new SQLFeatureNotSupportedException("isLast");
  }

  @Override
  public boolean last() throws SQLException
  {
    throw new SQLFeatureNotSupportedException("last");
  }

  @Override
  public void moveToCurrentRow() throws SQLException
  {
    throw new SQLFeatureNotSupportedException("scrolling");
  }

  @Override
  public void moveToInsertRow() throws SQLException
  {
    throw new SQLFeatureNotSupportedException("scrolling");
  }

  @Override
  public boolean next() throws SQLException
  {
    try
    {
      currentRec = null;
      if( ! iter.moveNext() )
      {
        iter = null;
        return false;
      }
      currentRec = (JsonRecord)iter.current();
      return true;
    }
    catch( Exception e )
    {
      throw new SQLException("error from jaql", e);
    }
  }

  @Override
  public boolean previous() throws SQLException
  {
    throw new SQLFeatureNotSupportedException("scrolling");
  }

  @Override
  public void refreshRow() throws SQLException
  {
    throw new SQLFeatureNotSupportedException("scrolling");
  }

  @Override
  public boolean relative(int rows) throws SQLException
  {
    if( rows < 0 )
    {
      throw new SQLFeatureNotSupportedException("scrolling");
    }
    try
    {
      currentRec = null;
      if( ! iter.moveN(rows) )
      {
        iter = null;
        return false;
      }
      currentRec = (JsonRecord)iter.current();
      return true;
    }
    catch( Exception e )
    {
      throw new SQLException("error from jaql", e);
    }
  }

  @Override
  public boolean rowDeleted() throws SQLException
  {
    return false;
  }

  @Override
  public boolean rowInserted() throws SQLException
  {
    return false;
  }

  @Override
  public boolean rowUpdated() throws SQLException
  {
    return false;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException
  {
    if( direction != ResultSet.FETCH_FORWARD)
    {
      throw new SQLFeatureNotSupportedException("scrolling");
    }
  }

  @Override
  public void setFetchSize(int rows) throws SQLException
  {
  }

  @Override
  public void updateArray(int columnIndex, Array x) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateArray(String columnLabel, Array x) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x)
      throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x)
      throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length)
      throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length)
      throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length)
      throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length)
      throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x)
      throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x)
      throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x)
      throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x)
      throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length)
      throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length)
      throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length)
      throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length)
      throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateBlob(String columnLabel, Blob x) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream)
      throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream)
      throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream, long length)
      throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream,
      long length) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateBoolean(String columnLabel, boolean x) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateByte(String columnLabel, byte x) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateBytes(String columnLabel, byte[] x) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x)
      throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader)
      throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length)
      throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader,
      int length) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length)
      throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader,
      long length) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateClob(String columnLabel, Clob x) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateClob(int columnIndex, Reader reader) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateClob(String columnLabel, Reader reader) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateClob(int columnIndex, Reader reader, long length)
      throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateClob(String columnLabel, Reader reader, long length)
      throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateDate(int columnIndex, Date x) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateDate(String columnLabel, Date x) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateDouble(String columnLabel, double x) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateFloat(String columnLabel, float x) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateInt(int columnIndex, int x) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateInt(String columnLabel, int x) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateLong(int columnIndex, long x) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateLong(String columnLabel, long x) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x)
      throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader)
      throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length)
      throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader,
      long length) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateNClob(int columnIndex, NClob clob) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateNClob(String columnLabel, NClob clob) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader)
      throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader, long length)
      throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader, long length)
      throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateNString(int columnIndex, String string) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateNString(String columnLabel, String string)
      throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateNull(int columnIndex) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateNull(String columnLabel) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateObject(String columnLabel, Object x) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateObject(int columnIndex, Object x, int scaleOrLength)
      throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateObject(String columnLabel, Object x, int scaleOrLength)
      throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateRef(int columnIndex, Ref x) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateRef(String columnLabel, Ref x) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateRow() throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateRowId(int columnIndex, RowId x) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateRowId(String columnLabel, RowId x) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateSQLXML(int columnIndex, SQLXML xmlObject)
      throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateSQLXML(String columnLabel, SQLXML xmlObject)
      throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateShort(int columnIndex, short x) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateShort(String columnLabel, short x) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateString(int columnIndex, String x) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateString(String columnLabel, String x) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateTime(String columnLabel, Time x) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public void updateTimestamp(String columnLabel, Timestamp x)
      throws SQLException
  {
    throw new SQLFeatureNotSupportedException("updates");
  }

  @Override
  public boolean wasNull() throws SQLException
  {
    return value == null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException
  {
    return false;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException
  {
    return null;
  }

}
