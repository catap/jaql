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

import java.io.IOException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.ibm.jaql.lang.ConsumingExceptionHandler;
import com.ibm.jaql.lang.Jaql;
import com.ibm.jaql.util.BaseUtil;

public class JaqlJdbcConnection implements Connection
{
  public static final String PROP_INIT_SCRIPT = "jaql.initial.script";
  public static final String PROP_MODULE_PATH = "jaql.modules.dir";
  
  protected String url;
  protected Properties properties;
  protected Jaql jaql;
  
  public JaqlJdbcConnection(String url, Properties properties) throws SQLException
  {
    if( properties == null )
    {
      properties = new Properties();
    }
    this.url = url;
    this.properties = properties;
    String script = "";
    String s;

    int i = url.indexOf(';');
    if( i >= 0 )
    {
      s = url.substring(i+1);
      url = url.substring(0,i);
      for( String p : s.split(";") )
      {
        i = p.indexOf('=');
        if( i >= 0 )
        {
          String key = p.substring(0,i);
          String val = p.substring(i+1);
          properties.put(key, val);
        }
      }
    }
    
    s = properties.getProperty(PROP_MODULE_PATH);
    if( s != null )
    {
      System.setProperty(PROP_MODULE_PATH, s);
      // TODO: jaql.addModulePath, setModulePath, getModulePath
    }
    
    i = url.indexOf('/');
    if( i >= 0 )
    {
      String p = url.substring(i+1);
      if( ! "".equals(p) )
      {
        script += "import "+p+" (*);";
      }
    }
    
    // TODO: what if this returns results... It shouldn't be allowed to...
    s = properties.getProperty(PROP_INIT_SCRIPT);
    if( s != null )
    {
      script += s;
    }
    
    jaql = new Jaql();
    jaql.setExceptionHandler(new ConsumingExceptionHandler(jaql));
    jaql.setInput(script);
    try
    {
      jaql.run();
    }
    catch (SQLException e)
    {
      throw e;
    }
    catch (Exception e)
    {
      Throwable c = BaseUtil.getRootCause(e);
      throw new SQLException("while running jaql initialization script: ["+c.getClass().getSimpleName()+"]"+c.getMessage(),"58033",e);
    }
  }
  
  @Override
  public void clearWarnings() throws SQLException
  {
  }

  @Override
  public void close() throws SQLException
  {
    try
    {
      jaql.close();
    }
    catch (IOException e)
    {
      throw new SQLException("while closing jaql", e);
    }
    finally
    {
      jaql = null;
    }
  }

  @Override
  public void commit() throws SQLException
  {
    // TODO: jaql.reset();
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements)
      throws SQLException
  {
    throw new UnsupportedOperationException("NYI");
  }

  @Override
  public Blob createBlob() throws SQLException
  {
    throw new UnsupportedOperationException("NYI");
  }

  @Override
  public Clob createClob() throws SQLException
  {
    throw new UnsupportedOperationException("NYI");
  }

  @Override
  public NClob createNClob() throws SQLException
  {
    throw new UnsupportedOperationException("NYI");
  }

  @Override
  public SQLXML createSQLXML() throws SQLException
  {
    throw new UnsupportedOperationException("NYI");
  }

  @Override
  public JaqlJdbcStatement createStatement() throws SQLException
  {
    return new JaqlJdbcStatement(this);
  }

  @Override
  public JaqlJdbcStatement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException
  {
    if( resultSetType != ResultSet.TYPE_FORWARD_ONLY )
    {
      throw new SQLFeatureNotSupportedException("scrollable cursors");
    }
    if( resultSetConcurrency != ResultSet.CONCUR_READ_ONLY )
    {
      throw new SQLFeatureNotSupportedException("updatable cursors");
    }
    return createStatement();
  }

  @Override
  public JaqlJdbcStatement createStatement(
      int resultSetType, 
      int resultSetConcurrency,
      int resultSetHoldability) 
    throws SQLException
  {
    if( resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT )
    {
      throw new SQLFeatureNotSupportedException("holding cursors");
    }
    return createStatement(resultSetType, resultSetConcurrency);
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes)
      throws SQLException
  {
    throw new UnsupportedOperationException("NYI");
  }

  @Override
  public boolean getAutoCommit() throws SQLException
  {
    return true;
  }

  @Override
  public String getCatalog() throws SQLException
  {
    return "jaql";
  }

  @Override
  public Properties getClientInfo() throws SQLException
  {
    return new Properties();
  }

  @Override
  public String getClientInfo(String name) throws SQLException
  {
    return null;
  }

  @Override
  public int getHoldability() throws SQLException
  {
    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override
  public JaqlJdbcDatabaseMetaData getMetaData() throws SQLException
  {
    return new JaqlJdbcDatabaseMetaData(this);
  }

  @Override
  public int getTransactionIsolation() throws SQLException
  {
    return Connection.TRANSACTION_NONE;
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException
  {
    return new HashMap<String, Class<?>>();
  }

  @Override
  public SQLWarning getWarnings() throws SQLException
  {
    return null;
  }

  @Override
  public boolean isClosed() throws SQLException
  {
    return jaql == null;
  }

  @Override
  public boolean isReadOnly() throws SQLException
  {
    return false;
  }

  @Override
  public boolean isValid(int timeout) throws SQLException
  {
    return isClosed();
  }

  @Override
  public String nativeSQL(String sql) throws SQLException
  {
    jaql.setInput(sql);
    String exp;
    try
    {
      exp = jaql.explain();
      return exp;
    }
    catch (Exception e)
    {
      throw new SQLException("during jaql explain", e);
    }
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException
  {
    return new JaqlJdbcCallableStatement(jaql, sql);
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType,
      int resultSetConcurrency) throws SQLException
  {
    if( resultSetType != ResultSet.TYPE_FORWARD_ONLY )
    {
      throw new SQLException("only forward cursors are supported");
    }
    if( resultSetConcurrency != ResultSet.CONCUR_READ_ONLY )
    {
      throw new SQLException("only readonly cursors are supported");
    }
    return prepareCall(sql);
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType,
      int resultSetConcurrency, int resultSetHoldability) throws SQLException
  {
    if( resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT )
    {
      throw new SQLException("cursors are not holdable");
    }
    return prepareCall(sql, resultSetType, resultSetConcurrency);
  }

  @Override
  public JaqlJdbcPreparedStatement prepareStatement(String script) throws SQLException
  {
    return new JaqlJdbcPreparedStatement(this, script);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
      throws SQLException
  {
    if( autoGeneratedKeys != Statement.NO_GENERATED_KEYS )
    {
      throw new SQLException("generated keys are not available");
    }
    return prepareStatement(sql);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
      throws SQLException
  {
    throw new SQLException("generated keys are not available");
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames)
      throws SQLException
  {
    throw new SQLException("generated keys are not available");
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType,
      int resultSetConcurrency) throws SQLException
  {
    if( resultSetType != ResultSet.TYPE_FORWARD_ONLY )
    {
      throw new SQLException("only forward cursors are supported");
    }
    if( resultSetConcurrency != ResultSet.CONCUR_READ_ONLY )
    {
      throw new SQLException("only readonly cursors are supported");
    }
    return prepareStatement(sql);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType,
      int resultSetConcurrency, int resultSetHoldability) throws SQLException
  {
    if( resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT )
    {
      throw new SQLException("cursors are not holdable");
    }
    return prepareStatement(sql, resultSetType, resultSetConcurrency);
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException
  {
    throw new SQLException("savepoint not supported");
  }

  @Override
  public void rollback() throws SQLException
  {
    throw new SQLException("rollback not supported");
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException
  {
    throw new SQLException("rollback not supported");
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException
  {
    if( autoCommit != true )
    {
      throw new SQLException("autocommit must be on");
    }
  }

  @Override
  public void setCatalog(String catalog) throws SQLException
  {
    throw new UnsupportedOperationException("NYI");
  }

  @Override
  public void setClientInfo(Properties properties)
      throws SQLClientInfoException
  {
    throw new UnsupportedOperationException("NYI");
  }

  @Override
  public void setClientInfo(String name, String value)
      throws SQLClientInfoException
  {
    throw new UnsupportedOperationException("NYI");
  }

  @Override
  public void setHoldability(int holdability) throws SQLException
  {
    throw new UnsupportedOperationException("NYI");
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException
  {
    if( readOnly != false )
    {
      throw new SQLException("readOnly must be false");
    }
  }

  @Override
  public Savepoint setSavepoint() throws SQLException
  {
    throw new SQLException("savepoints not supported");
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException
  {
    throw new SQLException("savepoints not supported");
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException
  {
    if( level != Connection.TRANSACTION_NONE )
    {
      throw new SQLException("isolation is not supported");
    }
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException
  {
    throw new UnsupportedOperationException("NYI");
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
