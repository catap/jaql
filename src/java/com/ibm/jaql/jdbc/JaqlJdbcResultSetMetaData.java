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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.ibm.jaql.json.schema.RecordSchema;

public class JaqlJdbcResultSetMetaData implements ResultSetMetaData
{
  protected RecordSchema schema;
  protected JaqlSqlColumn[] columns;
  
  public JaqlJdbcResultSetMetaData(RecordSchema schema)
  {
    this.schema = schema;
    // TODO: include extra fields? or full record?
    columns = new JaqlSqlColumn[schema.noRequiredOrOptional()];
    int i = 0;
    for( RecordSchema.Field f: schema.getFieldsByPosition() )
    {
      columns[i] = new JaqlSqlColumn(f.getName(), i+1, JaqlSqlType.make(f.getSchema(), f.isOptional()));
      i++;
    }
  }

  @Override
  public String getCatalogName(int column) throws SQLException
  {
    return "";
  }

  @Override
  public String getColumnClassName(int column) throws SQLException
  {
    return columns[column-1].getClass().getName();
  }

  @Override
  public int getColumnCount() throws SQLException
  {
    return columns.length;
  }

  @Override
  public int getColumnDisplaySize(int column) throws SQLException
  {
    return 10; // TODO: what to do here?
  }

  @Override
  public String getColumnLabel(int column) throws SQLException
  {
    return columns[column-1].getName();
  }

  @Override
  public String getColumnName(int column) throws SQLException
  {
    return columns[column-1].getName();
  }

  @Override
  public int getColumnType(int column) throws SQLException
  {
    return columns[column-1].getType().getSqlType();
  }

  @Override
  public String getColumnTypeName(int column) throws SQLException
  {
    return columns[column-1].getType().getJsonSchema().toString();
  }

  @Override
  public int getPrecision(int column) throws SQLException
  {
    return 30; // TODO: what to do here?
  }

  @Override
  public int getScale(int column) throws SQLException
  {
    return 15; // TODO: what to do here?
  }

  @Override
  public String getSchemaName(int column) throws SQLException
  {
    return "";
  }

  @Override
  public String getTableName(int column) throws SQLException
  {
    return "";
  }

  @Override
  public boolean isAutoIncrement(int column) throws SQLException
  {
    return false;
  }

  @Override
  public boolean isCaseSensitive(int column) throws SQLException
  {
    return true;
  }

  @Override
  public boolean isCurrency(int column) throws SQLException
  {
    return false;
  }

  @Override
  public boolean isDefinitelyWritable(int column) throws SQLException
  {
    return false;
  }

  @Override
  public int isNullable(int column) throws SQLException
  {
    // TODO: we do have columnNullableUnknown sometimes, but currently we just say nullable
    return columns[column-1].getType().isNullable() ? columnNullable : columnNoNulls;
  }

  @Override
  public boolean isReadOnly(int column) throws SQLException
  {
    return true;
  }

  @Override
  public boolean isSearchable(int column) throws SQLException
  {
    return false;
  }

  @Override
  public boolean isSigned(int column) throws SQLException
  {
    return false;
  }

  @Override
  public boolean isWritable(int column) throws SQLException
  {
    return false;
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

  public JaqlSqlColumn getColumn(int columnIndex)
  {
    return columns[columnIndex-1];
  }

  public JaqlSqlColumn getColumn(String columnLabel) throws SQLException
  {
    // TODO: add map
    for( int i = 0 ; i < columns.length ; i++ )
    {
      if( columns[i].getName().equals(columnLabel) )
      {
        return columns[i];
      }
    }
    throw new SQLException("column not found: "+columnLabel);
  }
  
  public JaqlSqlType getType(int columnIndex)
  {
    return getColumn(columnIndex).getType();
  }

  public JaqlSqlType getType(String columnLabel) throws SQLException
  {
    return getColumn(columnLabel).getType();
  }

}
