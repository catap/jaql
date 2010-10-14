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

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.List;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.JsonType;
import com.ibm.jaql.lang.ParsedJaql;
import com.ibm.jaql.lang.core.Var;

public class JaqlJdbcParameterMetaData implements ParameterMetaData
{
  protected ParsedJaql parsedJaql;
  protected Var[] vars;

  public JaqlJdbcParameterMetaData(ParsedJaql parsedJaql)
  {
    List<Var> varlist = parsedJaql.getExternalVariables();
    vars = varlist.toArray(new Var[varlist.size()]);
  }
  
  @Override
  public int getParameterCount() throws SQLException
  {
    System.err.println("param count: "+vars.length);
    return vars.length;
  }

  /** Return the name of the parameter.  Unfortunately, JDBC only uses positions... */
  public String getParameterName(int param) throws SQLException
  {
    Var var = vars[param-1];
    return var.name();
  }

  /** Return the index of the named parameter or -1 if not found. */
  public int getParameterIndex(String name) throws SQLException
  {
    for(int i = vars.length ; i > 0 ; i--)
    {
      if( vars[i-1].name().equals(name) )
      {
        return i;
      }
    }
    return -1;
  }


  @Override
  public String getParameterClassName(int param) throws SQLException
  {
    Var var = vars[param-1];
    Schema schema = var.getSchema();
    String cls = schema.getSchemaType().getJsonType().getMainClass().getName();
    System.err.println("param "+param+" cls is "+cls+" "+schema);
    return cls;
  }

  @Override
  public int getParameterMode(int param) throws SQLException
  {
    // support other modes?
    return parameterModeIn;
  }

  @Override
  public int getParameterType(int param) throws SQLException
  {
    Var var = vars[param-1];
    Schema schema = var.getSchema();
    JaqlSqlType type = JaqlSqlType.make(schema, false);
    System.err.println("param "+param+" type is "+type.getSqlType()+" "+schema);
    return type.getSqlType();
  }

  @Override
  public String getParameterTypeName(int param) throws SQLException
  {
    Var var = vars[param-1];
    Schema schema = var.getSchema();
    System.err.println("param "+param+" has "+schema);
    return schema.toString();
  }

  @Override
  public int isNullable(int param) throws SQLException
  {
    Var var = vars[param-1];
    return var.getSchema().is(JsonType.NULL).never() ? parameterNoNulls : parameterNullable;
  }

  @Override
  public int getPrecision(int param) throws SQLException
  {
    // what to do here?
    return 0;
  }

  @Override
  public int getScale(int param) throws SQLException
  {
    // what to do here?
    return 0;
  }

  @Override
  public boolean isSigned(int param) throws SQLException
  {
    // what to do here?
    return true;
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
