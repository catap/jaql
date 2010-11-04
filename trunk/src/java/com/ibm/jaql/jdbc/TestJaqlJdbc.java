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

import static org.junit.Assert.fail;

import java.sql.SQLException;

import junit.framework.Assert;

import org.hsqldb.Types;
import org.junit.Test;


public class TestJaqlJdbc
{
  @Test public void foo() throws SQLException
  {
    String url = "jdbc:jaql:";
    // String url = "jdbc:jaql:/fake_catalog;jaql.modules.dir=c:/temp/jaql-modules";
    // TODO: get a catalog in the test
    JaqlJdbcDriver driver = new JaqlJdbcDriver();
    JaqlJdbcConnection conn = driver.connect(url, null);
    
    JaqlJdbcDatabaseMetaData dbmeta = conn.getMetaData();

    JaqlJdbcResultSet rs = dbmeta.getTables(null, null, null, null);
    JaqlJdbcResultSetMetaData meta = rs.getMetaData();
    int n = meta.getColumnCount();
    System.out.println("\nresult columns: (count="+n+")");
    for(int i = 1 ; i <= n ; i++)
    {
      System.out.println(meta.getColumnName(i) + ": " + Types.getTypeName(meta.getColumnType(i)));
    }
    System.out.println("\ntables: ");
    while( rs.next() )
    {
      System.out.println(meta.getColumnLabel(3) + ": "+rs.getString(3));
    }

    rs = dbmeta.getColumns(null, null, null, null);
    meta = rs.getMetaData();
    n = meta.getColumnCount();
    System.out.println("\nresult columns: (count="+n+")");
    for(int i = 1 ; i <= n ; i++)
    {
      System.out.println(meta.getColumnName(i) + ": " + Types.getTypeName(meta.getColumnType(i)));
    }
    System.out.println("\ncolumns: ");
    while( rs.next() )
    {
      System.out.println(meta.getColumnLabel(3) + ": "+rs.getString(3));
      System.out.println(meta.getColumnLabel(4) + ": "+rs.getString(4));
    }

    JaqlJdbcStatement stmt = conn.createStatement();
    if( ! stmt.execute("range(3) -> transform { long: $, double: double($), decimal: $+0m, string: strcat('hi',$), date: now() };") )
    {
      fail("expected result set");
    }
    rs = stmt.getResultSet();
    meta = rs.getMetaData();
    n = meta.getColumnCount();
    System.out.println("\nresult columns: (count="+n+")");
    for(int i = 1 ; i <= n ; i++)
    {
      System.out.println(meta.getColumnName(i) + ": " + Types.getTypeName(meta.getColumnType(i)));
    }
    System.out.println("\nresults:");
    while( rs.next() )
    {
      int i = 1;
      System.out.print(meta.getColumnLabel(i) + ": "+rs.getLong(i++)+ " ");
      System.out.print(meta.getColumnLabel(i) + ": "+rs.getDouble(i++)+ " ");
      System.out.print(meta.getColumnLabel(i) + ": "+rs.getBigDecimal(i++)+ " ");
      System.out.print(meta.getColumnLabel(i) + ": '"+rs.getString(i++)+ "' ");
      System.out.print(meta.getColumnLabel(i) + ": "+rs.getTimestamp(i++)+ " ");
      System.out.println();
    }
    
    JaqlJdbcPreparedStatement pstmt = conn.prepareStatement(
        "extern x: long := 1; "+
        "extern y: string := 'hello'; "+
        "extern z: string := 'jdbc'; "+
        "range(x) -> transform { i: $, x, y, z };"
      );
    JaqlJdbcParameterMetaData params = pstmt.getParameterMetaData();
    n = params.getParameterCount();
    System.out.println("\nparameters: (count="+n+")");
    Assert.assertEquals(3, n);
    for(int i = 1 ; i <= n ; i++)
    {    
      System.out.println(
          i +": "+ params.getParameterName(i) 
          + ": " + Types.getTypeName(params.getParameterType(i)) 
          + ": " + params.getParameterTypeName(i));
    }
    meta = pstmt.getMetaData();
    n = meta.getColumnCount();
    System.out.println("result columns: (count="+n+")");
    for(int i = 1 ; i <= n ; i++)
    {
      System.out.println(meta.getColumnName(i) + ": " + Types.getTypeName(meta.getColumnType(i)));
    }
    for( int x = 0 ; x < 4 ; x++ )
    {
      pstmt.setLong(1, x);
      if( ! pstmt.execute() )
      {
        fail("expected result set");
      }
      rs = pstmt.getResultSet();
      System.out.println("\nresults:");
      while( rs.next() )
      {
        int i = 1;
        System.out.print(meta.getColumnLabel(i) + ": "+rs.getLong(i++)+ " ");
        System.out.print(meta.getColumnLabel(i) + ": "+rs.getLong(i++)+ " ");
        System.out.print(meta.getColumnLabel(i) + ": "+rs.getString(i++)+ " ");
        System.out.print(meta.getColumnLabel(i) + ": '"+rs.getString(i++)+ "' ");
        System.out.println();
      }
      pstmt.close();
    }
  }
}
