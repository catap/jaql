package com.ibm.jaql.jdbc;

import static org.junit.Assert.fail;

import java.sql.SQLException;

import org.hsqldb.Types;
import org.junit.Test;


public class TestJaqlJdbc
{
  @Test public void foo() throws SQLException
  {
    JaqlJdbcDriver driver = new JaqlJdbcDriver();
    JaqlJdbcConnection conn = driver.connect("jdbc:jaql:", null);
    JaqlJdbcStatement stmt = conn.createStatement();
    if( ! stmt.execute("range(3) -> transform { long: $, double: double($), decimal: $+0m, string: strcat('hi',$), date: now() };") )
    {
      fail("expected result set");
    }
    JaqlJdbcResultSet rs = stmt.getResultSet();
    JaqlJdbcResultSetMetaData meta = rs.getMetaData();
    for(int i = 1 ; i <= meta.getColumnCount() ; i++)
    {
      System.out.println(meta.getColumnName(i) + ": " + Types.getTypeName(meta.getColumnType(i)));
    }
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
  }
}
