package com.ibm.jaql.lang.expr.sql;

import com.ibm.jaql.lang.core.Var;


public class SqlTableImport
{
  protected final SqlTableExpr table;
  protected String alias;
  protected Var iterVar;
  
  public SqlTableImport(SqlTableExpr table, String alias)
  {
    this.table = table;
    this.alias = alias;
    assert table != null;
    assert alias != null;
  }
  
  public SqlColumn findColumn(String columnName)
  {
    for( SqlColumn col: table.columns() )
    {
      if( columnName.equals(col.id) )
      {
        return col;
      }
    }
    return null;
  }
}

