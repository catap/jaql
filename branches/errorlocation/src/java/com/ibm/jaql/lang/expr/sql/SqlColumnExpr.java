package com.ibm.jaql.lang.expr.sql;

import java.sql.SQLException;
import java.util.Stack;


public class SqlColumnExpr extends SqlColumn
{
  protected SqlExpr expr;
  
  public SqlColumnExpr(SqlExpr expr, String id, int index)
  {
    super( makeId(id,expr,index), null );
    this.expr = expr;
    assert this.id != null;
  }
  
  public void resolveColumns(Stack<SqlTableImport> context) throws SQLException
  {
    expr.resolveColumns(context);
    schema = expr.getSchema();
  }


  private static String makeId(String id, SqlExpr expr, int index)
  {
    if( id == null )
    {
      if( expr instanceof SqlColumnRef )
      {
        id = ((SqlColumnRef)expr).columnName;
      }
      else
      {
        id = "#"+index;
      }
    }
    return id;
  }
}

