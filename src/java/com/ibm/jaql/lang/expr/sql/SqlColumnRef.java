package com.ibm.jaql.lang.expr.sql;

import java.sql.SQLException;
import java.util.Stack;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.path.PathExpr;
import com.ibm.jaql.lang.expr.path.PathFieldValue;


public class SqlColumnRef extends SqlExpr
{
  protected String tableAlias;
  protected final String columnName;
  protected SqlTableImport table;
  protected SqlColumn column;
  
  public SqlColumnRef(String tableAlias, String columnName)
  {
    this.tableAlias = tableAlias;
    this.columnName = columnName;
  }

  public SqlColumnRef(String columnName)
  {
    this(null, columnName);
  }
  

  public void resolveColumns(Stack<SqlTableImport> context) throws SQLException
  {
    for(int i = context.size() - 1; i >= 0; i--) 
    {
      SqlTableImport table = context.get(i);
      if( tableAlias != null  )
      {
        if( tableAlias.equals(table.alias) )
        {
          column = table.findColumn(columnName); 
          if( column == null )
          {
            throw new SQLException("table alias "+tableAlias+" does not have column "+columnName);
          }
          this.table = table;
          break;
        }
      }
      else
      {
        column = table.findColumn(columnName); 
        if( column != null )
        {
          this.table = table;
          break;
        }
      }
    }
    if( table == null )
    {
      if( tableAlias != null )
      {
        throw new SQLException("table alias not found: "+tableAlias);
      }
      else
      {
        throw new SQLException("column not found: "+columnName);
      }
    }
    tableAlias = table.alias;
  }
  
  @Override
  public Schema getSchema()
  {
    return column.getSchema();
  }

  @Override
  public Expr toJaql(Env env) throws Exception
  {
    return new PathExpr(
        new VarExpr(table.iterVar), 
        new PathFieldValue(new ConstExpr(new JsonString(columnName))));
  }
}

