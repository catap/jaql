package com.ibm.jaql.lang.expr.sql;

import java.sql.SQLException;
import java.util.Stack;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.expr.core.Expr;


public class SqlExprToTable extends SchemaBasedSqlTableExpr
{
  protected SqlExpr expr;

  public SqlExprToTable(SqlExpr column)
  {
    this.expr = column;
  }

  @Override
  public void resolveColumns(Stack<SqlTableImport> context) throws SQLException
  {
    expr.resolveColumns(context);
    super.resolveColumns(context);
  }
  
  @Override
  public Schema getSchema()
  {
    return expr.getSchema();
  }

  @Override
  public Expr toJaql(Env env) throws Exception
  {
    return expr.toJaql(env);
  }

}
