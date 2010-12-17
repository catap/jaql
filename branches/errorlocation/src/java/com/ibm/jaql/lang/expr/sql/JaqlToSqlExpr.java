package com.ibm.jaql.lang.expr.sql;

import java.sql.SQLException;
import java.util.Stack;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.expr.core.Expr;

public class JaqlToSqlExpr extends SqlExpr
{
  protected Expr expr;
  
  public JaqlToSqlExpr(Expr expr)
  {
    this.expr = expr;
  }

  @Override
  public void resolveColumns(Stack<SqlTableImport> context) throws SQLException
  {
  }

  @Override
  public Expr toJaql(Env env) throws Exception
  {
    return expr;
  }
  
  @Override
  public Schema getSchema()
  {
    return expr.getSchema();
  }
}

