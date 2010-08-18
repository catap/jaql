package com.ibm.jaql.lang.expr.sql;

import java.sql.SQLException;
import java.util.Stack;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.util.JaqlUtil;

// FIXME: The com.ibm.jaql.lang.expr.sql package is disabled.  Maybe we will ressurect it someday...

public abstract class SqlExpr
{
  public abstract void resolveColumns(Stack<SqlTableImport> context) throws SQLException;
  public abstract Schema getSchema();
  public abstract Expr toJaql(Env env) throws Exception;
  
  public final Expr wrapToJaql(Env env)
  {
    try
    {
      Stack<SqlTableImport> context = new Stack<SqlTableImport>();
      resolveColumns(context);
      return toJaql(env);
    }
    catch (Exception e)
    {
      throw JaqlUtil.rethrow(e);
    }
  }
}

