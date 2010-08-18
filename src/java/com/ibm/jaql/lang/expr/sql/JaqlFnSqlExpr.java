package com.ibm.jaql.lang.expr.sql;

import java.lang.reflect.Constructor;
import java.sql.SQLException;
import java.util.Stack;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.expr.core.Expr;


public class JaqlFnSqlExpr extends SqlExpr
{
  protected Class<? extends Expr> exprClass;
  protected SqlExpr[] args;
  protected int op;
  
  public JaqlFnSqlExpr(Class<? extends Expr> exprClass, SqlExpr... args)
  {
    this.exprClass = exprClass;
    this.args = args;
  }

  @Override
  public void resolveColumns(Stack<SqlTableImport> context) throws SQLException
  {
    for( SqlExpr e: args )
    {
      e.resolveColumns(context);
    }
  }

  @Override
  public Expr toJaql(Env env) throws Exception
  {
    Expr[] es = new Expr[args.length];
    for(int i = 0 ; i < es.length ; i++)
    {
      es[i] = args[i].toJaql(env);
    }
    return makeExpr(es);
  }
  
  protected Expr makeExpr(Expr[] exprs) throws Exception
  {
    Constructor<? extends Expr> cons = exprClass.getConstructor(Expr[].class);
    return cons.newInstance(new Object[]{exprs});
  }

  @Override
  public Schema getSchema()
  {
    return SchemaFactory.anySchema(); // TODO: add schema!
  }
}

