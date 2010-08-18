package com.ibm.jaql.lang.expr.sql;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.lang.expr.core.CompareExpr;
import com.ibm.jaql.lang.expr.core.Expr;

public class SqlCompareExpr extends JaqlFnSqlExpr
{
  protected int op;
  
  public SqlCompareExpr(int op, SqlExpr... args)
  {
    super(CompareExpr.class, args);
    this.op = op;
  }

  @Override
  protected Expr makeExpr(Expr[] exprs)
  {
    return new CompareExpr(op, exprs);
  }  
  
  @Override
  public Schema getSchema()
  {
    return SchemaFactory.booleanOrNullSchema();
  }
}

