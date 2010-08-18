package com.ibm.jaql.lang.expr.sql;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.VarExpr;


public class JaqlToSqlTable extends SchemaBasedSqlTableExpr
{
  protected Expr jaql;
  
  public JaqlToSqlTable(Expr jaql)
  {
    this.jaql = jaql;
  }

  public JaqlToSqlTable(Env env, String id)
  {
    this( new VarExpr(env.inscope(id)) );
  }

  @Override
  public Schema getSchema()
  {
    return jaql.getSchema();
  }
  
  @Override
  public Expr toJaql(Env env) throws Exception
  {
    return jaql;
  }

}


