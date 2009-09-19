package com.ibm.jaql.lang.expr.function;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.lang.expr.core.Expr;

/** Parameters associated with variables. */
public class VarParameters extends Parameters<Expr>
{
  public VarParameters(VarParameter ... parameters)
  {
    super(parameters);
  }

  @Override
  protected Parameter<Expr> createParameter(JsonString name, Schema schema)
  {
    throw new UnsupportedOperationException(); // not needed here
  }

  @Override
  protected Parameter<Expr> createParameter(JsonString name, Schema schema,
      Expr defaultValue)
  {
    throw new UnsupportedOperationException(); // not needed here
  }

  @Override
  protected Expr[] newArrayOfT(int size)
  {
    return new Expr[size];
  }

  @Override
  public VarParameter get(int i)
  {
    return (VarParameter)super.get(i);
  }
}
