package com.ibm.jaql.lang.expr.function;

import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.Expr;

/** An implementation of {@link Parameter} associated with a variable. */
public class VarParameter extends Parameter<Expr>
{
  Var var;
  Expr defaultValue;
  
  public VarParameter(Var var, Expr defaultValue)
  {
    super(var.name(), var.getSchema(), defaultValue);
    this.var = var;
    this.defaultValue = defaultValue;
  }
  
  public VarParameter(Var var)
  {
    super(var.name(), var.getSchema());
    this.var = var;
    this.defaultValue = null;
  }

  @Override
  protected Expr processDefault(Expr value)
  {
    return value;
  }
  
  public Var getVar()
  {
    return var;
  }
}
