package com.ibm.jaql.lang.expr.function;

import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.Expr;

public class VarParameter extends Parameter<Expr>
{
  BindingExpr binding;
  
  public VarParameter(BindingExpr binding, Expr defaultValue)
  {
    super(binding.var.name(), binding.var.getSchema(), defaultValue);
    this.binding = binding;
  }
  
  public VarParameter(BindingExpr binding)
  {
    super(binding.var.name(), binding.var.getSchema());
    this.binding = binding;
  }

  @Override
  protected Expr processDefaultValue(Expr value)
  {
    return value;
  }
  
  public BindingExpr getBinding()
  {
    return binding;
  }
  
  public Var getVar()
  {
    return binding.var;
  }

}
