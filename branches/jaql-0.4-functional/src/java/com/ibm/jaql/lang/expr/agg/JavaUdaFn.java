package com.ibm.jaql.lang.expr.agg;

import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.MacroExpr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.lang.expr.function.DefineJaqlFunctionExpr;
import com.ibm.jaql.lang.expr.function.JsonValueParameter;
import com.ibm.jaql.lang.expr.function.JsonValueParameters;

public class JavaUdaFn extends MacroExpr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor
  {
    public Descriptor()
    {
      super(
          "javauda",
          JavaUdaFn.class,
          new JsonValueParameters(
              new JsonValueParameter("class", SchemaFactory.stringSchema()),
              new JsonValueParameter("args", SchemaFactory.anySchema(), true)),
            SchemaFactory.functionSchema());
    }
  }
  
  public JavaUdaFn(Expr ... exprs)
  {
    super(exprs);
  }
  
  @Override
  public Expr expand(Env env) throws Exception
  {   
    if (exprs.length == 0)
    {
      throw new IllegalArgumentException("class name expected");
    }
    Expr[] callExprs = new Expr[exprs.length + 1];

    Var var = env.makeVar("$");
    callExprs[0] = new VarExpr(var);
    System.arraycopy(exprs, 0, callExprs, 1, exprs.length);
    
    Expr body = new JavaUdaCallFn(callExprs);
    return new DefineJaqlFunctionExpr(new Var[] { var }, body);
  }
}
